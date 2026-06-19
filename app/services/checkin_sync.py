"""
Service to sync calendar events (Cal.com/Calendly) with clients by email matching.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, text, func
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
import uuid
import json
import re
from app.models.client import Client, LifecycleState
from app.models.client_checkin import ClientCheckIn
from app.models.calendar_booking_sales import CalendarBookingSales, EventTypeSalesCall
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.core.encryption import decrypt_token
from app.services.calcom_auth import get_calcom_access_token_optional
from app.services.calendar_booking_time import ensure_utc, format_calendly_api_time
from app.services.calcom_bookings_client import (
    extract_calcom_attendees,
    fetch_all_calcom_bookings,
    fetch_calcom_booking_attendees,
    fetch_calcom_booking_detail,
    resolve_calcom_participants,
    _booking_uid,
)
import httpx


def normalize_email(email: str) -> str:
    """Normalize email for matching (lowercase, strip whitespace)"""
    if not email:
        return ""
    return re.sub(r'\s+', '', email.lower().strip())


def _build_org_email_client_index(db: Session, org_id: uuid.UUID) -> Dict[str, Client]:
    """
    One query over org clients: map normalized email -> Client (primary + emails[]).
    Avoids reloading all clients for every calendar attendee (major perf win).
    """
    index: Dict[str, Client] = {}
    rows = (
        db.query(Client)
        .filter(
            Client.org_id == org_id,
            or_(Client.email.isnot(None), Client.emails.isnot(None)),
        )
        .all()
    )
    for c in rows:
        for em in c.get_all_emails_normalized():
            if em and em not in index:
                index[em] = c
    return index


def _add_client_to_email_index(index: Dict[str, Client], client: Client) -> None:
    for em in client.get_all_emails_normalized():
        if em and em not in index:
            index[em] = client


def _calendly_fetch_invitees_threadsafe(event_uri: str, headers: dict) -> tuple:
    """HTTP-only (safe for ThreadPoolExecutor). Returns (event_uri, invitees_list|None, status_code)."""
    if not event_uri:
        return ("", None, 0)
    try:
        # Prefer documented path: GET /scheduled_events/{uuid}/invitees
        invitees_url = event_uri.rstrip("/") + "/invitees"
        r = httpx.get(
            invitees_url,
            headers=headers,
            params={"count": 100},
            timeout=httpx.Timeout(22.0, connect=8.0),
        )
        if r.status_code != 200:
            # Legacy fallback
            r = httpx.get(
                "https://api.calendly.com/event_invitees",
                headers=headers,
                params={"event": event_uri},
                timeout=httpx.Timeout(22.0, connect=8.0),
            )
        if r.status_code != 200:
            return (event_uri, None, r.status_code)
        return (event_uri, r.json().get("collection", []) or [], r.status_code)
    except Exception:
        return (event_uri, None, -1)


def _fetch_calendly_scheduled_events(http: httpx.Client, headers: dict, user_uri: str) -> List[dict]:
    """
    Paginate Calendly scheduled_events with microsecond-precision datetimes.
    Uses page_token for follow-up pages (next_page URLs can 400 when first-page times lack precision).
    """
    events: List[dict] = []
    now_utc = datetime.now(timezone.utc)
    min_start = format_calendly_api_time(now_utc - timedelta(days=365))
    max_start = format_calendly_api_time(now_utc + timedelta(days=365))
    page_token: Optional[str] = None

    for page_idx in range(25):
        if page_idx == 0:
            params: Dict[str, Any] = {
                "user": user_uri,
                "count": 100,
                "min_start_time": min_start,
                "max_start_time": max_start,
                "sort": "start_time:asc",
            }
            r = http.get("https://api.calendly.com/scheduled_events", headers=headers, params=params)
        elif page_token:
            r = http.get(
                "https://api.calendly.com/scheduled_events",
                headers=headers,
                params={"page_token": page_token, "count": 100},
            )
        else:
            break

        if r.status_code != 200:
            print(f"[CHECKIN SYNC] [CALENDLY] ❌ scheduled_events page={page_idx + 1} error: {r.status_code}")
            print(f"[CHECKIN SYNC] [CALENDLY] Response: {r.text[:500]}")
            break

        payload = r.json()
        batch = payload.get("collection", []) or []
        events.extend(batch)
        pagination = payload.get("pagination") or {}
        page_token = pagination.get("next_page_token")
        if not batch or not page_token:
            break

    return events


def _apply_provider_completed_flag(
    existing_checkin: ClientCheckIn,
    completed: bool,
    *,
    times_changed: bool = False,
) -> None:
    """Sync completed from provider; reset to False when a booking is rescheduled into the future."""
    if times_changed or not completed:
        existing_checkin.completed = completed
    elif completed and not existing_checkin.completed:
        existing_checkin.completed = True


def _calendar_placeholder_email(org_id: uuid.UUID) -> str:
    return f"calendar-events-{org_id}@internal.sweep.local"


def is_calendar_placeholder_email(email: Optional[str]) -> bool:
    if not email:
        return False
    return email.startswith("calendar-events-") and email.endswith("@internal.sweep.local")


def get_or_create_calendar_placeholder_client(db: Session, org_id: uuid.UUID) -> Client:
    """Holder client for calendar events that have no external attendee email."""
    email = _calendar_placeholder_email(org_id)
    existing = (
        db.query(Client)
        .filter(Client.org_id == org_id, Client.email == email)
        .first()
    )
    if existing:
        return existing
    client = Client(
        org_id=org_id,
        email=email,
        first_name="Calendar",
        last_name="Events",
        lifecycle_state=LifecycleState.COLD_LEAD,
        notes="Internal placeholder for synced calendar events without an attendee email",
    )
    db.add(client)
    db.flush()
    return client


def _pick_primary_participant(
    participants: List[Dict[str, Any]],
    *,
    placeholder_email: str,
    fallback_name: str,
) -> Dict[str, Any]:
    """One row per calendar event — prefer first external attendee, else placeholder."""
    for row in participants:
        if row.get("email") and not row.get("_placeholder"):
            return row
    for row in participants:
        if row.get("email"):
            return row
    return {
        "email": placeholder_email,
        "name": fallback_name or "Calendar event",
        "_placeholder": True,
    }


def _find_calcom_checkin(
    db: Session,
    org_id: uuid.UUID,
    event_id: str,
    legacy_event_id: Optional[str],
) -> Optional[ClientCheckIn]:
    id_filters = [ClientCheckIn.event_id == event_id]
    if legacy_event_id and legacy_event_id != event_id:
        id_filters.append(ClientCheckIn.event_id == legacy_event_id)
    return (
        db.query(ClientCheckIn)
        .filter(
            and_(
                ClientCheckIn.org_id == org_id,
                ClientCheckIn.provider == "calcom",
                or_(*id_filters),
            )
        )
        .first()
    )


def extract_calendly_participants(event: dict, invitees: List[dict]) -> List[Dict[str, Any]]:
    """Invitees from API plus event_guests embedded on scheduled_events rows."""
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    def add(email: Optional[str], name: Optional[str] = None) -> None:
        if not email or not str(email).strip():
            return
        key = normalize_email(str(email))
        if not key or key in seen:
            return
        seen.add(key)
        out.append({"email": str(email).strip(), "name": name})

    for inv in invitees or []:
        if isinstance(inv, dict):
            email = inv.get("email")
            if not email:
                continue
            key = normalize_email(str(email))
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "email": str(email).strip(),
                    "name": inv.get("name"),
                    "status": inv.get("status"),
                }
            )

    for guest in event.get("event_guests") or []:
        if isinstance(guest, dict):
            add(guest.get("email"))

    return out


def _calcom_enrich_participants_threadsafe(access_token: str, booking: dict) -> tuple:
    """HTTP-only: detail + /attendees fallbacks for list rows missing participants."""
    uid = _booking_uid(booking)
    if not uid:
        return ("", [])
    detail = fetch_calcom_booking_detail(access_token, uid)
    extra = fetch_calcom_booking_attendees(access_token, uid)
    rows = resolve_calcom_participants(booking, detail=detail, extra_attendee_rows=extra)
    return (uid, rows)


def _calcom_fetch_attendees_threadsafe(access_token: str, booking_uid: str) -> tuple:
    """Deprecated alias — prefer _calcom_enrich_participants_threadsafe."""
    rows = fetch_calcom_booking_attendees(access_token, booking_uid)
    return (booking_uid, rows)


def get_sales_call_flags(
    db: Session, org_id: uuid.UUID, provider: str, event_id: str, event_type_id: Optional[str] = None
) -> tuple:
    """Return (is_sales_call, sale_closed) from CalendarBookingSales or EventTypeSalesCall."""
    row = db.query(CalendarBookingSales).filter(
        CalendarBookingSales.org_id == org_id,
        CalendarBookingSales.provider == provider,
        CalendarBookingSales.event_id == event_id,
    ).first()
    if row:
        return (row.is_sales_call, row.sale_closed)
    if event_type_id:
        et = db.query(EventTypeSalesCall).filter(
            EventTypeSalesCall.org_id == org_id,
            EventTypeSalesCall.provider == provider,
            EventTypeSalesCall.event_type_id == event_type_id,
        ).first()
        if et:
            return (True, None)
    return (False, None)


def ensure_client_for_booking_attendee(
    db: Session, org_id: uuid.UUID, email: str, name: Optional[str] = None
) -> Optional[Client]:
    """
    Find a client by attendee email (normalized; checks primary email and emails list).
    If none exists, create a new client as booked so booking data can populate the client board.
    """
    from app.models.client import find_client_by_email
    existing = find_client_by_email(db, org_id, email)
    if existing:
        return existing
    if not email or not str(email).strip():
        return None
    normalized = normalize_email(email)
    if not normalized:
        return None
    first_name, last_name = None, None
    if name and isinstance(name, str) and name.strip():
        parts = name.strip().split(None, 1)
        first_name = parts[0]
        last_name = parts[1] if len(parts) > 1 else None
    if not first_name and not last_name:
        local = normalized.split("@", 1)[0].replace(".", " ").replace("_", " ").strip()
        first_name = local.title() if local else "Guest"
    client = Client(
        org_id=org_id,
        email=email.strip(),
        first_name=first_name,
        last_name=last_name,
            lifecycle_state=LifecycleState.BOOKED,
        notes="Created from calendar booking (attendee)",
    )
    db.add(client)
    db.flush()
    print(f"[CHECKIN SYNC] Created new booked client for booking attendee: {email}")
    try:
        from app.services.fathom_client_link import (
            queue_fathom_relink_followups,
            relink_fathom_records_for_client,
        )

        linked = relink_fathom_records_for_client(db, org_id, client)
        if linked:
            queue_fathom_relink_followups(None, org_id, linked)
    except Exception as relink_err:
        print(f"[CHECKIN SYNC] Fathom relink after booking client create skipped: {relink_err}")
    return client


def sync_calcom_bookings(
    db: Session,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    new_bookings_out: Optional[List[Dict[str, Any]]] = None,
) -> int:
    """
    Sync Cal.com bookings with clients by matching attendee emails.
    Returns the number of check-ins created/updated.

    When ``new_bookings_out`` is provided, freshly inserted (not updated) bookings are
    appended as dicts so the caller can fire post-sync automation triggers
    (e.g. pre-sale post-booking emails) after the DB commit succeeds.
    
    NEW APPROACH: Also check existing check-ins in database for past events
    that may have been synced previously, and ensure they're up to date.
    """
    print(f"[CHECKIN SYNC] [CALCOM] Starting Cal.com sync for org {org_id}")

    existing_calcom_n = (
        db.query(func.count(ClientCheckIn.id))
        .filter(
            and_(
                ClientCheckIn.org_id == org_id,
                ClientCheckIn.provider == "calcom",
            )
        )
        .scalar()
        or 0
    )
    print(f"[CHECKIN SYNC] [CALCOM] Existing Cal.com check-ins in DB: {existing_calcom_n}")

    # Prefer CALCOM_API_KEY env; fall back to org OAuth token in oauth_tokens.
    access_token = get_calcom_access_token_optional(db, org_id, user_id)
    if not access_token:
        print(f"[CHECKIN SYNC] [CALCOM] ❌ No Cal.com connection found for org {org_id}")
        return 0

    print(f"[CHECKIN SYNC] [CALCOM] ✅ Cal.com credentials resolved")
    
    # Fetch bookings from Cal.com API (per-status requests — see calcom_bookings_client.py)
    email_index = _build_org_email_client_index(db, org_id)

    try:
        print(f"[CHECKIN SYNC] [CALCOM] Fetching bookings (per-status Cal.com v2 API)...")
        all_bookings = fetch_all_calcom_bookings(access_token)
        
        # Check breakdown of past vs future bookings
        past_count = 0
        future_count = 0
        now_utc = datetime.now(timezone.utc)
        for booking in all_bookings:
            start_time_str = booking.get("start") or booking.get("startTime")
            end_time_str = booking.get("end") or booking.get("endTime")
            if start_time_str:
                try:
                    start_time = ensure_utc(datetime.fromisoformat(start_time_str.replace('Z', '+00:00')))
                    end_time = (
                        ensure_utc(datetime.fromisoformat(end_time_str.replace('Z', '+00:00')))
                        if end_time_str
                        else start_time
                    )
                    if end_time < now_utc:
                        past_count += 1
                    else:
                        future_count += 1
                except Exception:
                    pass
        
        print(f"[CHECKIN SYNC] [CALCOM] Bookings breakdown: {past_count} past, {future_count} future")
        
        bookings = all_bookings
        
        if len(bookings) == 0:
            print(f"[CHECKIN SYNC] [CALCOM] ⚠️ No bookings found after fetching all pages")
            return 0
        
        print(f"[CHECKIN SYNC] [CALCOM] ✅ Processing {len(bookings)} total Cal.com bookings (past and future)")

        # Prefetch participants for list rows that omit them (parallel detail + /attendees).
        participants_by_uid: Dict[str, List[Dict[str, Any]]] = {}
        bookings_needing_participants = [
            b for b in bookings if isinstance(b, dict) and _booking_uid(b) and not extract_calcom_attendees(b)
        ]
        if bookings_needing_participants:
            max_workers = min(12, max(1, len(bookings_needing_participants)))
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(_calcom_enrich_participants_threadsafe, access_token, booking): _booking_uid(booking)
                    for booking in bookings_needing_participants
                }
                for fut in as_completed(futures):
                    uid, rows = fut.result()
                    if uid and rows:
                        participants_by_uid[uid] = rows
            print(
                f"[CHECKIN SYNC] [CALCOM] Enriched participants for "
                f"{len(participants_by_uid)}/{len(bookings_needing_participants)} bookings"
            )

        placeholder_client = get_or_create_calendar_placeholder_client(db, org_id)

        synced_count = 0
        bookings_without_attendees = 0
        bookings_without_matching_clients = 0
        # Tracks rows we just *created* (not updated) so the caller can fire
        # post-booking automation triggers after the commit succeeds.
        new_calcom_bookings: List[Dict[str, Any]] = (
            new_bookings_out if new_bookings_out is not None else []
        )
        
        for idx, booking in enumerate(bookings):
            try:
                # Handle case where booking might not be a dict
                if not isinstance(booking, dict):
                    print(f"[CHECKIN SYNC] [CALCOM] ⚠️ Booking {idx} is not a dict: {type(booking)}")
                    print(f"[CHECKIN SYNC] [CALCOM] Booking {idx} value: {str(booking)[:200]}")
                    continue
                
                # Cal.com v2 canonical id is uid (string); numeric id is legacy.
                event_id = str(booking.get("uid") or booking.get("id") or "").strip()
                legacy_event_id = str(booking.get("id") or "").strip() if booking.get("id") else None
                if not event_id:
                    print(f"[CHECKIN SYNC] [CALCOM] ⚠️ Booking {idx} has no ID, skipping")
                    continue
                
                title = booking.get("title") or (booking.get("eventType", {}) or {}).get("title") if isinstance(booking.get("eventType"), dict) else "Untitled"
                # Use same field extraction as calendar tab: booking.get("start") first, then "startTime"
                start_time_str = booking.get("start") or booking.get("startTime")
                end_time_str = booking.get("end") or booking.get("endTime")
                location = booking.get("location")
                meeting_url = booking.get("meetingUrl") or location
                
                if not start_time_str:
                    print(f"[CHECKIN SYNC] Skipping booking {event_id}: no start_time")
                    continue
                
                start_time = ensure_utc(datetime.fromisoformat(start_time_str.replace('Z', '+00:00')))
                end_time = None
                if end_time_str:
                    end_time = ensure_utc(datetime.fromisoformat(end_time_str.replace('Z', '+00:00')))
                
                # All event types sync — sales flag is metadata only, never a filter.
                attendees_list = extract_calcom_attendees(booking)
                if not attendees_list:
                    attendees_list = participants_by_uid.get(event_id, [])
                if not attendees_list:
                    attendees_list = [
                        {
                            "email": placeholder_client.email,
                            "name": title or "Calendar event",
                            "_placeholder": True,
                        }
                    ]
                    bookings_without_attendees += 1

                attendee = _pick_primary_participant(
                    attendees_list,
                    placeholder_email=placeholder_client.email,
                    fallback_name=title or "Calendar event",
                )
                use_placeholder = bool(attendee.get("_placeholder"))
                attendee_email = attendee.get("email")
                if not attendee_email:
                    continue

                attendee_name = attendee.get("name") or (title if use_placeholder else None)
                if use_placeholder:
                    matching_client = placeholder_client
                else:
                    normalized_email = normalize_email(attendee_email)
                    matching_client = email_index.get(normalized_email)
                    if not matching_client:
                        matching_client = ensure_client_for_booking_attendee(
                            db, org_id, attendee_email, attendee_name
                        )
                        if matching_client:
                            _add_client_to_email_index(email_index, matching_client)
                    if not matching_client:
                        bookings_without_matching_clients += 1
                        continue

                existing_checkin = _find_calcom_checkin(db, org_id, event_id, legacy_event_id)
                if existing_checkin and existing_checkin.event_id != event_id:
                    existing_checkin.event_id = event_id

                now = datetime.now(timezone.utc)
                effective_end = end_time if end_time else start_time
                completed = effective_end < now
                cancelled = (
                    str(booking.get("status") or "").lower() in ("cancelled", "canceled", "rejected")
                    or booking.get("cancelled", False)
                )
                absent_host = booking.get("absentHost", False)
                raw_attendees = booking.get("attendees") or []
                attendee_absent = any(a.get("absent") for a in raw_attendees if isinstance(a, dict))
                no_show = bool(absent_host or attendee_absent)
                et = booking.get("eventType") or {}
                event_type_id = str(et.get("id") or booking.get("eventTypeId") or "")
                event_type_label = str(et.get("title") or et.get("slug") or booking.get("title") or "") or None
                is_sales_call, sale_closed = get_sales_call_flags(db, org_id, "calcom", event_id, event_type_id or None)

                if existing_checkin:
                    times_changed = (
                        existing_checkin.start_time != start_time
                        or existing_checkin.end_time != end_time
                    )
                    existing_checkin.client_id = matching_client.id
                    existing_checkin.title = title
                    existing_checkin.start_time = start_time
                    existing_checkin.end_time = end_time
                    existing_checkin.location = location
                    existing_checkin.meeting_url = meeting_url
                    existing_checkin.attendee_email = attendee_email
                    existing_checkin.attendee_name = attendee_name
                    if event_type_id and not getattr(existing_checkin, "event_type_id", None):
                        existing_checkin.event_type_id = event_type_id
                    if event_type_label and not getattr(existing_checkin, "event_type_label", None):
                        existing_checkin.event_type_label = event_type_label
                    if cancelled:
                        existing_checkin.cancelled = True
                    elif not getattr(existing_checkin, "cancelled", False):
                        existing_checkin.cancelled = False
                    if no_show:
                        existing_checkin.no_show = True
                    elif not getattr(existing_checkin, "no_show", False):
                        existing_checkin.no_show = False
                    _apply_provider_completed_flag(
                        existing_checkin, completed, times_changed=times_changed
                    )
                    if not getattr(existing_checkin, "is_sales_call", False) and is_sales_call:
                        existing_checkin.is_sales_call = True
                    if getattr(existing_checkin, "sale_closed", None) is None and sale_closed is not None:
                        existing_checkin.sale_closed = sale_closed
                    existing_checkin.updated_at = datetime.now(timezone.utc)
                    synced_count += 1
                else:
                    checkin = ClientCheckIn(
                        org_id=org_id,
                        client_id=matching_client.id,
                        event_id=event_id,
                        provider="calcom",
                        title=title,
                        start_time=start_time,
                        end_time=end_time,
                        location=location,
                        meeting_url=meeting_url,
                        attendee_email=attendee_email,
                        attendee_name=attendee_name,
                        event_type_id=event_type_id or None,
                        event_type_label=event_type_label,
                        completed=completed,
                        cancelled=cancelled,
                        no_show=no_show,
                        is_sales_call=is_sales_call,
                        sale_closed=sale_closed,
                        raw_event_data=json.dumps(booking),
                    )
                    db.add(checkin)
                    synced_count += 1
                    if not use_placeholder:
                        new_calcom_bookings.append({
                            "client_id": matching_client.id,
                            "external_booking_id": event_id,
                            "event_type_id": event_type_id or None,
                            "event_type_label": event_type_label,
                            "attendee_email": attendee_email,
                            "start_time": start_time,
                        })

                db.flush()
                
            except Exception as e:
                print(f"[CHECKIN SYNC] [CALCOM] ❌ Error processing booking {idx}: {str(e)}")
                print(f"[CHECKIN SYNC] [CALCOM] Booking type: {type(booking)}")
                if isinstance(booking, dict):
                    print(f"[CHECKIN SYNC] [CALCOM] Booking keys: {list(booking.keys())[:10]}")
                else:
                    print(f"[CHECKIN SYNC] [CALCOM] Booking value: {str(booking)[:200]}")
                import traceback
                traceback.print_exc()
                continue
        
        try:
            db.commit()
            print(f"[CHECKIN SYNC] [CALCOM] ✅ Successfully committed {synced_count} Cal.com check-ins to database")
        except Exception as commit_error:
            db.rollback()
            print(f"[CHECKIN SYNC] [CALCOM] ❌ Failed to commit check-ins: {commit_error}")
            import traceback
            traceback.print_exc()
            raise
        
        print(f"[CHECKIN SYNC] [CALCOM] 📊 Summary: {synced_count} synced, {bookings_without_attendees} without attendees, {bookings_without_matching_clients} without matching clients")
        return synced_count
        
    except Exception as e:
        db.rollback()
        print(f"[CHECKIN SYNC] [CALCOM] ❌ Exception syncing Cal.com bookings: {type(e).__name__}: {str(e)}")
        import traceback
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"[CHECKIN SYNC] [CALCOM] Full traceback:")
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
        return 0


def sync_calendly_events(
    db: Session,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    new_bookings_out: Optional[List[Dict[str, Any]]] = None,
) -> int:
    """
    Sync Calendly events with clients by matching invitee emails.
    Returns the number of check-ins created/updated.

    When ``new_bookings_out`` is provided, freshly inserted (not updated) events are
    appended as dicts so the caller can fire post-sync automation triggers
    (e.g. pre-sale post-booking emails) after the DB commit succeeds.
    """
    print(f"[CHECKIN SYNC] [CALENDLY] Starting Calendly sync for org {org_id}")
    
    # Get Calendly OAuth token using raw SQL to bypass SQLAlchemy's enum name conversion
    # SQLAlchemy converts enum values to names (CALENDLY) but database has lowercase (calendly)
    from sqlalchemy import text
    result = db.execute(
        text("""
            SELECT id, access_token, expires_at FROM oauth_tokens 
            WHERE provider = 'calendly'::oauthprovider
            AND org_id = :org_id 
            LIMIT 1
        """),
        {"org_id": org_id}
    ).first()
    
    if not result:
        print(f"[CHECKIN SYNC] [CALENDLY] ❌ No Calendly connection found for org {org_id}")
        return 0
    
    print(f"[CHECKIN SYNC] [CALENDLY] ✅ Found Calendly token")
    
    token_id, access_token_encrypted, expires_at = result[0], result[1], result[2]
    
    # Check if token is expired (only for OAuth tokens, not API keys)
    if expires_at and expires_at < datetime.utcnow():
        print(f"[CHECKIN SYNC] Calendly token has expired for org {org_id}")
        return 0
    
    try:
        access_token = decrypt_token(
            access_token_encrypted,
            audit_context={
                "db": db,
                "org_id": org_id,
                "user_id": user_id,
                "resource_type": "calendly_token",
                "resource_id": str(token_id)
            }
        )
    except Exception as e:
        print(f"[CHECKIN SYNC] Failed to decrypt Calendly token: {e}")
        return 0
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    email_index = _build_org_email_client_index(db, org_id)

    try:
        cal_timeout = httpx.Timeout(25.0, connect=10.0)
        with httpx.Client(timeout=cal_timeout) as http:
            user_info_response = http.get("https://api.calendly.com/users/me", headers=headers)

            if user_info_response.status_code != 200:
                print(f"[CHECKIN SYNC] Failed to get Calendly user info: {user_info_response.status_code}")
                return 0

            user_uri = user_info_response.json().get("resource", {}).get("uri")
            if not user_uri:
                print(f"[CHECKIN SYNC] No user URI found in Calendly response")
                return 0

            print(f"[CHECKIN SYNC] [CALENDLY] Fetching scheduled events (paginated, ±365d)...")
            events = _fetch_calendly_scheduled_events(http, headers, user_uri)

            print(f"[CHECKIN SYNC] [CALENDLY] ✅ Loaded {len(events)} events")

            if len(events) == 0:
                print(f"[CHECKIN SYNC] [CALENDLY] ⚠️ No events found")
                return 0

        invitees_by_uri: Dict[str, List[dict]] = {}
        uris = list(dict.fromkeys(str(e.get("uri") or "") for e in events if e.get("uri")))
        max_workers = min(12, max(1, len(uris)))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_calendly_fetch_invitees_threadsafe, uri, headers): uri for uri in uris if uri
            }
            for fut in as_completed(futures):
                uri, coll, code = fut.result()
                if coll is None:
                    invitees_by_uri[uri] = []
                    if code not in (200, 0) and code != -1:
                        print(f"[CHECKIN SYNC] [CALENDLY] invitees fetch failed for uri={uri[-40:]} status={code}")
                else:
                    invitees_by_uri[uri] = coll

        placeholder_client = get_or_create_calendar_placeholder_client(db, org_id)

        synced_count = 0
        events_without_invitees = 0
        events_without_matching_clients = 0
        new_calendly_bookings: List[Dict[str, Any]] = (
            new_bookings_out if new_bookings_out is not None else []
        )

        for event in events:
            try:
                event_uri = event.get("uri", "")
                event_uuid = event_uri.split("/")[-1] if "/" in event_uri else event_uri
                name = event.get("name", "Untitled")
                start_time_str = event.get("start_time")
                end_time_str = event.get("end_time")
                status = event.get("status", "active")
                location = event.get("location", {})

                if not start_time_str:
                    continue

                start_time = ensure_utc(datetime.fromisoformat(start_time_str.replace("Z", "+00:00")))
                end_time = None
                if end_time_str:
                    end_time = ensure_utc(datetime.fromisoformat(end_time_str.replace("Z", "+00:00")))

                meeting_url = None
                if isinstance(location, dict):
                    meeting_url = location.get("location") or location.get("join_url")
                elif isinstance(location, str):
                    meeting_url = location

                invitees = invitees_by_uri.get(event_uri, [])
                participants = extract_calendly_participants(event, invitees)
                if not participants:
                    events_without_invitees += 1
                    participants = [
                        {
                            "email": placeholder_client.email,
                            "name": name or "Calendar event",
                            "_placeholder": True,
                        }
                    ]

                invitee = _pick_primary_participant(
                    participants,
                    placeholder_email=placeholder_client.email,
                    fallback_name=name or "Calendar event",
                )
                use_placeholder = bool(invitee.get("_placeholder"))
                invitee_email = invitee.get("email")
                if not invitee_email:
                    continue

                invitee_name = invitee.get("name") or (name if use_placeholder else None)
                invitee_no_show = (
                    str(invitee.get("status") or "").lower() == "no_show" if not use_placeholder else False
                )

                if use_placeholder:
                    matching_client = placeholder_client
                else:
                    normalized_email = normalize_email(invitee_email)
                    matching_client = email_index.get(normalized_email)
                    if not matching_client:
                        matching_client = ensure_client_for_booking_attendee(
                            db, org_id, invitee_email, invitee_name
                        )
                        if matching_client:
                            _add_client_to_email_index(email_index, matching_client)
                    if not matching_client:
                        events_without_matching_clients += 1
                        continue

                existing_checkin = db.query(ClientCheckIn).filter(
                    and_(
                        ClientCheckIn.org_id == org_id,
                        ClientCheckIn.provider == "calendly",
                        or_(
                            ClientCheckIn.event_uri == event_uri,
                            ClientCheckIn.event_id == event_uuid,
                        ),
                    )
                ).first()

                now = datetime.now(timezone.utc)
                effective_end = end_time if end_time else start_time
                completed = effective_end < now
                cancelled = status == "canceled"
                no_show = invitee_no_show
                event_type_uri = event.get("event_type") or ""
                if isinstance(event_type_uri, dict):
                    event_type_uri = event_type_uri.get("uri") or ""
                event_type_uri = str(event_type_uri or "") or None
                event_type_label_value = name or None
                is_sales_call, sale_closed = get_sales_call_flags(
                    db, org_id, "calendly", event_uuid, event_type_uri or None
                )

                if existing_checkin:
                    times_changed = (
                        existing_checkin.start_time != start_time
                        or existing_checkin.end_time != end_time
                    )
                    existing_checkin.client_id = matching_client.id
                    existing_checkin.event_id = event_uuid
                    existing_checkin.event_uri = event_uri
                    existing_checkin.title = name
                    existing_checkin.start_time = start_time
                    existing_checkin.end_time = end_time
                    existing_checkin.location = meeting_url
                    existing_checkin.meeting_url = meeting_url
                    existing_checkin.attendee_email = invitee_email
                    existing_checkin.attendee_name = invitee_name
                    if event_type_uri and not getattr(existing_checkin, "event_type_id", None):
                        existing_checkin.event_type_id = event_type_uri
                    if event_type_label_value and not getattr(existing_checkin, "event_type_label", None):
                        existing_checkin.event_type_label = event_type_label_value
                    if cancelled:
                        existing_checkin.cancelled = True
                    elif not getattr(existing_checkin, "cancelled", False):
                        existing_checkin.cancelled = False
                    if no_show:
                        existing_checkin.no_show = True
                    elif not getattr(existing_checkin, "no_show", False):
                        existing_checkin.no_show = False
                    _apply_provider_completed_flag(
                        existing_checkin, completed, times_changed=times_changed
                    )
                    if not getattr(existing_checkin, "is_sales_call", False) and is_sales_call:
                        existing_checkin.is_sales_call = True
                    if getattr(existing_checkin, "sale_closed", None) is None and sale_closed is not None:
                        existing_checkin.sale_closed = sale_closed
                    existing_checkin.updated_at = datetime.now(timezone.utc)
                    synced_count += 1
                else:
                    checkin = ClientCheckIn(
                        org_id=org_id,
                        client_id=matching_client.id,
                        event_id=event_uuid,
                        event_uri=event_uri,
                        provider="calendly",
                        title=name,
                        start_time=start_time,
                        end_time=end_time,
                        location=meeting_url,
                        meeting_url=meeting_url,
                        attendee_email=invitee_email,
                        attendee_name=invitee_name,
                        event_type_id=event_type_uri,
                        event_type_label=event_type_label_value,
                        completed=completed,
                        cancelled=cancelled,
                        no_show=no_show,
                        is_sales_call=is_sales_call,
                        sale_closed=sale_closed,
                        raw_event_data=json.dumps(event),
                    )
                    db.add(checkin)
                    synced_count += 1
                    if not use_placeholder:
                        new_calendly_bookings.append({
                            "client_id": matching_client.id,
                            "external_booking_id": event_uuid,
                            "event_type_id": event_type_uri,
                            "event_type_label": event_type_label_value,
                            "attendee_email": invitee_email,
                            "start_time": start_time,
                        })

                db.flush()
                
            except Exception as e:
                print(f"[CHECKIN SYNC] Error processing Calendly event: {e}")
                continue
        
        db.commit()
        print(f"[CHECKIN SYNC] ✅ Synced {synced_count} Calendly check-ins")
        print(f"[CHECKIN SYNC] 📊 Stats: {events_without_invitees} events without invitees, {events_without_matching_clients} events without matching clients")
        return synced_count
        
    except Exception as e:
        db.rollback()
        print(f"[CHECKIN SYNC] Error syncing Calendly events: {e}")
        import traceback
        traceback.print_exc()
        return 0


def _ensure_client_check_ins_table(db: Session) -> None:
    """Create client_check_ins table and add optional columns if missing (fallback when migrations not run)."""
    try:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS client_check_ins (
                id UUID PRIMARY KEY,
                org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                client_id UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                event_id VARCHAR NOT NULL,
                event_uri VARCHAR,
                provider VARCHAR NOT NULL,
                title VARCHAR,
                start_time TIMESTAMPTZ NOT NULL,
                end_time TIMESTAMPTZ,
                location VARCHAR,
                meeting_url VARCHAR,
                attendee_email VARCHAR NOT NULL,
                attendee_name VARCHAR,
                completed BOOLEAN NOT NULL DEFAULT false,
                cancelled BOOLEAN NOT NULL DEFAULT false,
                raw_event_data TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[CHECKIN SYNC] ensure client_check_ins create: {e}")
        raise
    # Add columns from later migrations if missing
    for col_sql in (
        "ALTER TABLE client_check_ins ADD COLUMN IF NOT EXISTS no_show BOOLEAN NOT NULL DEFAULT false",
        "ALTER TABLE client_check_ins ADD COLUMN IF NOT EXISTS is_sales_call BOOLEAN NOT NULL DEFAULT false",
        "ALTER TABLE client_check_ins ADD COLUMN IF NOT EXISTS sale_closed BOOLEAN",
        # Migration 044: event-type identity for the post-booking automation trigger.
        "ALTER TABLE client_check_ins ADD COLUMN IF NOT EXISTS event_type_id VARCHAR",
        "ALTER TABLE client_check_ins ADD COLUMN IF NOT EXISTS event_type_label VARCHAR",
    ):
        try:
            db.execute(text(col_sql))
            db.commit()
        except Exception as alt_e:
            db.rollback()
            print(f"[CHECKIN SYNC] ensure client_check_ins column: {alt_e}")
    # Create indexes if not exist (idempotent)
    for idx_sql in (
        "CREATE INDEX IF NOT EXISTS ix_client_check_ins_org_id ON client_check_ins (org_id)",
        "CREATE INDEX IF NOT EXISTS ix_client_check_ins_client_id ON client_check_ins (client_id)",
        "CREATE INDEX IF NOT EXISTS ix_client_check_ins_event_id ON client_check_ins (event_id)",
        "CREATE INDEX IF NOT EXISTS ix_client_check_ins_start_time ON client_check_ins (start_time)",
        "CREATE INDEX IF NOT EXISTS ix_client_check_ins_attendee_email ON client_check_ins (attendee_email)",
    ):
        try:
            db.execute(text(idx_sql))
            db.commit()
        except Exception as idx_e:
            db.rollback()
            print(f"[CHECKIN SYNC] ensure client_check_ins index: {idx_e}")
    # Unique constraint (one check-in per event per org) - skip if already exists
    try:
        db.execute(text("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'uq_client_check_ins_event_org'
                ) THEN
                    ALTER TABLE client_check_ins
                    ADD CONSTRAINT uq_client_check_ins_event_org UNIQUE (event_id, org_id);
                END IF;
            END $$
        """))
        db.commit()
    except Exception as uq_e:
        db.rollback()
        print(f"[CHECKIN SYNC] ensure client_check_ins unique: {uq_e}")


def sync_all_checkins(
    db: Session,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    *,
    apply_pipeline_lifecycle_rules: bool = True,
) -> Dict[str, Any]:
    """Sync check-ins from all connected calendar providers."""
    from datetime import datetime, timezone

    sync_start_time = datetime.now(timezone.utc)

    print(f"[CHECKIN SYNC] ===== STARTING SYNC ======")
    print(f"[CHECKIN SYNC] Org ID: {org_id}, User ID: {user_id}")
    
    results = {
        "calcom": 0,
        "calendly": 0,
        "total": 0,
        "affected_client_ids": []
    }

    # Collected per-provider so the post-commit hook can fire pre-sale post-booking
    # automation triggers for *newly inserted* bookings only (not updates).
    new_calcom_bookings: List[Dict[str, Any]] = []
    new_calendly_bookings: List[Dict[str, Any]] = []

    try:
        # Ensure table (and optional columns) exist so sync works even if migrations weren't run
        print(f"[CHECKIN SYNC] Ensuring client_check_ins table exists...")
        _ensure_client_check_ins_table(db)
        print(f"[CHECKIN SYNC] ✅ Table ready")
        
        print(f"[CHECKIN SYNC] Syncing Cal.com bookings...")
        results["calcom"] = sync_calcom_bookings(db, org_id, user_id, new_calcom_bookings)
        print(f"[CHECKIN SYNC] Cal.com sync complete: {results['calcom']} check-ins")

        print(f"[CHECKIN SYNC] Syncing Calendly events...")
        results["calendly"] = sync_calendly_events(db, org_id, user_id, new_calendly_bookings)
        print(f"[CHECKIN SYNC] Calendly sync complete: {results['calendly']} check-ins")
        
        results["total"] = results["calcom"] + results["calendly"]
        
        from app.models.client_checkin import ClientCheckIn
        affected = db.query(ClientCheckIn.client_id).filter(
            and_(
                ClientCheckIn.org_id == org_id,
                ClientCheckIn.updated_at >= sync_start_time
            )
        ).all()
        results["affected_client_ids"] = list({str(r[0]) for r in affected if r[0]})
        results["sync_org_id"] = str(org_id)

        # Fire pre-sale post-booking automation triggers for newly created rows. Done
        # AFTER the sync commit above so a failed enqueue can never roll back the
        # ClientCheckIn rows. Each call is independently committed; one failure does
        # not poison the others. Imported lazily to avoid an import cycle on cold
        # start (engine -> models -> session).
        try:
            from app.services.automation_engine import on_booking_created_pre_sale

            def _fire(provider: str, rows: List[Dict[str, Any]]) -> None:
                for row in rows:
                    try:
                        on_booking_created_pre_sale(
                            db,
                            org_id=org_id,
                            client_id=row["client_id"],
                            provider=provider,
                            external_booking_id=str(row["external_booking_id"]),
                            event_type_id=row.get("event_type_id"),
                            event_type_label=row.get("event_type_label"),
                            attendee_email=row.get("attendee_email"),
                            start_time=row.get("start_time"),
                        )
                        db.commit()
                    except Exception as fire_err:
                        db.rollback()
                        print(
                            f"[CHECKIN SYNC] post-booking automation enqueue failed "
                            f"({provider} {row.get('external_booking_id')}): {fire_err}"
                        )

            _fire("calcom", new_calcom_bookings)
            _fire("calendly", new_calendly_bookings)
            results["new_bookings_calcom"] = len(new_calcom_bookings)
            results["new_bookings_calendly"] = len(new_calendly_bookings)
        except Exception as auto_err:
            print(f"[CHECKIN SYNC] post-booking automation pass skipped: {auto_err}")

        if apply_pipeline_lifecycle_rules:
            try:
                from app.services.client_automation import run_pipeline_lifecycle_for_org

                pipeline_changes = run_pipeline_lifecycle_for_org(db, org_id)
                results["pipeline_lifecycle_changes"] = pipeline_changes
            except Exception as pipe_err:
                print(f"[CHECKIN SYNC] pipeline lifecycle pass skipped: {pipe_err}")
        else:
            results["pipeline_lifecycle_changes"] = 0

        print(f"[CHECKIN SYNC] ===== SYNC COMPLETE ======")
        print(f"[CHECKIN SYNC] Total: {results['total']} check-ins (Cal.com: {results['calcom']}, Calendly: {results['calendly']})")
        print(f"[CHECKIN SYNC] Affected clients for Fathom: {len(results['affected_client_ids'])}")
    except Exception as e:
        print(f"[CHECKIN SYNC] ❌ Error in sync_all_checkins: {e}")
        import traceback
        traceback.print_exc()
        raise

    return results

