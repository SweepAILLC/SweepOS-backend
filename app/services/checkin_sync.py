"""
Service to sync calendar events (Cal.com/Calendly) with clients by email matching.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, text, func
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
import uuid
import json
import re
from app.models.client import Client, LifecycleState
from app.models.client_checkin import ClientCheckIn
from app.models.calendar_booking_sales import CalendarBookingSales, EventTypeSalesCall
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.core.encryption import decrypt_token
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
    If none exists, create a new client as warm_lead so booking data can populate the client board.
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
    # Do not create an unnamed client card (email-only with no name)
    if not first_name and not last_name:
        print(f"[CHECKIN SYNC] Skipping client creation for unnamed attendee: {email}")
        return None
    client = Client(
        org_id=org_id,
        email=email.strip(),
        first_name=first_name,
        last_name=last_name,
        lifecycle_state=LifecycleState.WARM_LEAD,
        notes="Created from calendar booking (attendee)",
    )
    db.add(client)
    db.flush()
    print(f"[CHECKIN SYNC] Created new warm lead client for booking attendee: {email}")
    return client


def sync_calcom_bookings(db: Session, org_id: uuid.UUID, user_id: uuid.UUID) -> int:
    """
    Sync Cal.com bookings with clients by matching attendee emails.
    Returns the number of check-ins created/updated.
    
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

    # Get Cal.com OAuth token using raw SQL to bypass SQLAlchemy's enum name conversion
    # SQLAlchemy converts enum values to names (CALCOM) but database has lowercase (calcom)
    from sqlalchemy import text
    result = db.execute(
        text("""
            SELECT id, access_token, expires_at FROM oauth_tokens 
            WHERE provider = CAST('calcom' AS oauthprovider)
            AND org_id = :org_id 
            LIMIT 1
        """),
        {"org_id": org_id}
    ).first()
    
    if not result:
        print(f"[CHECKIN SYNC] [CALCOM] ❌ No Cal.com connection found for org {org_id}")
        return 0
    
    print(f"[CHECKIN SYNC] [CALCOM] ✅ Found Cal.com token")
    
    token_id, access_token_encrypted, expires_at = result[0], result[1], result[2]
    
    # Check if token is expired (only for OAuth tokens, not API keys)
    if expires_at and expires_at < datetime.utcnow():
        print(f"[CHECKIN SYNC] Cal.com token has expired for org {org_id}")
        return 0
    
    try:
        access_token = decrypt_token(
            access_token_encrypted,
            audit_context={
                "db": db,
                "org_id": org_id,
                "user_id": user_id,
                "resource_type": "calcom_token",
                "resource_id": str(token_id)
            }
        )
    except Exception as e:
        print(f"[CHECKIN SYNC] Failed to decrypt Cal.com token: {e}")
        return 0
    
    # Fetch bookings from Cal.com API
    # According to Cal.com API v2 docs: https://cal.com/docs/api-reference/v2/bookings/get-all-bookings
    # Must include cal-api-version header set to 2024-08-13, otherwise defaults to older version
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "cal-api-version": "2024-08-13"  # Required header for v2 API
    }

    email_index = _build_org_email_client_index(db, org_id)

    try:
        print(f"[CHECKIN SYNC] [CALCOM] Fetching ALL bookings from Cal.com API (past and future)...")
        # Cal.com API v2 defaults to only returning future/upcoming bookings
        # We need to fetch past and future separately or use status filters
        # Try fetching with status=all or without status filter to get all bookings
        all_bookings = []
        skip = 0
        take = 100
        max_iterations = 8  # Safety cap (8 * take bookings max per sync)

        cal_timeout = httpx.Timeout(25.0, connect=10.0)
        with httpx.Client(timeout=cal_timeout) as http_client:
            for iteration in range(max_iterations):
                print(f"[CHECKIN SYNC] [CALCOM] Fetching bookings batch {iteration + 1}: skip={skip}, take={take}")
                params = {
                    "take": take,
                    "skip": skip,
                    "status": "upcoming,past,cancelled",
                }
                response = http_client.get(
                    "https://api.cal.com/v2/bookings",
                    headers=headers,
                    params=params,
                )

                print(f"[CHECKIN SYNC] [CALCOM] Batch API response status: {response.status_code}")

                if response.status_code != 200:
                    print(f"[CHECKIN SYNC] [CALCOM] ❌ API error: {response.status_code}")
                    print(f"[CHECKIN SYNC] [CALCOM] Response: {response.text[:500]}")
                    break

                api_response = response.json()
                bookings_data = api_response.get("data", [])

                if isinstance(bookings_data, list):
                    batch_bookings = bookings_data
                elif isinstance(bookings_data, dict):
                    batch_bookings = bookings_data.get("bookings", [])
                else:
                    batch_bookings = []

                if not batch_bookings:
                    print(f"[CHECKIN SYNC] [CALCOM] No more bookings in batch {iteration + 1}")
                    break

                all_bookings.extend(batch_bookings)
                print(
                    f"[CHECKIN SYNC] [CALCOM] Batch {iteration + 1}: +{len(batch_bookings)} "
                    f"(total {len(all_bookings)})"
                )

                pagination = api_response.get("pagination", {})
                has_more = pagination.get("hasMore", False)
                if not has_more:
                    break

                skip += take

        print(f"[CHECKIN SYNC] [CALCOM] ✅ Fetched {len(all_bookings)} total bookings from API")
        
        # Check breakdown of past vs future bookings (same as calendar tab does client-side)
        past_count = 0
        future_count = 0
        for booking in all_bookings:
            # Use same field extraction as calendar tab: booking.get("start")
            start_time_str = booking.get("start") or booking.get("startTime")
            if start_time_str:
                try:
                    booking_date = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    now = datetime.now(booking_date.tzinfo)
                    if booking_date < now:
                        past_count += 1
                    else:
                        future_count += 1
                except:
                    pass
        
        print(f"[CHECKIN SYNC] [CALCOM] Bookings breakdown: {past_count} past, {future_count} future")
        print(f"[CHECKIN SYNC] [CALCOM] ✅ Processing {len(all_bookings)} total bookings (past and future)")
        
        # Use all_bookings instead of fetching again
        bookings = all_bookings
        
        if len(bookings) == 0:
            print(f"[CHECKIN SYNC] [CALCOM] ⚠️ No bookings found after fetching all pages")
            return 0
        
        print(f"[CHECKIN SYNC] [CALCOM] ✅ Processing {len(bookings)} total Cal.com bookings (past and future)")

        synced_count = 0
        bookings_without_attendees = 0
        bookings_without_matching_clients = 0
        
        for idx, booking in enumerate(bookings):
            try:
                # Handle case where booking might not be a dict
                if not isinstance(booking, dict):
                    print(f"[CHECKIN SYNC] [CALCOM] ⚠️ Booking {idx} is not a dict: {type(booking)}")
                    print(f"[CHECKIN SYNC] [CALCOM] Booking {idx} value: {str(booking)[:200]}")
                    continue
                
                # Extract event details
                event_id = str(booking.get("id", ""))
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
                
                start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                end_time = None
                if end_time_str:
                    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                
                # Extract attendee emails - Cal.com API structure
                # Attendees can be in "attendees" field or might be in other fields
                attendees = booking.get("attendees", [])
                
                # Also check for "guests" field which might contain emails
                if not attendees or not isinstance(attendees, list):
                    guests = booking.get("guests", [])
                    if isinstance(guests, list) and len(guests) > 0:
                        # Guests might be just email strings or dicts
                        print(f"[CHECKIN SYNC] [CALCOM] No attendees field, but found {len(guests)} guests")
                        attendees = [{"email": g} if isinstance(g, str) else g for g in guests]
                
                if not attendees or not isinstance(attendees, list) or len(attendees) == 0:
                    bookings_without_attendees += 1
                    continue

                attendees_list = list(attendees)

                # Process each attendee
                for attendee_idx, attendee in enumerate(attendees_list):
                    # Handle case where attendee might be a string (email)
                    if isinstance(attendee, str):
                        attendee_email = attendee
                        attendee = {"email": attendee_email}
                    elif not isinstance(attendee, dict):
                        print(f"[CHECKIN SYNC] [CALCOM] ⚠️ Attendee {attendee_idx} is not a dict or string: {type(attendee)} - {str(attendee)[:100]}")
                        continue
                    else:
                        attendee_email = attendee.get("email")
                    
                    if not attendee_email:
                        print(f"[CHECKIN SYNC] [CALCOM] Attendee {attendee_idx} has no email field")
                        print(f"[CHECKIN SYNC] [CALCOM] Attendee data: {attendee}")
                        continue
                    
                    attendee_name = attendee.get("name")
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
                    
                    # Check if check-in already exists
                    existing_checkin = db.query(ClientCheckIn).filter(
                        and_(
                            ClientCheckIn.org_id == org_id,
                            ClientCheckIn.event_id == event_id,
                            ClientCheckIn.provider == "calcom"
                        )
                    ).first()
                    
                    # Determine if completed (past event), cancelled, no-show
                    now = datetime.now(timezone.utc)
                    completed = start_time < now
                    cancelled = booking.get("status") == "cancelled" or booking.get("cancelled", False)
                    absent_host = booking.get("absentHost", False)
                    raw_attendees = booking.get("attendees") or []
                    attendee_absent = any(a.get("absent") for a in raw_attendees if isinstance(a, dict))
                    no_show = bool(absent_host or attendee_absent)
                    # Sales call flags from calendar_booking_sales or event_type_sales_calls
                    et = booking.get("eventType") or {}
                    event_type_id = str(et.get("id") or booking.get("eventTypeId") or "")
                    is_sales_call, sale_closed = get_sales_call_flags(db, org_id, "calcom", event_id, event_type_id or None)
                    
                    if existing_checkin:
                        # Update existing check-in — preserve manually-edited
                        # flags that the provider cannot know about.
                        existing_checkin.title = title
                        existing_checkin.start_time = start_time
                        existing_checkin.end_time = end_time
                        existing_checkin.location = location
                        existing_checkin.meeting_url = meeting_url
                        # Cancelled / no-show: provider True always wins; never overwrite CRM True with
                        # provider False (Event Details modal / board edits persist across sync).
                        if cancelled:
                            existing_checkin.cancelled = True
                        elif not getattr(existing_checkin, "cancelled", False):
                            existing_checkin.cancelled = False
                        if no_show:
                            existing_checkin.no_show = True
                        elif not getattr(existing_checkin, "no_show", False):
                            existing_checkin.no_show = False
                        # Only flip completed to True (past); never reset a
                        # manually-completed event back to False.
                        if completed and not existing_checkin.completed:
                            existing_checkin.completed = True
                        # Sales flags: keep the row's own values when they were
                        # set directly (via the modal); fall back to the sales-
                        # table / event-type lookup for new rows only.
                        if not getattr(existing_checkin, "is_sales_call", False) and is_sales_call:
                            existing_checkin.is_sales_call = True
                        if getattr(existing_checkin, "sale_closed", None) is None and sale_closed is not None:
                            existing_checkin.sale_closed = sale_closed
                        existing_checkin.updated_at = datetime.now(timezone.utc)
                        synced_count += 1
                    else:
                        # Create new check-in
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
                            completed=completed,
                            cancelled=cancelled,
                            no_show=no_show,
                            is_sales_call=is_sales_call,
                            sale_closed=sale_closed,
                            raw_event_data=json.dumps(booking)
                        )
                        db.add(checkin)
                        synced_count += 1
                
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


def sync_calendly_events(db: Session, org_id: uuid.UUID, user_id: uuid.UUID) -> int:
    """
    Sync Calendly events with clients by matching invitee emails.
    Returns the number of check-ins created/updated.
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

            print(f"[CHECKIN SYNC] [CALENDLY] Fetching scheduled events (paginated)...")
            events: List[dict] = []
            next_page_url: Optional[str] = None
            for page_idx in range(6):
                if page_idx == 0:
                    r = http.get(
                        "https://api.calendly.com/scheduled_events",
                        headers=headers,
                        params={"user": user_uri, "count": 100},
                    )
                else:
                    if not next_page_url:
                        break
                    r = http.get(next_page_url, headers=headers)

                if r.status_code != 200:
                    print(f"[CHECKIN SYNC] [CALENDLY] ❌ scheduled_events error: {r.status_code}")
                    print(f"[CHECKIN SYNC] [CALENDLY] Response: {r.text[:500]}")
                    return 0

                payload = r.json()
                batch = payload.get("collection", []) or []
                events.extend(batch)
                next_page_url = (payload.get("pagination") or {}).get("next_page")
                if not batch or not next_page_url:
                    break

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

        synced_count = 0
        events_without_invitees = 0
        events_without_matching_clients = 0

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

                start_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
                end_time = None
                if end_time_str:
                    end_time = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))

                meeting_url = None
                if isinstance(location, dict):
                    meeting_url = location.get("location") or location.get("join_url")
                elif isinstance(location, str):
                    meeting_url = location

                invitees = invitees_by_uri.get(event_uri, [])
                if not invitees:
                    events_without_invitees += 1
                    continue

                for invitee in invitees:
                    invitee_email = invitee.get("email")
                    if not invitee_email:
                        continue

                    invitee_name = invitee.get("name")
                    invitee_status = str(invitee.get("status") or "").lower()
                    invitee_no_show = invitee_status == "no_show"
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
                    
                    # Check if check-in already exists
                    existing_checkin = db.query(ClientCheckIn).filter(
                        and_(
                            ClientCheckIn.org_id == org_id,
                            ClientCheckIn.event_uri == event_uri,
                            ClientCheckIn.provider == "calendly"
                        )
                    ).first()
                    
                    # Determine if completed (past event), cancelled; no_show not available from Calendly event
                    now = datetime.now(timezone.utc)
                    completed = start_time < now
                    cancelled = status == "canceled"
                    no_show = invitee_no_show
                    event_type_uri = event.get("event_type") or ""
                    if isinstance(event_type_uri, dict):
                        event_type_uri = event_type_uri.get("uri") or ""
                    is_sales_call, sale_closed = get_sales_call_flags(db, org_id, "calendly", event_uuid, event_type_uri or None)
                    
                    if existing_checkin:
                        # Update existing check-in — preserve manually-edited
                        # flags (same logic as Cal.com branch above).
                        existing_checkin.title = name
                        existing_checkin.start_time = start_time
                        existing_checkin.end_time = end_time
                        existing_checkin.location = meeting_url
                        existing_checkin.meeting_url = meeting_url
                        if cancelled:
                            existing_checkin.cancelled = True
                        elif not getattr(existing_checkin, "cancelled", False):
                            existing_checkin.cancelled = False
                        if no_show:
                            existing_checkin.no_show = True
                        elif not getattr(existing_checkin, "no_show", False):
                            existing_checkin.no_show = False
                        if completed and not existing_checkin.completed:
                            existing_checkin.completed = True
                        # Sales flags: keep row values when already set.
                        if not getattr(existing_checkin, "is_sales_call", False) and is_sales_call:
                            existing_checkin.is_sales_call = True
                        if getattr(existing_checkin, "sale_closed", None) is None and sale_closed is not None:
                            existing_checkin.sale_closed = sale_closed
                        existing_checkin.updated_at = datetime.now(timezone.utc)
                        synced_count += 1
                    else:
                        # Create new check-in
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
                            completed=completed,
                            cancelled=cancelled,
                            no_show=no_show,
                            is_sales_call=is_sales_call,
                            sale_closed=sale_closed,
                            raw_event_data=json.dumps(event)
                        )
                        db.add(checkin)
                        synced_count += 1
                
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


def sync_all_checkins(db: Session, org_id: uuid.UUID, user_id: uuid.UUID) -> Dict[str, Any]:
    """Sync check-ins from all connected calendar providers"""
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
    
    try:
        # Ensure table (and optional columns) exist so sync works even if migrations weren't run
        print(f"[CHECKIN SYNC] Ensuring client_check_ins table exists...")
        _ensure_client_check_ins_table(db)
        print(f"[CHECKIN SYNC] ✅ Table ready")
        
        print(f"[CHECKIN SYNC] Syncing Cal.com bookings...")
        results["calcom"] = sync_calcom_bookings(db, org_id, user_id)
        print(f"[CHECKIN SYNC] Cal.com sync complete: {results['calcom']} check-ins")

        print(f"[CHECKIN SYNC] Syncing Calendly events...")
        results["calendly"] = sync_calendly_events(db, org_id, user_id)
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

        print(f"[CHECKIN SYNC] ===== SYNC COMPLETE ======")
        print(f"[CHECKIN SYNC] Total: {results['total']} check-ins (Cal.com: {results['calcom']}, Calendly: {results['calendly']})")
        print(f"[CHECKIN SYNC] Affected clients for Fathom: {len(results['affected_client_ids'])}")
    except Exception as e:
        print(f"[CHECKIN SYNC] ❌ Error in sync_all_checkins: {e}")
        import traceback
        traceback.print_exc()
        raise

    return results

