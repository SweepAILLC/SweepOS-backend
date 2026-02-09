"""
Service to sync calendar events (Cal.com/Calendly) with clients by email matching.
"""
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
import uuid
import json
import re
from app.models.client import Client
from app.models.client_checkin import ClientCheckIn
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.core.encryption import decrypt_token
import httpx
from app.core.config import settings


def normalize_email(email: str) -> str:
    """Normalize email for matching (lowercase, strip whitespace)"""
    if not email:
        return ""
    return re.sub(r'\s+', '', email.lower().strip())


def sync_calcom_bookings(db: Session, org_id: uuid.UUID, user_id: uuid.UUID) -> int:
    """
    Sync Cal.com bookings with clients by matching attendee emails.
    Returns the number of check-ins created/updated.
    
    NEW APPROACH: Also check existing check-ins in database for past events
    that may have been synced previously, and ensure they're up to date.
    """
    print(f"[CHECKIN SYNC] [CALCOM] Starting Cal.com sync for org {org_id}")
    
    # Check what check-ins we already have in the database (for deduplication)
    from app.models.client_checkin import ClientCheckIn
    from datetime import datetime, timezone
    
    existing_checkins = db.query(ClientCheckIn).filter(
        and_(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.provider == "calcom"
        )
    ).all()
    
    existing_event_ids = {checkin.event_id for checkin in existing_checkins}
    past_checkins = [c for c in existing_checkins if c.start_time and c.start_time < datetime.now(timezone.utc)]
    
    print(f"[CHECKIN SYNC] [CALCOM] Found {len(existing_checkins)} existing check-ins in database")
    print(f"[CHECKIN SYNC] [CALCOM]   - {len(past_checkins)} past check-ins")
    print(f"[CHECKIN SYNC] [CALCOM]   - {len(existing_checkins) - len(past_checkins)} future check-ins")
    print(f"[CHECKIN SYNC] [CALCOM] Already have {len(existing_event_ids)} unique event IDs in database")
    
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
    
    try:
        print(f"[CHECKIN SYNC] [CALCOM] Fetching ALL bookings from Cal.com API (past and future)...")
        # Cal.com API v2 defaults to only returning future/upcoming bookings
        # We need to fetch past and future separately or use status filters
        # Try fetching with status=all or without status filter to get all bookings
        all_bookings = []
        skip = 0
        take = 100
        max_iterations = 10  # Safety limit to prevent infinite loops
        
        # First, try fetching all bookings (may only return future by default)
        for iteration in range(max_iterations):
            print(f"[CHECKIN SYNC] [CALCOM] Fetching bookings batch {iteration + 1}: skip={skip}, take={take}")
            # According to Cal.com API v2 docs: https://cal.com/docs/api-reference/v2/bookings/get-all-bookings
            # Use status parameter to get both past and upcoming bookings
            # status can be: upcoming, recurring, past, cancelled, unconfirmed
            # Can pass multiple statuses separated by comma: "upcoming,past"
            params = {
                "take": take,
                "skip": skip,
                "status": "upcoming,past"  # Get both past and upcoming bookings
            }
            print(f"[CHECKIN SYNC] [CALCOM] Request params: {params}")
            response = httpx.get(
                "https://api.cal.com/v2/bookings",
                headers=headers,
                params=params,
                timeout=30.0
            )
            
            print(f"[CHECKIN SYNC] [CALCOM] Batch API response status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"[CHECKIN SYNC] [CALCOM] ❌ API error: {response.status_code}")
                print(f"[CHECKIN SYNC] [CALCOM] Response: {response.text[:500]}")
                break
            
            api_response = response.json()
            
            # Log response structure for debugging - compare with calendar tab
            print(f"[CHECKIN SYNC] [CALCOM] Batch {iteration + 1} API response keys: {list(api_response.keys())}")
            print(f"[CHECKIN SYNC] [CALCOM] Batch {iteration + 1} API response status: {api_response.get('status')}")
            
            # Extract bookings from response - use EXACT same logic as calendar tab endpoint
            # Calendar tab endpoint at integrations.py line 677 uses: bookings_data = api_response.get("data", [])
            bookings_data = api_response.get("data", [])
            
            print(f"[CHECKIN SYNC] [CALCOM] Batch {iteration + 1}: data type: {type(bookings_data)}")
            
            # Handle both response formats (we've seen both):
            # 1. data is a list: [booking1, booking2, ...] - this is what calendar tab expects
            # 2. data is a dict with bookings key: {"bookings": [booking1, booking2, ...]} - we've seen this too
            if isinstance(bookings_data, list):
                batch_bookings = bookings_data
                print(f"[CHECKIN SYNC] [CALCOM] Batch {iteration + 1}: data is list with {len(batch_bookings)} items")
            elif isinstance(bookings_data, dict):
                batch_bookings = bookings_data.get("bookings", [])
                print(f"[CHECKIN SYNC] [CALCOM] Batch {iteration + 1}: data is dict, extracted {len(batch_bookings)} from 'bookings' key")
            else:
                batch_bookings = []
                print(f"[CHECKIN SYNC] [CALCOM] Batch {iteration + 1}: data is unexpected type: {type(bookings_data)}")
            
            # Log first booking date if available to see if we're getting past events
            if len(batch_bookings) > 0:
                first_booking = batch_bookings[0]
                start_str = first_booking.get("start") or first_booking.get("startTime")
                if start_str:
                    try:
                        booking_date = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                        now = datetime.now(booking_date.tzinfo)
                        is_past = booking_date < now
                        print(f"[CHECKIN SYNC] [CALCOM] Batch {iteration + 1}: First booking date: {start_str} ({'PAST' if is_past else 'FUTURE'})")
                    except:
                        pass
            
            if not batch_bookings:
                print(f"[CHECKIN SYNC] [CALCOM] No more bookings in batch {iteration + 1}")
                break
            
            all_bookings.extend(batch_bookings)
            print(f"[CHECKIN SYNC] [CALCOM] Batch {iteration + 1}: Got {len(batch_bookings)} bookings (total so far: {len(all_bookings)})")
            
            # Check if there are more bookings (pagination)
            pagination = api_response.get("pagination", {})
            has_more = pagination.get("hasMore", False)
            total_items = pagination.get("totalItems", len(all_bookings))
            print(f"[CHECKIN SYNC] [CALCOM] Pagination: hasMore={has_more}, totalItems={total_items}")
            
            if not has_more:
                print(f"[CHECKIN SYNC] [CALCOM] No more bookings to fetch (hasMore: false)")
                break
            
            skip += take
        
        print(f"[CHECKIN SYNC] [CALCOM] ✅ Fetched {len(all_bookings)} total bookings from API")
        
        # Check breakdown of past vs future bookings (same as calendar tab does client-side)
        from datetime import datetime
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
        
        # Debug: Check the structure of the first booking
        if len(bookings) > 0:
            first_booking = bookings[0]
            print(f"[CHECKIN SYNC] [CALCOM] First booking type: {type(first_booking)}")
            if isinstance(first_booking, dict):
                print(f"[CHECKIN SYNC] [CALCOM] First booking keys: {list(first_booking.keys())}")
                # Log the full first booking for debugging
                import json
                print(f"[CHECKIN SYNC] [CALCOM] First booking full data: {json.dumps(first_booking, indent=2, default=str)[:1000]}")
            else:
                print(f"[CHECKIN SYNC] [CALCOM] First booking value: {str(first_booking)[:200]}")
        
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
                
                # Debug: Log booking structure for first few bookings
                if idx < 2:
                    import json
                    print(f"[CHECKIN SYNC] [CALCOM] Booking {idx} keys: {list(booking.keys())}")
                    print(f"[CHECKIN SYNC] [CALCOM] Booking {idx} attendees field: {booking.get('attendees', 'NOT FOUND')}")
                    print(f"[CHECKIN SYNC] [CALCOM] Booking {idx} full data: {json.dumps(booking, indent=2, default=str)[:1500]}")
                
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
                    print(f"[CHECKIN SYNC] [CALCOM] Booking {event_id} ({title}) has no attendees")
                    print(f"[CHECKIN SYNC] [CALCOM] Booking keys: {list(booking.keys())}")
                    print(f"[CHECKIN SYNC] [CALCOM] Attendees field: {booking.get('attendees')}")
                    print(f"[CHECKIN SYNC] [CALCOM] Guests field: {booking.get('guests')}")
                    continue
                
                print(f"[CHECKIN SYNC] [CALCOM] Processing booking {event_id} ({title}) with {len(attendees)} attendees")
                
                # Process each attendee
                for attendee_idx, attendee in enumerate(attendees):
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
                    print(f"[CHECKIN SYNC] [CALCOM] Looking for client with email: {attendee_email} (normalized: {normalized_email})")
                    
                    # Find matching client - query all clients and match by normalized email
                    all_clients = db.query(Client).filter(
                        and_(
                            Client.org_id == org_id,
                            Client.email.isnot(None)
                        )
                    ).all()
                    
                    print(f"[CHECKIN SYNC] [CALCOM] Found {len(all_clients)} clients with emails in org {org_id}")
                    
                    # Debug: Log first few client emails for comparison
                    if len(all_clients) > 0:
                        print(f"[CHECKIN SYNC] [CALCOM] Sample client emails (first 10):")
                        for i, c in enumerate(all_clients[:10]):
                            normalized_client_email = normalize_email(c.email) if c.email else None
                            match_status = "✅ MATCH" if normalized_client_email == normalized_email else "❌"
                            client_name = f"{c.first_name or ''} {c.last_name or ''}".strip() or "No name"
                            print(f"[CHECKIN SYNC] [CALCOM]   {i+1}. {client_name} - {c.email} (normalized: {normalized_client_email}) {match_status}")
                    
                    # Also check if we should search all clients (not just first 10)
                    if len(all_clients) > 10:
                        print(f"[CHECKIN SYNC] [CALCOM] ... and {len(all_clients) - 10} more clients")
                    
                    matching_client = None
                    for c in all_clients:
                        if c.email:
                            normalized_client_email = normalize_email(c.email)
                            if normalized_client_email == normalized_email:
                                matching_client = c
                                print(f"[CHECKIN SYNC] [CALCOM] ✅ Matched client: {c.first_name} {c.last_name} ({c.email})")
                                break
                    
                    if not matching_client:
                        bookings_without_matching_clients += 1
                        print(f"[CHECKIN SYNC] [CALCOM] ❌ No matching client found for attendee: {attendee_name or 'Unknown'} ({attendee_email})")
                        print(f"[CHECKIN SYNC] [CALCOM] This booking will be skipped - the attendee is not a client in this organization")
                        print(f"[CHECKIN SYNC] [CALCOM] Searched through {len(all_clients)} clients in org {org_id}")
                        continue
                    
                    # Check if check-in already exists
                    existing_checkin = db.query(ClientCheckIn).filter(
                        and_(
                            ClientCheckIn.org_id == org_id,
                            ClientCheckIn.event_id == event_id,
                            ClientCheckIn.provider == "calcom"
                        )
                    ).first()
                    
                    # Determine if completed (past event)
                    now = datetime.now(timezone.utc)
                    completed = start_time < now
                    cancelled = booking.get("status") == "cancelled" or booking.get("cancelled", False)
                    
                    if existing_checkin:
                        # Update existing check-in
                        existing_checkin.title = title
                        existing_checkin.start_time = start_time
                        existing_checkin.end_time = end_time
                        existing_checkin.location = location
                        existing_checkin.meeting_url = meeting_url
                        existing_checkin.completed = completed
                        existing_checkin.cancelled = cancelled
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
    
    try:
        # Get user URI first
        user_info_response = httpx.get(
            "https://api.calendly.com/users/me",
            headers=headers,
            timeout=10.0
        )
        
        if user_info_response.status_code != 200:
            print(f"[CHECKIN SYNC] Failed to get Calendly user info: {user_info_response.status_code}")
            return 0
        
        user_uri = user_info_response.json().get("resource", {}).get("uri")
        if not user_uri:
            print(f"[CHECKIN SYNC] No user URI found in Calendly response")
            return 0
        
        # Fetch scheduled events (both past and future)
        print(f"[CHECKIN SYNC] [CALENDLY] Fetching events from Calendly API...")
        response = httpx.get(
            "https://api.calendly.com/scheduled_events",
            headers=headers,
            params={"user": user_uri, "count": 100},
            timeout=30.0
        )
        
        print(f"[CHECKIN SYNC] [CALENDLY] API response status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"[CHECKIN SYNC] [CALENDLY] ❌ API error: {response.status_code}")
            print(f"[CHECKIN SYNC] [CALENDLY] Response: {response.text[:500]}")
            return 0
        
        events_data = response.json()
        events = events_data.get("collection", [])
        print(f"[CHECKIN SYNC] [CALENDLY] ✅ Found {len(events)} Calendly events")
        
        if len(events) == 0:
            print(f"[CHECKIN SYNC] [CALENDLY] ⚠️ No events found in API response")
            print(f"[CHECKIN SYNC] [CALENDLY] Response keys: {list(events_data.keys())}")
            return 0
        
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
                
                start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                end_time = None
                if end_time_str:
                    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                
                # Extract location URL if available
                meeting_url = None
                if isinstance(location, dict):
                    meeting_url = location.get("location") or location.get("join_url")
                elif isinstance(location, str):
                    meeting_url = location
                
                # Fetch invitees for this event
                invitees_response = httpx.get(
                    "https://api.calendly.com/event_invitees",
                    headers=headers,
                    params={"event": event_uri},
                    timeout=30.0
                )
                
                if invitees_response.status_code != 200:
                    print(f"[CHECKIN SYNC] Failed to fetch invitees for event {event_uuid}: {invitees_response.status_code}")
                    continue
                
                invitees_data = invitees_response.json()
                invitees = invitees_data.get("collection", [])
                print(f"[CHECKIN SYNC] Processing event {event_uuid} ({name}) with {len(invitees)} invitees")
                
                if not invitees:
                    events_without_invitees += 1
                    print(f"[CHECKIN SYNC] Event {event_uuid} ({name}) has no invitees")
                    continue
                
                for invitee in invitees:
                    invitee_email = invitee.get("email")
                    if not invitee_email:
                        print(f"[CHECKIN SYNC] Invitee has no email: {invitee}")
                        continue
                    
                    invitee_name = invitee.get("name")
                    normalized_email = normalize_email(invitee_email)
                    print(f"[CHECKIN SYNC] Looking for client with email: {invitee_email} (normalized: {normalized_email})")
                    
                    # Find matching client - query all clients and match by normalized email
                    all_clients = db.query(Client).filter(
                        and_(
                            Client.org_id == org_id,
                            Client.email.isnot(None)
                        )
                    ).all()
                    
                    print(f"[CHECKIN SYNC] Found {len(all_clients)} clients with emails in org {org_id}")
                    
                    matching_client = None
                    for c in all_clients:
                        if c.email and normalize_email(c.email) == normalized_email:
                            matching_client = c
                            print(f"[CHECKIN SYNC] ✅ Matched client: {c.first_name} {c.last_name} ({c.email})")
                            break
                    
                    if not matching_client:
                        events_without_matching_clients += 1
                        print(f"[CHECKIN SYNC] ❌ No matching client found for email: {invitee_email}")
                        continue
                    
                    # Check if check-in already exists
                    existing_checkin = db.query(ClientCheckIn).filter(
                        and_(
                            ClientCheckIn.org_id == org_id,
                            ClientCheckIn.event_uri == event_uri,
                            ClientCheckIn.provider == "calendly"
                        )
                    ).first()
                    
                    # Determine if completed (past event)
                    now = datetime.now(timezone.utc)
                    completed = start_time < now
                    cancelled = status == "canceled"
                    
                    if existing_checkin:
                        # Update existing check-in
                        existing_checkin.title = name
                        existing_checkin.start_time = start_time
                        existing_checkin.end_time = end_time
                        existing_checkin.location = meeting_url
                        existing_checkin.meeting_url = meeting_url
                        existing_checkin.completed = completed
                        existing_checkin.cancelled = cancelled
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


def sync_all_checkins(db: Session, org_id: uuid.UUID, user_id: uuid.UUID) -> Dict[str, int]:
    """Sync check-ins from all connected calendar providers"""
    print(f"[CHECKIN SYNC] ===== STARTING SYNC ======")
    print(f"[CHECKIN SYNC] Org ID: {org_id}, User ID: {user_id}")
    
    results = {
        "calcom": 0,
        "calendly": 0,
        "total": 0
    }
    
    try:
        # Check if table exists by trying a simple query
        print(f"[CHECKIN SYNC] Checking if client_check_ins table exists...")
        try:
            db.query(ClientCheckIn).limit(1).all()
            print(f"[CHECKIN SYNC] ✅ Table exists")
        except Exception as e:
            error_msg = str(e)
            print(f"[CHECKIN SYNC] ❌ Table check failed: {error_msg}")
            if "does not exist" in error_msg or "relation" in error_msg.lower():
                raise Exception(
                    "client_check_ins table does not exist. Please run database migration 018: "
                    "`make migrate-up` or `alembic upgrade head`"
                )
            raise
    
        print(f"[CHECKIN SYNC] Syncing Cal.com bookings...")
        results["calcom"] = sync_calcom_bookings(db, org_id, user_id)
        print(f"[CHECKIN SYNC] Cal.com sync complete: {results['calcom']} check-ins")
        
        print(f"[CHECKIN SYNC] Syncing Calendly events...")
        results["calendly"] = sync_calendly_events(db, org_id, user_id)
        print(f"[CHECKIN SYNC] Calendly sync complete: {results['calendly']} check-ins")
        
        results["total"] = results["calcom"] + results["calendly"]
        print(f"[CHECKIN SYNC] ===== SYNC COMPLETE ======")
        print(f"[CHECKIN SYNC] Total: {results['total']} check-ins (Cal.com: {results['calcom']}, Calendly: {results['calendly']})")
    except Exception as e:
        print(f"[CHECKIN SYNC] ❌ Error in sync_all_checkins: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    return results

