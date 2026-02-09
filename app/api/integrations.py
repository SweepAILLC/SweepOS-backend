from fastapi import APIRouter, Depends, HTTPException, Query, status, Request
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.schemas.integration import (
    BrevoStatus, CalComStatus, CalComBooking, CalComEventType,
    CalComBookingsResponse, CalComEventTypesResponse,
    CalendlyStatus, CalendlyScheduledEvent, CalendlyEventType,
    CalendlyScheduledEventsResponse, CalendlyEventTypesResponse,
    CalendarNotificationsSummary, CalendarUpcomingAppointment
)
from app.schemas.brevo import (
    BrevoContactCreate, BrevoContactUpdate, BrevoContactResponse, BrevoContactList,
    BrevoListCreate, BrevoListResponse, BrevoListList,
    BrevoMoveContactsRequest, BrevoAddContactsToListRequest, BrevoRemoveContactsFromListRequest, BrevoBulkDeleteContactsRequest, BrevoCreateClientsFromContactsRequest, BrevoListContactsRequest,
    BrevoSendEmailRequest, BrevoSendEmailResponse, BrevoEmailRecipient,
    BrevoAnalyticsResponse, BrevoAccountStatistics, BrevoTransactionalStatistics, BrevoCampaignStatistics
)
from app.models.client import Client, LifecycleState
from app.api.deps import get_current_user
from app.models.user import User
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.core.encryption import decrypt_token
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
import uuid
import httpx

router = APIRouter()


def get_brevo_auth_headers(
    db: Session,
    org_id: uuid.UUID,
    user_id: uuid.UUID
) -> dict:
    """
    Helper function to get Brevo authentication headers.
    Returns headers dict with appropriate auth (API key or OAuth token).
    """
    import uuid
    
    brevo_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.BREVO,
        OAuthToken.org_id == org_id
    ).first()
    
    if not brevo_token:
        raise HTTPException(
            status_code=401,
            detail="Brevo not connected. Please connect your Brevo account first."
        )
    
    # Check if token is expired (only for OAuth tokens, not API keys)
    is_expired = brevo_token.expires_at and brevo_token.expires_at < datetime.utcnow()
    if is_expired:
        raise HTTPException(
            status_code=401,
            detail="Brevo token has expired. Please reconnect your account."
        )
    
    # Decrypt the access token
    access_token = decrypt_token(
        brevo_token.access_token,
        audit_context={
            "db": db,
            "org_id": org_id,
            "user_id": user_id,
            "resource_type": "brevo_token",
            "resource_id": str(brevo_token.id)
        }
    )
    
    # Determine authentication method
    is_api_key = brevo_token.scope == "api_key"
    
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    
    if is_api_key:
        headers["api-key"] = access_token
    else:
        headers["Authorization"] = f"Bearer {access_token}"
    
    return headers


# NOTE: Stripe summary endpoint has been moved to backend/app/api/stripe.py
# This endpoint is kept for backward compatibility but should not be used
# The new endpoint at /integrations/stripe/summary uses real database data from webhooks


@router.get("/brevo/status", response_model=BrevoStatus)
def get_brevo_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Brevo connection status and account information.
    Fetches real account data from Brevo API if connected.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    brevo_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.BREVO,
        OAuthToken.org_id == org_id
    ).first()
    
    if not brevo_token:
        return BrevoStatus(
            connected=False,
            message="Brevo not connected. Click 'Install Brevo' to connect."
        )
    
    # Check if token is expired (only for OAuth tokens, not API keys)
    is_expired = brevo_token.expires_at and brevo_token.expires_at < datetime.utcnow()
    if is_expired:
        return BrevoStatus(
            connected=False,
            message="Brevo token has expired. Please reconnect your account."
        )
    
    # Fetch real account info from Brevo API
    try:
        # Decrypt the access token (could be OAuth token or API key)
        access_token = decrypt_token(
            brevo_token.access_token,
            audit_context={
                "db": db,
                "org_id": org_id,
                "user_id": current_user.id,
                "resource_type": "brevo_token",
                "resource_id": str(brevo_token.id)
            }
        )
        
        # Determine authentication method: API key uses 'api-key' header, OAuth uses 'Authorization: Bearer'
        is_api_key = brevo_token.scope == "api_key"
        
        # Call Brevo API to get account information
        # According to Brevo docs: GET https://api.brevo.com/v3/account
        # API keys use 'api-key' header, OAuth tokens use 'Authorization: Bearer'
        headers = {
            "accept": "application/json"
        }
        
        if is_api_key:
            # API key authentication
            headers["api-key"] = access_token
        else:
            # OAuth token authentication
            headers["Authorization"] = f"Bearer {access_token}"
        
        response = httpx.get(
            "https://api.brevo.com/v3/account",
            headers=headers,
            timeout=10.0
        )
        
        if response.status_code == 200:
            account_data = response.json()
            account_email = account_data.get("email")
            account_name = account_data.get("firstName") or account_data.get("companyName")
            if account_name and account_data.get("lastName"):
                account_name = f"{account_name} {account_data.get('lastName')}"
            
            return BrevoStatus(
                connected=True,
                account_email=account_email,
                account_name=account_name or "Connected Account",
                message="Brevo is connected and ready to use."
            )
        elif response.status_code == 401:
            # Token is invalid or expired
            return BrevoStatus(
                connected=False,
                message="Brevo token is invalid. Please reconnect your account."
            )
        else:
            # API error but token exists
            return BrevoStatus(
                connected=True,
                message=f"Brevo is connected but unable to fetch account details (API error: {response.status_code})."
            )
            
    except httpx.HTTPError as e:
        # Network error - token exists but can't verify
        return BrevoStatus(
            connected=True,
            message="Brevo is connected but unable to verify account details (network error)."
        )
    except Exception as e:
        # Other error - return basic status
        print(f"[BREVO] Error fetching account info: {str(e)}")
        return BrevoStatus(
            connected=True,
            message="Brevo is connected and ready to use."
        )


# Test route to verify routing works - can be removed later
@router.get("/calcom/test-route")
def test_calcom_route():
    return {"message": "Cal.com routes are working", "route": "/integrations/calcom/test-route"}

@router.get("/calcom/status", response_model=CalComStatus)
def get_calcom_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Cal.com connection status and account information.
    Fetches real account data from Cal.com API if connected.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    try:
        # Use raw SQL to bypass SQLAlchemy's enum name conversion
        # SQLAlchemy converts enum values to names (CALCOM) but database has lowercase (calcom)
        # Don't load the OAuthToken object - it will trigger enum validation
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
        
        # Create a minimal object-like structure without loading the full OAuthToken
        # This avoids SQLAlchemy enum validation
        calcom_token = None
        if result:
            # Create a simple object with just the fields we need
            class TokenProxy:
                def __init__(self, token_id, access_token, expires_at):
                    self.id = token_id
                    self.access_token = access_token
                    self.expires_at = expires_at
            calcom_token = TokenProxy(result[0], result[1], result[2])
    except Exception as db_error:
        # Catch database errors (e.g., enum mismatch)
        error_msg = str(db_error)
        print(f"[CALCOM STATUS] Database query error: {error_msg}")
        import traceback
        traceback.print_exc()
        # Return a safe response instead of raising
        return CalComStatus(
            connected=False,
            message=f"Database error checking Cal.com status. Please ensure migration 013 has been applied and backend has been restarted. Error: {error_msg[:100]}"
        )
    
    if not calcom_token:
        return CalComStatus(
            connected=False,
            message="Cal.com not connected. Click 'Connect Cal.com' to connect."
        )
    
    # Check if token is expired (only for OAuth tokens, not API keys)
    is_expired = calcom_token.expires_at and calcom_token.expires_at < datetime.utcnow()
    if is_expired:
        return CalComStatus(
            connected=False,
            message="Cal.com token has expired. Please reconnect your account."
        )
    
    # Fetch real account info from Cal.com API
    try:
        # Decrypt the access token (could be OAuth token or API key)
        access_token = decrypt_token(
            calcom_token.access_token,
            audit_context={
                "db": db,
                "org_id": org_id,
                "user_id": current_user.id,
                "resource_type": "calcom_token",
                "resource_id": str(calcom_token.id)
            }
        )
        
        # Call Cal.com API v2 to get user info
        # According to Cal.com API v2 docs: https://cal.com/docs/api-reference/v2/introduction
        # Authentication: Authorization: Bearer {API_KEY}
        # Endpoint: GET /me (under v2)
        response = httpx.get(
            "https://api.cal.com/v2/me",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            },
            timeout=10.0
        )
        
        if response.status_code == 200:
            account_data = response.json()
            # Cal.com v2 API returns user data in different structure
            # Check for email and name in various possible locations
            account_email = account_data.get("email") or account_data.get("username")
            account_name = account_data.get("name") or account_data.get("username")
            
            return CalComStatus(
                connected=True,
                account_email=account_email,
                account_name=account_name,
                message="Cal.com connected successfully."
            )
        elif response.status_code == 401:
            # Invalid API key
            return CalComStatus(
                connected=False,
                message="Cal.com API key is invalid. Please reconnect with a valid API key."
            )
        else:
            # Other API error
            error_text = response.text[:200] if response.text else "Unknown error"
            print(f"[CALCOM STATUS] API error {response.status_code}: {error_text}")
            return CalComStatus(
                connected=False,
                message=f"Failed to fetch Cal.com account information (HTTP {response.status_code}). Please reconnect."
            )
            
    except httpx.HTTPError as e:
        # Network error - token exists but can't verify
        print(f"[CALCOM STATUS] Network error: {str(e)}")
        return CalComStatus(
            connected=True,  # Token exists, so consider it connected
            message="Cal.com is connected but unable to verify account details (network error)."
        )
    except Exception as e:
        # Other error - return basic status
        print(f"[CALCOM STATUS] Error fetching account info: {str(e)}")
        import traceback
        traceback.print_exc()
        return CalComStatus(
            connected=True,  # Token exists, so consider it connected
            message="Cal.com is connected and ready to use."
        )


def get_calcom_auth_headers(
    db: Session,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    api_version: str = "2024-08-13"  # Default for bookings, can be overridden
) -> dict:
    """
    Helper function to get Cal.com authentication headers.
    Returns headers dict with API key in Authorization Bearer format.
    
    Args:
        db: Database session
        org_id: Organization ID
        user_id: User ID
        api_version: Cal.com API version (default: "2024-08-13" for bookings, "2024-06-14" for event types)
    """
    # Use raw SQL to bypass SQLAlchemy's enum name conversion
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
        raise HTTPException(
            status_code=401,
            detail="Cal.com not connected. Please connect your Cal.com account first."
        )
    
    token_id, access_token_encrypted, expires_at = result[0], result[1], result[2]
    
    # Check if token is expired (only for OAuth tokens, not API keys)
    if expires_at and expires_at < datetime.utcnow():
        raise HTTPException(
            status_code=401,
            detail="Cal.com token has expired. Please reconnect your account."
        )
    
    # Decrypt the access token
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
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "cal-api-version": api_version  # Required by Cal.com API v2
    }
    
    print(f"[CALCOM AUTH] Headers prepared with API version: {api_version}")
    print(f"[CALCOM AUTH] Authorization header present: {'Authorization' in headers}")
    print(f"[CALCOM AUTH] API version header: {headers.get('cal-api-version')}")
    
    return headers


# IMPORTANT: Using singular "booking" to avoid route conflicts with plural "bookings"
# FastAPI matches routes in order, but using a different base path is more reliable
# This route MUST be defined before /calcom/bookings to ensure proper matching
# Route path: /integrations/calcom/booking/{booking_id}
@router.get("/calcom/booking/{booking_id}")
def get_calcom_booking_details(
    booking_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get detailed information for a specific Cal.com booking, including form responses.
    This endpoint fetches:
    - Full booking details from Cal.com API
    - Routing form responses matched by booking UID (or email as fallback)
    - All pre-call client information
    
    Route: GET /integrations/calcom/booking/{booking_id}
    """
    print(f"[CALCOM BOOKING DETAILS] ===== ENDPOINT CALLED =====")
    print(f"[CALCOM BOOKING DETAILS] Route: /integrations/calcom/booking/{booking_id}")
    print(f"[CALCOM BOOKING DETAILS] Request URL: {request.url}")
    print(f"[CALCOM BOOKING DETAILS] Request path: {request.url.path}")
    print(f"[CALCOM BOOKING DETAILS] booking_id: {booking_id}, type: {type(booking_id)}")
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    print(f"[CALCOM BOOKING DETAILS] user: {current_user.id}, org: {org_id}")
    headers = get_calcom_auth_headers(db, org_id, current_user.id)
    
    try:
        # Fetch the specific booking
        response = httpx.get(
            f"https://api.cal.com/v2/bookings/{booking_id}",
            headers=headers,
            timeout=30.0
        )
        
        print(f"[CALCOM BOOKING DETAILS] Cal.com API response status: {response.status_code}")
        
        if response.status_code == 200:
            api_response = response.json()
            print(f"[CALCOM BOOKING DETAILS] API response keys: {list(api_response.keys())}")
            
            if api_response.get("status") != "success":
                error_msg = api_response.get("message", "Cal.com API returned error status")
                print(f"[CALCOM BOOKING DETAILS] API error: {error_msg}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Cal.com API error: {error_msg}"
                )
            
            booking_data = api_response.get("data", {})
            if not booking_data:
                print(f"[CALCOM BOOKING DETAILS] WARNING: No booking data in response")
                raise HTTPException(
                    status_code=404,
                    detail="Booking not found or no data returned"
                )
            
            print(f"[CALCOM BOOKING DETAILS] Booking data keys: {list(booking_data.keys())}")
            booking_uid = booking_data.get("uid")
            booking_email = None
            
            # Extract email from attendees
            attendees = booking_data.get("attendees", [])
            if attendees and len(attendees) > 0:
                booking_email = attendees[0].get("email")
            
            print(f"[CALCOM BOOKING DETAILS] Booking UID: {booking_uid}, Email: {booking_email}")
            
            # Fetch routing form responses if booking UID is available
            # Also try matching by email as fallback
            routing_form_responses = []
            if booking_uid or booking_email:
                try:
                    # First, get the user's organization info to get orgId
                    # Cal.com API: GET /me to get organization info
                    user_response = httpx.get(
                        "https://api.cal.com/v2/me",
                        headers=headers,
                        timeout=10.0
                    )
                    
                    if user_response.status_code == 200:
                        user_data = user_response.json()
                        if user_data.get("status") == "success":
                            user_info = user_data.get("data", {})
                            # Get organization ID - might be in different places
                            org_id = user_info.get("organizationId") or user_info.get("orgId") or user_info.get("organization", {}).get("id")
                            
                            if org_id:
                                print(f"[CALCOM BOOKING DETAILS] Found org ID: {org_id}, fetching routing form responses...")
                                
                                # Get all routing forms for the organization
                                # Cal.com API: GET /v2/organizations/{orgId}/routing-forms
                                routing_forms_response = httpx.get(
                                    f"https://api.cal.com/v2/organizations/{org_id}/routing-forms",
                                    headers=headers,
                                    timeout=10.0
                                )
                                
                                if routing_forms_response.status_code == 200:
                                    routing_forms_data = routing_forms_response.json()
                                    if routing_forms_data.get("status") == "success":
                                        routing_forms = routing_forms_data.get("data", [])
                                        
                                        # For each routing form, get responses filtered by booking UID or email
                                        for routing_form in routing_forms:
                                            routing_form_id = routing_form.get("id")
                                            if routing_form_id:
                                                try:
                                                    # Build query params - try UID first, then email
                                                    query_params = {}
                                                    if booking_uid:
                                                        query_params["routedToBookingUid"] = booking_uid
                                                    
                                                    form_responses_response = httpx.get(
                                                        f"https://api.cal.com/v2/organizations/{org_id}/routing-forms/{routing_form_id}/responses",
                                                        headers=headers,
                                                        params=query_params,
                                                        timeout=10.0
                                                    )
                                                    
                                                    if form_responses_response.status_code == 200:
                                                        form_responses_data = form_responses_response.json()
                                                        if form_responses_data.get("status") == "success":
                                                            # Cal.com API returns data in different structures
                                                            # Try both possible structures
                                                            data_obj = form_responses_data.get("data", {})
                                                            if isinstance(data_obj, list):
                                                                responses = data_obj
                                                            elif isinstance(data_obj, dict):
                                                                # Could be {"data": [...]} or direct array
                                                                responses = data_obj.get("data", []) if "data" in data_obj else []
                                                            else:
                                                                responses = []
                                                            
                                                            # Filter responses to only include those matching this booking UID or email
                                                            matching_responses = []
                                                            for r in responses:
                                                                # Match by booking UID (primary)
                                                                if booking_uid and r.get("routedToBookingUid") == booking_uid:
                                                                    matching_responses.append(r)
                                                                # Fallback: match by email if UID doesn't match
                                                                elif booking_email:
                                                                    # Check if response has email field that matches
                                                                    response_email = r.get("email") or r.get("submitterEmail") or r.get("formFillerEmail")
                                                                    if response_email and response_email.lower() == booking_email.lower():
                                                                        matching_responses.append(r)
                                                            
                                                            routing_form_responses.extend(matching_responses)
                                                            print(f"[CALCOM BOOKING DETAILS] Found {len(matching_responses)} matching routing form responses from form {routing_form_id} (out of {len(responses)} total)")
                                                            if matching_responses:
                                                                print(f"[CALCOM BOOKING DETAILS] Matched by: UID={booking_uid}, Email={booking_email}")
                                                except Exception as e:
                                                    print(f"[CALCOM BOOKING DETAILS] Error fetching responses for form {routing_form_id}: {e}")
                                                    continue
                            else:
                                print(f"[CALCOM BOOKING DETAILS] Could not find organization ID from user data")
                    else:
                        print(f"[CALCOM BOOKING DETAILS] Failed to get user info: {user_response.status_code}")
                except Exception as e:
                    print(f"[CALCOM BOOKING DETAILS] Error fetching routing form responses: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Transform to our schema format
            transformed_booking = {
                **booking_data,
                "startTime": booking_data.get("start"),
                "endTime": booking_data.get("end"),
                "user": booking_data.get("hosts", [{}])[0] if booking_data.get("hosts") else None,
                "eventType": booking_data.get("eventType", {}),
                "routingFormResponses": routing_form_responses  # Add routing form responses
            }
            
            try:
                result = CalComBooking(**transformed_booking)
                print(f"[CALCOM BOOKING DETAILS] Successfully fetched booking: {result.id} with {len(routing_form_responses)} routing form responses")
                # Return as dict to avoid Pydantic validation issues
                return result.model_dump() if hasattr(result, 'model_dump') else result.dict()
            except Exception as e:
                print(f"[CALCOM BOOKING DETAILS] Error creating CalComBooking object: {e}")
                print(f"[CALCOM BOOKING DETAILS] Transformed booking keys: {list(transformed_booking.keys())}")
                import traceback
                traceback.print_exc()
                # Return raw data if Pydantic validation fails
                return transformed_booking
        elif response.status_code == 404:
            print(f"[CALCOM BOOKING DETAILS] Booking {booking_id} not found in Cal.com API")
            raise HTTPException(
                status_code=404,
                detail=f"Booking {booking_id} not found"
            )
        else:
            error_text = response.text[:500] if hasattr(response, 'text') else "Unknown error"
            print(f"[CALCOM BOOKING DETAILS] Cal.com API error {response.status_code}: {error_text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch Cal.com booking details: {error_text}"
            )
    except HTTPException:
        raise
    except Exception as e:
        print(f"[CALCOM BOOKING DETAILS] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Cal.com booking details: {str(e)}"
        )


@router.get("/calcom/bookings", response_model=CalComBookingsResponse)
def get_calcom_bookings(
    take: int = Query(50, ge=1, le=100, alias="limit"),  # Support both 'limit' and 'take' for backward compatibility
    skip: int = Query(0, ge=0, alias="offset"),  # Support both 'offset' and 'skip' for backward compatibility
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Cal.com bookings for the connected account.
    According to Cal.com API v2: GET /bookings
    Docs: https://cal.com/docs/api-reference/v2/bookings/get-all-bookings
    
    NOTE: This route MUST come AFTER /calcom/booking/{booking_id} to avoid route conflicts.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    print(f"[CALCOM BOOKINGS LIST] ===== ENDPOINT CALLED =====")
    print(f"[CALCOM BOOKINGS LIST] This is the LIST endpoint, not the details endpoint")
    headers = get_calcom_auth_headers(db, org_id, current_user.id)
    
    try:
        # Cal.com API v2: GET /bookings
        # Query parameters: take (not limit), skip (not offset)
        # According to docs: https://cal.com/docs/api-reference/v2/bookings/get-all-bookings
        print(f"[CALCOM BOOKINGS] Making request to Cal.com API with take={take}, skip={skip}")
        response = httpx.get(
            "https://api.cal.com/v2/bookings",
            headers=headers,
            params={
                "take": take,
                "skip": skip
            },
            timeout=30.0
        )
        
        print(f"[CALCOM BOOKINGS] Response status: {response.status_code}")
        print(f"[CALCOM BOOKINGS] Response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            api_response = response.json()
            print(f"[CALCOM BOOKINGS] Raw API response: {api_response}")
            print(f"[CALCOM BOOKINGS] Response type: {type(api_response)}")
            
            # Cal.com API v2 returns: { "status": "success", "data": [...], "pagination": {...}, "error": {} }
            # According to docs: https://cal.com/docs/api-reference/v2/bookings/get-all-bookings
            if not isinstance(api_response, dict):
                raise HTTPException(
                    status_code=500,
                    detail="Unexpected response format from Cal.com API"
                )
            
            # Check status
            if api_response.get("status") != "success":
                error_msg = api_response.get("error", {}).get("message", "Unknown error from Cal.com API")
                raise HTTPException(
                    status_code=500,
                    detail=f"Cal.com API returned error status: {error_msg}"
                )
            
            # Extract bookings from 'data' array (not 'bookings')
            bookings_data = api_response.get("data", [])
            print(f"[CALCOM BOOKINGS] Found {len(bookings_data)} bookings in 'data' array")
            print(f"[CALCOM BOOKINGS] All keys in response: {list(api_response.keys())}")
            
            # Extract pagination info
            pagination = api_response.get("pagination", {})
            total_items = pagination.get("totalItems", len(bookings_data))
            print(f"[CALCOM BOOKINGS] Pagination info: {pagination}")
            
            # Parse bookings, handling any validation errors gracefully
            bookings = []
            for idx, booking in enumerate(bookings_data):
                try:
                    # Transform Cal.com API fields to our schema format
                    # Cal.com uses 'start'/'end', we use 'startTime'/'endTime'
                    transformed_booking = {
                        **booking,
                        "startTime": booking.get("start"),  # Map 'start' to 'startTime'
                        "endTime": booking.get("end"),  # Map 'end' to 'endTime'
                        "user": booking.get("hosts", [{}])[0] if booking.get("hosts") else None,  # Map 'hosts' to 'user'
                        "eventType": booking.get("eventType", {})  # Keep as is
                    }
                    print(f"[CALCOM BOOKINGS] Parsing booking {idx}: {booking.get('id', 'unknown')}")
                    bookings.append(CalComBooking(**transformed_booking))
                except Exception as e:
                    print(f"[CALCOM BOOKINGS] Warning: Failed to parse booking {booking.get('id', 'unknown')}: {e}")
                    print(f"[CALCOM BOOKINGS] Booking data: {booking}")
                    import traceback
                    traceback.print_exc()
                    # Continue with other bookings even if one fails
            
            print(f"[CALCOM BOOKINGS] Successfully parsed {len(bookings)} bookings")
            
            result = CalComBookingsResponse(
                bookings=bookings,
                total=total_items,
                nextCursor=None  # Cal.com uses pagination object, not cursor
            )
            print(f"[CALCOM BOOKINGS] Returning response with {len(result.bookings)} bookings, total: {result.total}")
            return result
        else:
            error_text = response.text[:200] if response.text else "Unknown error"
            print(f"[CALCOM BOOKINGS] API returned error status {response.status_code}: {error_text}")
            print(f"[CALCOM BOOKINGS] Full response text: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch Cal.com bookings: {error_text}"
            )
            
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Network error fetching Cal.com bookings: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Cal.com bookings: {str(e)}"
        )


@router.get("/calcom/event-types", response_model=CalComEventTypesResponse)
def get_calcom_event_types(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Cal.com event types for the connected account.
    According to Cal.com API v2: GET /event-types
    Docs: https://cal.com/docs/api-reference/v2/event-types/get-all-event-types
    Note: Event types endpoint uses cal-api-version: 2024-06-14 (different from bookings)
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    # Event types endpoint requires cal-api-version: 2024-06-14 (not 2024-08-13)
    headers = get_calcom_auth_headers(db, org_id, current_user.id, api_version="2024-06-14")
    
    try:
        # First, get the username from /me endpoint to use as query parameter
        # According to Cal.com docs, passing username gets all event types for that user
        username = None
        try:
            me_headers = get_calcom_auth_headers(db, org_id, current_user.id, api_version="2024-08-13")
            me_response = httpx.get(
                "https://api.cal.com/v2/me",
                headers=me_headers,
                timeout=10.0
            )
            if me_response.status_code == 200:
                me_data = me_response.json()
                username = me_data.get("username") or me_data.get("email")
                print(f"[CALCOM EVENT TYPES] Got username from /me: {username}")
        except Exception as e:
            print(f"[CALCOM EVENT TYPES] Warning: Could not get username from /me endpoint: {e}")
            # Continue without username - API should still work
        
        # Cal.com API v2: GET /event-types
        # According to docs, if username is provided, it gets all event types for that user
        url = "https://api.cal.com/v2/event-types"
        params = {}
        if username:
            params["username"] = username
            print(f"[CALCOM EVENT TYPES] Adding username parameter: {username}")
        
        print(f"[CALCOM EVENT TYPES] Making request to Cal.com API")
        print(f"[CALCOM EVENT TYPES] Request URL: {url}")
        print(f"[CALCOM EVENT TYPES] Request params: {params}")
        print(f"[CALCOM EVENT TYPES] Request headers: {headers}")
        print(f"[CALCOM EVENT TYPES] API version header value: {headers.get('cal-api-version')}")
        print(f"[CALCOM EVENT TYPES] Authorization header present: {'Authorization' in headers}")
        
        response = httpx.get(
            url,
            headers=headers,
            params=params if params else None,
            timeout=30.0
        )
        
        print(f"[CALCOM EVENT TYPES] Response status: {response.status_code}")
        print(f"[CALCOM EVENT TYPES] Response headers: {dict(response.headers)}")
        print(f"[CALCOM EVENT TYPES] Response text (first 500 chars): {response.text[:500]}")
        
        if response.status_code == 200:
            api_response = response.json()
            print(f"[CALCOM EVENT TYPES] Raw API response: {api_response}")
            print(f"[CALCOM EVENT TYPES] Response type: {type(api_response)}")
            
            # Cal.com API v2 returns: { "status": "success", "data": [...], "error": {} }
            # According to docs: https://cal.com/docs/api-reference/v2/event-types/get-all-event-types
            if not isinstance(api_response, dict):
                raise HTTPException(
                    status_code=500,
                    detail="Unexpected response format from Cal.com API"
                )
            
            # Check status
            if api_response.get("status") != "success":
                error_msg = api_response.get("error", {}).get("message", "Unknown error from Cal.com API")
                raise HTTPException(
                    status_code=500,
                    detail=f"Cal.com API returned error status: {error_msg}"
                )
            
            # Extract event types from 'data' array (not 'event_types')
            event_types_data = api_response.get("data", [])
            print(f"[CALCOM EVENT TYPES] Found {len(event_types_data)} event types in 'data' array")
            print(f"[CALCOM EVENT TYPES] All keys in response: {list(api_response.keys())}")
            
            if not isinstance(event_types_data, list):
                print(f"[CALCOM EVENT TYPES] Warning: event_types_data is not a list, it's {type(event_types_data)}")
                event_types_data = []
            
            # Parse event types, handling any validation errors gracefully
            event_types = []
            for idx, et in enumerate(event_types_data):
                try:
                    # Transform Cal.com API fields to our schema format
                    # Cal.com uses 'lengthInMinutes', we use 'length'
                    length_value = et.get("lengthInMinutes") or et.get("length")
                    transformed_event_type = {
                        **et,
                        "length": length_value,  # Map 'lengthInMinutes' to 'length'
                        "lengthInMinutes": et.get("lengthInMinutes")  # Keep original field too
                    }
                    
                    # Ensure required fields have defaults if missing
                    if "title" not in transformed_event_type:
                        transformed_event_type["title"] = f"Event Type {et.get('id', idx)}"
                    if "slug" not in transformed_event_type:
                        transformed_event_type["slug"] = f"event-type-{et.get('id', idx)}"
                    
                    print(f"[CALCOM EVENT TYPES] Parsing event type {idx}: ID={et.get('id', 'unknown')}, Title={et.get('title', 'N/A')}, Length={length_value}")
                    parsed_event = CalComEventType(**transformed_event_type)
                    event_types.append(parsed_event)
                    print(f"[CALCOM EVENT TYPES] Successfully parsed event type: {parsed_event.title}")
                except Exception as e:
                    print(f"[CALCOM EVENT TYPES] ERROR: Failed to parse event type {et.get('id', 'unknown')}: {e}")
                    print(f"[CALCOM EVENT TYPES] Error type: {type(e).__name__}")
                    # If it's a Pydantic ValidationError, print the detailed errors
                    if hasattr(e, 'errors'):
                        print(f"[CALCOM EVENT TYPES] Validation errors: {e.errors()}")
                    if hasattr(e, 'error_count'):
                        print(f"[CALCOM EVENT TYPES] Error count: {e.error_count()}")
                    print(f"[CALCOM EVENT TYPES] Event type raw data: {et}")
                    print(f"[CALCOM EVENT TYPES] Event type keys: {list(et.keys()) if isinstance(et, dict) else 'Not a dict'}")
                    print(f"[CALCOM EVENT TYPES] Transformed event type (before validation): {transformed_event_type}")
                    import traceback
                    traceback.print_exc()
                    # Continue with other event types even if one fails
            
            print(f"[CALCOM EVENT TYPES] Successfully parsed {len(event_types)} event types")
            
            result = CalComEventTypesResponse(event_types=event_types)
            print(f"[CALCOM EVENT TYPES] Returning response with {len(result.event_types)} event types")
            return result
        else:
            error_text = response.text[:500] if response.text else "Unknown error"
            print(f"[CALCOM EVENT TYPES] API returned error status {response.status_code}: {error_text}")
            print(f"[CALCOM EVENT TYPES] Full response text: {response.text}")
            print(f"[CALCOM EVENT TYPES] Response headers: {dict(response.headers)}")
            
            # Try to parse error response if it's JSON
            try:
                error_json = response.json()
                print(f"[CALCOM EVENT TYPES] Error JSON: {error_json}")
                error_msg = error_json.get("error", {}).get("message", error_text) if isinstance(error_json, dict) else error_text
            except:
                error_msg = error_text
            
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch Cal.com event types (HTTP {response.status_code}): {error_msg}"
            )
            
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Network error fetching Cal.com event types: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Cal.com event types: {str(e)}"
        )


# ============================================================================
# CALENDLY ENDPOINTS
# ============================================================================

@router.get("/calendly/status", response_model=CalendlyStatus)
def get_calendly_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Calendly connection status and account information.
    Fetches real account data from Calendly API if connected.
    """
    try:
        # Use raw SQL to bypass SQLAlchemy's enum name conversion
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
        
        calendly_token = None
        if result:
            class TokenProxy:
                def __init__(self, token_id, access_token, expires_at):
                    self.id = token_id
                    self.access_token = access_token
                    self.expires_at = expires_at
            calendly_token = TokenProxy(result[0], result[1], result[2])
    except Exception as db_error:
        error_msg = str(db_error)
        print(f"[CALENDLY STATUS] Database query error: {error_msg}")
        return CalendlyStatus(
            connected=False,
            message=f"Database error checking Calendly status. Please ensure migration 014 has been applied and backend has been restarted. Error: {error_msg[:100]}"
        )
    
    if not calendly_token:
        return CalendlyStatus(
            connected=False,
            message="Calendly not connected. Click 'Connect Calendly' to connect."
        )
    
    # Check if token is expired (only for OAuth tokens, not API keys)
    is_expired = calendly_token.expires_at and calendly_token.expires_at < datetime.utcnow()
    if is_expired:
        return CalendlyStatus(
            connected=False,
            message="Calendly token has expired. Please reconnect your account."
        )
    
    # Fetch real account info from Calendly API
    try:
        access_token = decrypt_token(
            calendly_token.access_token,
            audit_context={
                "db": db,
                "org_id": org_id,
                "user_id": current_user.id,
                "resource_type": "calendly_token",
                "resource_id": str(calendly_token.id)
            }
        )
        
        # Call Calendly API to get current user info
        # According to Calendly API docs: GET /users/me
        # Docs: https://developer.calendly.com/api-docs/d7755e2f9e5fe-calendly-api
        response = httpx.get(
            "https://api.calendly.com/users/me",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            },
            timeout=10.0
        )
        
        if response.status_code == 200:
            account_data = response.json()
            resource = account_data.get("resource", {})
            account_email = resource.get("email")
            account_name = resource.get("name") or resource.get("slug")
            
            return CalendlyStatus(
                connected=True,
                account_email=account_email,
                account_name=account_name or "Connected Account",
                message="Calendly connected successfully."
            )
        elif response.status_code == 401:
            return CalendlyStatus(
                connected=False,
                message="Calendly API key is invalid. Please reconnect with a valid API key."
            )
        else:
            error_text = response.text[:200] if response.text else "Unknown error"
            print(f"[CALENDLY STATUS] API error {response.status_code}: {error_text}")
            return CalendlyStatus(
                connected=False,
                message=f"Failed to fetch Calendly account information (HTTP {response.status_code}). Please reconnect."
            )
            
    except httpx.HTTPError as e:
        print(f"[CALENDLY STATUS] Network error: {str(e)}")
        return CalendlyStatus(
            connected=True,
            message="Calendly is connected but unable to verify account details (network error)."
        )
    except Exception as e:
        print(f"[CALENDLY STATUS] Error fetching account info: {str(e)}")
        return CalendlyStatus(
            connected=True,
            message="Calendly is connected and ready to use."
        )


def get_calendly_auth_headers(
    db: Session,
    org_id: uuid.UUID,
    user_id: uuid.UUID
) -> Tuple[dict, Optional[str]]:
    """
    Helper function to get Calendly authentication headers and user URI.
    Returns tuple of (headers dict, user_uri).
    
    Args:
        db: Database session
        org_id: Organization ID
        user_id: User ID
    
    Returns:
        Tuple of (headers dict with API key in Authorization Bearer format, user_uri)
    """
    from sqlalchemy import text
    result = db.execute(
        text("""
            SELECT id, access_token, expires_at, account_id FROM oauth_tokens 
            WHERE provider = 'calendly'::oauthprovider
            AND org_id = :org_id 
            LIMIT 1
        """),
        {"org_id": org_id}
    ).first()
    
    if not result:
        raise HTTPException(
            status_code=401,
            detail="Calendly not connected. Please connect your Calendly account first."
        )
    
    token_id, access_token_encrypted, expires_at, account_id = result[0], result[1], result[2], result[3]
    
    # Check if token is expired (only for OAuth tokens, not API keys)
    if expires_at and expires_at < datetime.utcnow():
        raise HTTPException(
            status_code=401,
            detail="Calendly token has expired. Please reconnect your account."
        )
    
    # Decrypt the access token
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
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # account_id should be the user URI from /users/me endpoint
    # Validate that it looks like a Calendly URI (starts with https://api.calendly.com/)
    user_uri = None
    if account_id and account_id != "unknown" and account_id.startswith("https://api.calendly.com/"):
        user_uri = account_id
    
    return headers, user_uri


@router.get("/calendly/scheduled-events", response_model=CalendlyScheduledEventsResponse)
def get_calendly_scheduled_events(
    count: int = Query(20, ge=1, le=100, description="Number of results per page"),
    page_token: Optional[str] = Query(None, description="Pagination token"),
    sort: Optional[str] = Query("start_time:asc", description="Sort order"),
    user: Optional[str] = Query(None, description="Filter by user URI"),
    invitee_email: Optional[str] = Query(None, description="Filter by invitee email"),
    status: Optional[str] = Query(None, description="Filter by status (active, canceled)"),
    min_start_time: Optional[str] = Query(None, description="Filter by minimum start time (ISO 8601)"),
    max_start_time: Optional[str] = Query(None, description="Filter by maximum start time (ISO 8601)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Calendly scheduled events (bookings) for the connected account.
    According to Calendly API: GET /scheduled_events
    Docs: https://developer.calendly.com/api-docs/d7755e2f9e5fe-calendly-api
    
    Note: Calendly API requires at least one of user, organization, or group parameter.
    If user is not provided, we automatically use the connected user's URI.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers, stored_user_uri = get_calendly_auth_headers(db, org_id, current_user.id)
    
    try:
        # Build query parameters
        params = {
            "count": count,
            "sort": sort
        }
        if page_token:
            params["page_token"] = page_token
        
        # Calendly API requires at least one of user, organization, or group
        # If user is not provided, use the stored user URI from the connection
        if user:
            params["user"] = user
        elif stored_user_uri:
            params["user"] = stored_user_uri
            print(f"[CALENDLY EVENTS] Using stored user URI: {stored_user_uri}")
        else:
            # If we don't have a stored user URI, fetch it from the API
            print(f"[CALENDLY EVENTS] No stored user URI, fetching from /users/me...")
            user_info_response = httpx.get(
                "https://api.calendly.com/users/me",
                headers=headers,
                timeout=10.0
            )
            if user_info_response.status_code == 200:
                user_info = user_info_response.json()
                user_uri = user_info.get("resource", {}).get("uri")
                if user_uri:
                    params["user"] = user_uri
                    print(f"[CALENDLY EVENTS] Fetched user URI from API: {user_uri}")
                else:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to get user URI from Calendly API. Please reconnect your account."
                    )
            else:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to fetch user information from Calendly API. Please reconnect your account."
                )
        if invitee_email:
            params["invitee_email"] = invitee_email
        if status:
            params["status"] = status
        if min_start_time:
            params["min_start_time"] = min_start_time
        if max_start_time:
            params["max_start_time"] = max_start_time
        
        print(f"[CALENDLY EVENTS] Making request to Calendly API with params: {params}")
        response = httpx.get(
            "https://api.calendly.com/scheduled_events",
            headers=headers,
            params=params,
            timeout=30.0
        )
        
        print(f"[CALENDLY EVENTS] Response status: {response.status_code}")
        
        if response.status_code == 200:
            api_response = response.json()
            print(f"[CALENDLY EVENTS] Raw API response keys: {list(api_response.keys())}")
            
            # Calendly API returns: { "collection": [...], "pagination": {...} }
            collection_data = api_response.get("collection", [])
            pagination_data = api_response.get("pagination", {})
            
            print(f"[CALENDLY EVENTS] Found {len(collection_data)} scheduled events")
            
            # Parse scheduled events
            scheduled_events = []
            for idx, event in enumerate(collection_data):
                try:
                    # Transform Calendly API fields to our schema format
                    transformed_event = {
                        **event,
                        "start_time": event.get("start_time"),  # ISO 8601
                        "end_time": event.get("end_time"),  # ISO 8601
                    }
                    print(f"[CALENDLY EVENTS] Parsing event {idx}: {event.get('uri', 'unknown')}")
                    scheduled_events.append(CalendlyScheduledEvent(**transformed_event))
                except Exception as e:
                    print(f"[CALENDLY EVENTS] Warning: Failed to parse event {event.get('uri', 'unknown')}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            print(f"[CALENDLY EVENTS] Successfully parsed {len(scheduled_events)} scheduled events")
            
            result = CalendlyScheduledEventsResponse(
                collection=scheduled_events,
                pagination=pagination_data
            )
            return result
        else:
            error_text = response.text[:500] if response.text else "Unknown error"
            print(f"[CALENDLY EVENTS] API returned error status {response.status_code}: {error_text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch Calendly scheduled events: {error_text}"
            )
            
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Network error fetching Calendly scheduled events: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Calendly scheduled events: {str(e)}"
        )


@router.get("/calendly/event-types", response_model=CalendlyEventTypesResponse)
def get_calendly_event_types(
    count: int = Query(20, ge=1, le=100, description="Number of results per page"),
    page_token: Optional[str] = Query(None, description="Pagination token"),
    sort: Optional[str] = Query("name:asc", description="Sort order"),
    user: Optional[str] = Query(None, description="Filter by user URI"),
    active: Optional[bool] = Query(None, description="Filter by active status"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Calendly event types for the connected account.
    According to Calendly API: GET /event_types
    Docs: https://developer.calendly.com/api-docs/d7755e2f9e5fe-calendly-api
    
    Note: Calendly API requires at least one of user or organization parameter.
    If user is not provided, we automatically use the connected user's URI.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers, stored_user_uri = get_calendly_auth_headers(db, org_id, current_user.id)
    
    try:
        # Build query parameters
        params = {
            "count": count,
            "sort": sort
        }
        if page_token:
            params["page_token"] = page_token
        
        # Calendly API requires at least one of user or organization
        # If user is not provided, use the stored user URI from the connection
        if user:
            params["user"] = user
        elif stored_user_uri:
            params["user"] = stored_user_uri
            print(f"[CALENDLY EVENT TYPES] Using stored user URI: {stored_user_uri}")
        else:
            # If we don't have a stored user URI, fetch it from the API
            print(f"[CALENDLY EVENT TYPES] No stored user URI, fetching from /users/me...")
            user_info_response = httpx.get(
                "https://api.calendly.com/users/me",
                headers=headers,
                timeout=10.0
            )
            if user_info_response.status_code == 200:
                user_info = user_info_response.json()
                user_uri = user_info.get("resource", {}).get("uri")
                if user_uri:
                    params["user"] = user_uri
                    print(f"[CALENDLY EVENT TYPES] Fetched user URI from API: {user_uri}")
                else:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to get user URI from Calendly API. Please reconnect your account."
                    )
            else:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to fetch user information from Calendly API. Please reconnect your account."
                )
        if active is not None:
            params["active"] = str(active).lower()
        
        print(f"[CALENDLY EVENT TYPES] Making request to Calendly API with params: {params}")
        response = httpx.get(
            "https://api.calendly.com/event_types",
            headers=headers,
            params=params,
            timeout=30.0
        )
        
        print(f"[CALENDLY EVENT TYPES] Response status: {response.status_code}")
        
        if response.status_code == 200:
            api_response = response.json()
            print(f"[CALENDLY EVENT TYPES] Raw API response keys: {list(api_response.keys())}")
            
            # Calendly API returns: { "collection": [...], "pagination": {...} }
            collection_data = api_response.get("collection", [])
            pagination_data = api_response.get("pagination", {})
            
            print(f"[CALENDLY EVENT TYPES] Found {len(collection_data)} event types")
            
            # Parse event types
            event_types = []
            for idx, event_type in enumerate(collection_data):
                try:
                    # Transform Calendly API fields to our schema format
                    transformed_event_type = {
                        **event_type,
                        "duration": event_type.get("duration"),  # Duration in minutes
                    }
                    print(f"[CALENDLY EVENT TYPES] Parsing event type {idx}: {event_type.get('uri', 'unknown')}")
                    event_types.append(CalendlyEventType(**transformed_event_type))
                except Exception as e:
                    print(f"[CALENDLY EVENT TYPES] Warning: Failed to parse event type {event_type.get('uri', 'unknown')}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            print(f"[CALENDLY EVENT TYPES] Successfully parsed {len(event_types)} event types")
            
            result = CalendlyEventTypesResponse(
                collection=event_types,
                pagination=pagination_data
            )
            return result
        else:
            error_text = response.text[:500] if response.text else "Unknown error"
            print(f"[CALENDLY EVENT TYPES] API returned error status {response.status_code}: {error_text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch Calendly event types: {error_text}"
            )
            
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Network error fetching Calendly event types: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Calendly event types: {str(e)}"
        )


# ============================================================================
# BREVO CONTACTS ENDPOINTS
# ============================================================================

@router.get("/brevo/contacts/by-email/{email}")
def get_brevo_contact_by_email(
    email: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get a single contact from Brevo by email.
    According to Brevo API, we can use email as identifier with identifierType=email_id
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        # URL encode the email (Brevo requires URL-encoded email addresses)
        from urllib.parse import quote
        encoded_email = quote(email, safe='')
        
        # Use email as identifier with identifierType=email_id
        response = httpx.get(
            f"https://api.brevo.com/v3/contacts/{encoded_email}?identifierType=email_id",
            headers=headers,
            timeout=30.0
        )
        
        if response.status_code == 200:
            contact_data = response.json()
            
            # If email is missing, try to get it from attributes
            if 'email' not in contact_data or not contact_data.get('email'):
                if contact_data.get('attributes'):
                    email_from_attrs = contact_data['attributes'].get('EMAIL') or contact_data['attributes'].get('email')
                    if email_from_attrs:
                        contact_data['email'] = email_from_attrs
            
            # Ensure email field exists (even if None)
            if 'email' not in contact_data:
                contact_data['email'] = None
            
            return BrevoContactResponse(**contact_data)
        elif response.status_code == 404:
            return None  # Contact doesn't exist
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch contact: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching contact: {str(e)}"
        )


@router.get("/brevo/contacts/{contact_id}", response_model=BrevoContactResponse)
def get_brevo_contact(
    contact_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get a single contact from Brevo by ID.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        response = httpx.get(
            f"https://api.brevo.com/v3/contacts/{contact_id}",
            headers=headers,
            timeout=30.0
        )
        
        if response.status_code == 200:
            contact_data = response.json()
            
            # If email is missing, try to get it from attributes
            if 'email' not in contact_data or not contact_data.get('email'):
                if contact_data.get('attributes'):
                    email = contact_data['attributes'].get('EMAIL') or contact_data['attributes'].get('email')
                    if email:
                        contact_data['email'] = email
            
            # Ensure email field exists (even if None)
            if 'email' not in contact_data:
                contact_data['email'] = None
            
            return BrevoContactResponse(**contact_data)
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch contact: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching contact: {str(e)}"
        )


@router.get("/brevo/contacts", response_model=BrevoContactList)
def get_brevo_contacts(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get list of contacts from Brevo.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        response = httpx.get(
            "https://api.brevo.com/v3/contacts",
            headers=headers,
            params={"limit": limit, "offset": offset},
            timeout=30.0
        )
        
        if response.status_code == 200:
            data = response.json()
            contacts_list = []
            
            for contact in data.get("contacts", []):
                # Brevo API might not always include email directly
                # Try to extract email from attributes if missing
                contact_data = contact.copy()
                
                # If email is missing, try to get it from attributes
                if 'email' not in contact_data or not contact_data.get('email'):
                    if contact_data.get('attributes'):
                        email = contact_data['attributes'].get('EMAIL') or contact_data['attributes'].get('email')
                        if email:
                            contact_data['email'] = email
                
                # Ensure email field exists (even if None)
                if 'email' not in contact_data:
                    contact_data['email'] = None
                
                try:
                    contacts_list.append(BrevoContactResponse(**contact_data))
                except Exception as e:
                    print(f"[BREVO] Failed to parse contact: {e}")
                    print(f"[BREVO] Contact data: {contact_data}")
                    # Skip invalid contacts but log the issue
                    continue
            
            return BrevoContactList(
                contacts=contacts_list,
                count=data.get("count", len(contacts_list)),
                offset=data.get("offset", offset),
                limit=data.get("limit", limit)
            )
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch contacts: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching contacts: {str(e)}"
        )


@router.post("/brevo/contacts", response_model=BrevoContactResponse)
def create_brevo_contact(
    contact: BrevoContactCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Create a new contact in Brevo.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        payload = {
            "email": contact.email,
            "updateEnabled": contact.updateEnabled
        }
        if contact.attributes:
            payload["attributes"] = contact.attributes
        if contact.listIds:
            payload["listIds"] = contact.listIds
        
        response = httpx.post(
            "https://api.brevo.com/v3/contacts",
            headers=headers,
            json=payload,
            timeout=30.0
        )
        
        if response.status_code in [201, 200]:
            data = response.json()
            return BrevoContactResponse(**data)
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to create contact: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error creating contact: {str(e)}"
        )


@router.put("/brevo/contacts/{identifier}")
def update_brevo_contact(
    identifier: str,
    contact: BrevoContactUpdate,
    identifier_type: Optional[str] = Query(None, alias="identifierType"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Update an existing contact in Brevo.
    
    According to Brevo API docs: https://developers.brevo.com/reference/update-contact
    - identifier can be email (urlencoded), contact ID, EXT_ID, SMS, WHATSAPP, or LANDLINE
    - identifierType: email_id, contact_id, ext_id, phone_id, whatsapp_id, landline_number_id
    - If identifierType is email_id or contact_id, you can omit the query parameter
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        # URL encode the identifier (especially important for email addresses)
        from urllib.parse import quote
        encoded_identifier = quote(identifier, safe='')
        
        # Build URL with optional identifierType query parameter
        url = f"https://api.brevo.com/v3/contacts/{encoded_identifier}"
        if identifier_type:
            url += f"?identifierType={identifier_type}"
        
        payload = {}
        if contact.attributes:
            payload["attributes"] = contact.attributes
        if contact.listIds:
            payload["listIds"] = contact.listIds
        if contact.unlinkListIds:
            payload["unlinkListIds"] = contact.unlinkListIds
        
        response = httpx.put(
            url,
            headers=headers,
            json=payload,
            timeout=30.0
        )
        
        if response.status_code == 204:
            # 204 No Content means success
            return {"success": True, "message": "Contact updated successfully"}
        elif response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail="Contact not found"
            )
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to update contact: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error updating contact: {str(e)}"
        )


@router.delete("/brevo/contacts/{contact_id}")
def delete_brevo_contact(
    contact_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a contact from Brevo.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        response = httpx.delete(
            f"https://api.brevo.com/v3/contacts/{contact_id}",
            headers=headers,
            timeout=30.0
        )
        
        if response.status_code in [204, 200]:
            return {"success": True, "message": "Contact deleted successfully"}
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to delete contact: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting contact: {str(e)}"
        )


@router.post("/brevo/contacts/bulk-delete")
def bulk_delete_brevo_contacts(
    request: BrevoBulkDeleteContactsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete multiple contacts from Brevo.
    This endpoint deletes contacts one by one and returns a summary of successes and failures.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    if not request.contactIds:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No contact IDs provided"
        )
    
    successful_deletes = []
    failed_deletes = []
    
    for contact_id in request.contactIds:
        try:
            response = httpx.delete(
                f"https://api.brevo.com/v3/contacts/{contact_id}",
                headers=headers,
                timeout=30.0
            )
            
            if response.status_code in [204, 200]:
                successful_deletes.append(contact_id)
            else:
                error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
                error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
                failed_deletes.append({"contact_id": contact_id, "error": error_msg})
        except Exception as e:
            failed_deletes.append({"contact_id": contact_id, "error": str(e)})
    
    # Return summary
    total = len(request.contactIds)
    success_count = len(successful_deletes)
    failed_count = len(failed_deletes)
    
    if failed_count == 0:
        return {
            "success": True,
            "message": f"Successfully deleted {success_count} contact(s)",
            "deleted_count": success_count,
            "total_count": total
        }
    elif success_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete all {failed_count} contact(s). First error: {failed_deletes[0]['error']}"
        )
    else:
        # Partial success
        return {
            "success": True,
            "message": f"Deleted {success_count} of {total} contact(s). {failed_count} failed.",
            "deleted_count": success_count,
            "failed_count": failed_count,
            "total_count": total,
            "failed_contacts": failed_deletes
        }


# ============================================================================
# BREVO LISTS ENDPOINTS
# ============================================================================

@router.get("/brevo/lists", response_model=BrevoListList)
def get_brevo_lists(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get list of contact lists from Brevo.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        response = httpx.get(
            "https://api.brevo.com/v3/contacts/lists",
            headers=headers,
            params={"limit": limit, "offset": offset},
            timeout=30.0
        )
        
        if response.status_code == 200:
            data = response.json()
            return BrevoListList(
                lists=[BrevoListResponse(**lst) for lst in data.get("lists", [])],
                count=data.get("count", 0),
                offset=data.get("offset", offset),
                limit=data.get("limit", limit)
            )
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch lists: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching lists: {str(e)}"
        )


@router.post("/brevo/lists", response_model=BrevoListResponse)
def create_brevo_list(
    list_data: BrevoListCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Create a new contact list in Brevo.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        payload = {"name": list_data.name}
        if list_data.folderId:
            payload["folderId"] = list_data.folderId
        
        response = httpx.post(
            "https://api.brevo.com/v3/contacts/lists",
            headers=headers,
            json=payload,
            timeout=30.0
        )
        
        if response.status_code in [201, 200]:
            data = response.json()
            return BrevoListResponse(**data)
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to create list: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error creating list: {str(e)}"
        )


@router.delete("/brevo/lists/{list_id}")
def delete_brevo_list(
    list_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a contact list from Brevo.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        response = httpx.delete(
            f"https://api.brevo.com/v3/contacts/lists/{list_id}",
            headers=headers,
            timeout=30.0
        )
        
        if response.status_code in [204, 200]:
            return {"success": True, "message": "List deleted successfully"}
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to delete list: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting list: {str(e)}"
        )


@router.get("/brevo/lists/{list_id}/contacts", response_model=BrevoContactList)
def get_brevo_list_contacts(
    list_id: int,
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get contacts in a specific list.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        response = httpx.get(
            f"https://api.brevo.com/v3/contacts/lists/{list_id}/contacts",
            headers=headers,
            params={"limit": limit, "offset": offset},
            timeout=30.0
        )
        
        if response.status_code == 200:
            data = response.json()
            contacts_list = []
            
            for contact in data.get("contacts", []):
                # Brevo API might not always include email directly
                # Try to extract email from attributes if missing
                contact_data = contact.copy()
                
                # If email is missing, try to get it from attributes
                if 'email' not in contact_data or not contact_data.get('email'):
                    if contact_data.get('attributes'):
                        email = contact_data['attributes'].get('EMAIL') or contact_data['attributes'].get('email')
                        if email:
                            contact_data['email'] = email
                
                # Ensure email field exists (even if None)
                if 'email' not in contact_data:
                    contact_data['email'] = None
                
                try:
                    contacts_list.append(BrevoContactResponse(**contact_data))
                except Exception as e:
                    print(f"[BREVO] Failed to parse list contact: {e}")
                    print(f"[BREVO] Contact data: {contact_data}")
                    # Skip invalid contacts but log the issue
                    continue
            
            return BrevoContactList(
                contacts=contacts_list,
                count=data.get("count", len(contacts_list)),
                offset=data.get("offset", offset),
                limit=data.get("limit", limit)
            )
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch list contacts: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching list contacts: {str(e)}"
        )


@router.post("/brevo/contacts/move")
def move_brevo_contacts(
    request: BrevoMoveContactsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Move contacts from one list to another.
    This removes contacts from the source list and adds them to the destination list.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        # Step 1: Remove contacts from source list
        # Brevo API accepts contact IDs in the remove endpoint
        unlink_response = httpx.post(
            f"https://api.brevo.com/v3/contacts/lists/{request.sourceListId}/contacts/remove",
            headers=headers,
            json={"ids": request.contactIds},
            timeout=30.0
        )
        
        if unlink_response.status_code not in [204, 200]:
            error_data = unlink_response.json() if unlink_response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {unlink_response.status_code}: {unlink_response.text}")
            raise HTTPException(
                status_code=unlink_response.status_code,
                detail=f"Failed to remove contacts from source list: {error_msg}"
            )
        
        # Step 2: Add contacts to destination list
        # Brevo API accepts contact IDs in the add endpoint
        link_response = httpx.post(
            f"https://api.brevo.com/v3/contacts/lists/{request.destinationListId}/contacts/add",
            headers=headers,
            json={"ids": request.contactIds},
            timeout=30.0
        )
        
        if link_response.status_code in [204, 200, 201]:
            return {
                "success": True,
                "message": f"Successfully moved {len(request.contactIds)} contact(s) from list {request.sourceListId} to list {request.destinationListId}"
            }
        else:
            error_data = link_response.json() if link_response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {link_response.status_code}: {link_response.text}")
            raise HTTPException(
                status_code=link_response.status_code,
                detail=f"Failed to add contacts to destination list: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error moving contacts: {str(e)}"
        )


@router.post("/brevo/contacts/add-to-list")
def add_contacts_to_list(
    request: BrevoAddContactsToListRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Add contacts to a list (without removing from source list).
    Use this when adding contacts from the contacts tab to a list.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        # Add contacts to the list
        # Brevo API accepts contact IDs in the add endpoint
        link_response = httpx.post(
            f"https://api.brevo.com/v3/contacts/lists/{request.listId}/contacts/add",
            headers=headers,
            json={"ids": request.contactIds},
            timeout=30.0
        )
        
        if link_response.status_code in [204, 200, 201]:
            return {
                "success": True,
                "message": f"Successfully added {len(request.contactIds)} contact(s) to list {request.listId}"
            }
        else:
            error_data = link_response.json() if link_response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {link_response.status_code}: {link_response.text}")
            raise HTTPException(
                status_code=link_response.status_code,
                detail=f"Failed to add contacts to list: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error adding contacts to list: {str(e)}"
        )


@router.post("/brevo/contacts/remove-from-list")
def remove_contacts_from_list(
    request: BrevoRemoveContactsFromListRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Remove contacts from a list (without moving them to another list).
    Use this when removing contacts from a list in the lists tab.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        # Remove contacts from the list
        # Brevo API accepts contact IDs in the remove endpoint
        unlink_response = httpx.post(
            f"https://api.brevo.com/v3/contacts/lists/{request.listId}/contacts/remove",
            headers=headers,
            json={"ids": request.contactIds},
            timeout=30.0
        )
        
        if unlink_response.status_code in [204, 200]:
            return {
                "success": True,
                "message": f"Successfully removed {len(request.contactIds)} contact(s) from list {request.listId}"
            }
        else:
            error_data = unlink_response.json() if unlink_response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {unlink_response.status_code}: {unlink_response.text}")
            raise HTTPException(
                status_code=unlink_response.status_code,
                detail=f"Failed to remove contacts from list: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error removing contacts from list: {str(e)}"
        )


@router.post("/brevo/contacts/create-clients")
def create_clients_from_brevo_contacts(
    request: BrevoCreateClientsFromContactsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Create client cards from selected Brevo contacts.
    Fetches contact details from Brevo and creates clients in the database.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    if not request.contactIds:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No contact IDs provided"
        )
    
    created_clients = []
    skipped_clients = []
    failed_contacts = []
    
    try:
        # Fetch contact details from Brevo
        for contact_id in request.contactIds:
            try:
                # Get contact details from Brevo
                response = httpx.get(
                    f"https://api.brevo.com/v3/contacts/{contact_id}",
                    headers=headers,
                    timeout=30.0
                )
                
                if response.status_code != 200:
                    error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
                    error_msg = error_data.get("message", f"HTTP {response.status_code}")
                    failed_contacts.append({"contact_id": contact_id, "error": error_msg})
                    continue
                
                contact_data = response.json()
                
                # Extract email
                email = contact_data.get("email")
                if not email:
                    # Try to get from attributes
                    attributes = contact_data.get("attributes", {})
                    email = attributes.get("EMAIL") or attributes.get("email")
                
                if not email:
                    skipped_clients.append({"contact_id": contact_id, "reason": "No email address"})
                    continue
                
                # Extract name from attributes
                attributes = contact_data.get("attributes", {})
                first_name = attributes.get("FIRSTNAME") or attributes.get("FIRST_NAME") or attributes.get("firstName")
                last_name = attributes.get("LASTNAME") or attributes.get("LAST_NAME") or attributes.get("lastName")
                phone = attributes.get("SMS") or attributes.get("PHONE") or attributes.get("phone")
                
                # Check if client already exists
                existing_client = db.query(Client).filter(
                    Client.email == email,
                    Client.org_id == org_id
                ).first()
                
                if existing_client:
                    # Merge: Update existing client with Brevo contact data
                    updated_fields = []
                    
                    if first_name and (not existing_client.first_name or existing_client.first_name.strip() == ""):
                        existing_client.first_name = first_name
                        updated_fields.append("first_name")
                    
                    if last_name and (not existing_client.last_name or existing_client.last_name.strip() == ""):
                        existing_client.last_name = last_name
                        updated_fields.append("last_name")
                    
                    if phone and (not existing_client.phone or existing_client.phone.strip() == ""):
                        existing_client.phone = phone
                        updated_fields.append("phone")
                    
                    # Update lifecycle_state to cold_lead if it's currently null or if we want to reset it
                    # Only update if it's not already in a more advanced state
                    if existing_client.lifecycle_state is None or existing_client.lifecycle_state == LifecycleState.COLD_LEAD:
                        existing_client.lifecycle_state = LifecycleState.COLD_LEAD
                        if "lifecycle_state" not in updated_fields:
                            updated_fields.append("lifecycle_state")
                    
                    # Add note about merge
                    from datetime import datetime
                    merge_note = f"Merged with Brevo contact (ID: {contact_id}) on {datetime.utcnow().isoformat()}"
                    if updated_fields:
                        merge_note += f". Updated fields: {', '.join(updated_fields)}"
                    
                    # Append to existing notes or create new
                    if existing_client.notes:
                        existing_client.notes = f"{existing_client.notes}\n{merge_note}"
                    else:
                        existing_client.notes = merge_note
                    
                    db.flush()  # Flush to save changes
                    
                    created_clients.append({
                        "contact_id": contact_id,
                        "client_id": str(existing_client.id),
                        "email": email,
                        "merged": True,
                        "updated_fields": updated_fields
                    })
                    continue
                
                # Create new client
                client = Client(
                    org_id=org_id,
                    email=email,
                    first_name=first_name,
                    last_name=last_name,
                    phone=phone,
                    lifecycle_state=LifecycleState.COLD_LEAD,
                    notes=f"Created from Brevo contact ID: {contact_id}"
                )
                
                db.add(client)
                db.flush()  # Flush to get the ID without committing
                
                created_clients.append({
                    "contact_id": contact_id,
                    "client_id": str(client.id),
                    "email": email,
                    "merged": False
                })
                
            except Exception as e:
                failed_contacts.append({"contact_id": contact_id, "error": str(e)})
                continue
        
        # Commit all created clients at once
        db.commit()
        
        # Count merged vs created
        merged_count = sum(1 for c in created_clients if c.get("merged", False))
        new_count = len(created_clients) - merged_count
        
        message_parts = []
        if new_count > 0:
            message_parts.append(f"Created {new_count} new client(s)")
        if merged_count > 0:
            message_parts.append(f"Merged {merged_count} existing client(s)")
        if skipped_clients:
            message_parts.append(f"Skipped {len(skipped_clients)} contact(s)")
        
        message = ", ".join(message_parts) if message_parts else "No clients processed"
        
        return {
            "success": True,
            "created_count": new_count,
            "merged_count": merged_count,
            "skipped_count": len(skipped_clients),
            "failed_count": len(failed_contacts),
            "total_count": len(request.contactIds),
            "created_clients": created_clients,
            "skipped_clients": skipped_clients,
            "failed_contacts": failed_contacts,
            "message": message
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating clients: {str(e)}"
        )


# ============================================================================
# BREVO TRANSACTIONAL EMAIL ENDPOINTS
# ============================================================================

@router.post("/brevo/transactional/send", response_model=BrevoSendEmailResponse)
def send_brevo_transactional_email(
    request: BrevoSendEmailRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Send transactional emails to contacts, lists, or direct recipients.
    Supports sending to:
    - Individual contacts (by contactIds)
    - Multiple contacts (by contactIds)
    - Entire lists (by listId)
    - Direct email addresses (by recipients)
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        # Collect all recipient emails
        recipient_emails = []
        
        # Option 1: Get emails from contact IDs
        if request.contactIds:
            for contact_id in request.contactIds:
                try:
                    contact_response = httpx.get(
                        f"https://api.brevo.com/v3/contacts/{contact_id}",
                        headers=headers,
                        timeout=10.0
                    )
                    if contact_response.status_code == 200:
                        contact_data = contact_response.json()
                        email = contact_data.get('email')
                        if not email and contact_data.get('attributes'):
                            email = contact_data['attributes'].get('EMAIL') or contact_data['attributes'].get('email')
                        
                        if email:
                            name = None
                            if contact_data.get('attributes'):
                                first_name = contact_data['attributes'].get('FIRSTNAME') or contact_data['attributes'].get('firstName')
                                last_name = contact_data['attributes'].get('LASTNAME') or contact_data['attributes'].get('lastName')
                                if first_name or last_name:
                                    name = f"{first_name or ''} {last_name or ''}".strip()
                            
                            recipient_emails.append({"email": email, "name": name})
                except Exception as e:
                    print(f"[BREVO] Failed to fetch contact {contact_id}: {e}")
                    continue
        
        # Option 2: Get emails from list
        elif request.listId:
            # Fetch all contacts from the list
            offset = 0
            limit = 50
            while True:
                try:
                    list_response = httpx.get(
                        f"https://api.brevo.com/v3/contacts/lists/{request.listId}/contacts",
                        headers=headers,
                        params={"limit": limit, "offset": offset},
                        timeout=30.0
                    )
                    
                    if list_response.status_code != 200:
                        break
                    
                    data = list_response.json()
                    contacts = data.get("contacts", [])
                    
                    if not contacts:
                        break
                    
                    for contact in contacts:
                        email = contact.get('email')
                        if not email and contact.get('attributes'):
                            email = contact['attributes'].get('EMAIL') or contact['attributes'].get('email')
                        
                        if email:
                            name = None
                            if contact.get('attributes'):
                                first_name = contact['attributes'].get('FIRSTNAME') or contact['attributes'].get('firstName')
                                last_name = contact['attributes'].get('LASTNAME') or contact['attributes'].get('lastName')
                                if first_name or last_name:
                                    name = f"{first_name or ''} {last_name or ''}".strip()
                            
                            recipient_emails.append({"email": email, "name": name})
                    
                    # Check if there are more contacts
                    if len(contacts) < limit:
                        break
                    
                    offset += limit
                except Exception as e:
                    print(f"[BREVO] Error fetching list contacts: {e}")
                    break
        
        # Option 3: Use direct recipients
        elif request.recipients:
            recipient_emails = [{"email": r.email, "name": r.name} for r in request.recipients]
        
        else:
            raise HTTPException(
                status_code=400,
                detail="Must specify either contactIds, listId, or recipients"
            )
        
        if not recipient_emails:
            raise HTTPException(
                status_code=400,
                detail="No valid email addresses found for recipients"
            )
        
        # Prepare email payload for Brevo API
        # Brevo supports sending to multiple recipients in a single call
        email_payload = {
            "sender": request.sender,
            "subject": request.subject,
            "to": recipient_emails[:100],  # Brevo limits to 100 recipients per call
        }
        
        # Add content (template or HTML/text)
        if request.templateId:
            email_payload["templateId"] = request.templateId
            if request.params:
                email_payload["params"] = request.params
        else:
            if request.htmlContent:
                email_payload["htmlContent"] = request.htmlContent
            if request.textContent:
                email_payload["textContent"] = request.textContent
        
        # Optional fields
        if request.tags:
            email_payload["tags"] = request.tags
        if request.replyTo:
            email_payload["replyTo"] = request.replyTo
        if request.attachments:
            email_payload["attachment"] = request.attachments
        
        # Send email(s) - handle batches if more than 100 recipients
        total_sent = 0
        message_ids = []
        
        for i in range(0, len(recipient_emails), 100):
            batch_recipients = recipient_emails[i:i+100]
            batch_payload = email_payload.copy()
            batch_payload["to"] = batch_recipients
            
            response = httpx.post(
                "https://api.brevo.com/v3/smtp/email",
                headers=headers,
                json=batch_payload,
                timeout=30.0
            )
            
            if response.status_code in [201, 200]:
                data = response.json()
                message_id = data.get("messageId")
                if message_id:
                    message_ids.append(message_id)
                total_sent += len(batch_recipients)
            else:
                error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
                error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to send email batch: {error_msg}"
                )
        
        return BrevoSendEmailResponse(
            success=True,
            messageId=message_ids[0] if message_ids else None,
            message=f"Successfully sent email to {total_sent} recipient(s)",
            recipientsCount=total_sent
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error sending transactional email: {str(e)}"
        )


@router.get("/brevo/senders")
def get_brevo_senders(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get list of verified senders from Brevo account.
    Returns verified email addresses and domains that can be used as senders.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        # Get verified senders
        response = httpx.get(
            "https://api.brevo.com/v3/senders",
            headers=headers,
            timeout=30.0
        )
        
        if response.status_code == 200:
            senders_data = response.json()
            senders = senders_data.get("senders", [])
            
            # Format senders for frontend
            formatted_senders = []
            for sender in senders:
                formatted_senders.append({
                    "id": sender.get("id"),
                    "name": sender.get("name", ""),
                    "email": sender.get("email", ""),
                    "active": sender.get("active", False),
                    "ips": sender.get("ips", []),
                    "domains": sender.get("domains", [])
                })
            
            return {
                "senders": formatted_senders,
                "count": len(formatted_senders)
            }
        else:
            error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            error_msg = error_data.get("message", f"HTTP {response.status_code}: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch senders: {error_msg}"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching senders: {str(e)}"
        )


@router.get("/brevo/analytics", response_model=BrevoAnalyticsResponse)
def get_brevo_analytics(
    period: str = Query("30days", description="Statistics period: 7days, 30days, 90days"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Brevo analytics and statistics.
    Fetches campaign statistics, transactional email statistics, and account-level metrics.
    """
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers = get_brevo_auth_headers(db, org_id, current_user.id)
    
    try:
        # Initialize response data
        account_stats = BrevoAccountStatistics()
        transactional_stats = BrevoTransactionalStatistics(period=period)
        campaigns_list = []
        
        # 1. Get account-level statistics (contacts, lists)
        try:
            # Get contacts count
            contacts_response = httpx.get(
                "https://api.brevo.com/v3/contacts",
                headers=headers,
                params={"limit": 1},
                timeout=30.0
            )
            if contacts_response.status_code == 200:
                contacts_data = contacts_response.json()
                account_stats.totalContacts = contacts_data.get("count", 0)
        except Exception as e:
            print(f"[BREVO ANALYTICS] Error fetching contacts count: {str(e)}")
        
        try:
            # Get lists count
            lists_response = httpx.get(
                "https://api.brevo.com/v3/contacts/lists",
                headers=headers,
                params={"limit": 1},
                timeout=30.0
            )
            if lists_response.status_code == 200:
                lists_data = lists_response.json()
                account_stats.totalLists = lists_data.get("count", 0)
        except Exception as e:
            print(f"[BREVO ANALYTICS] Error fetching lists count: {str(e)}")
        
        # 2. Get email campaigns and their statistics
        try:
            campaigns_response = httpx.get(
                "https://api.brevo.com/v3/emailCampaigns",
                headers=headers,
                params={"limit": 50, "sort": "desc"},
                timeout=30.0
            )
            if campaigns_response.status_code == 200:
                campaigns_data = campaigns_response.json()
                campaigns = campaigns_data.get("campaigns", [])
                account_stats.totalCampaigns = len(campaigns)
                
                # Get statistics for each campaign
                for campaign in campaigns[:10]:  # Limit to 10 most recent campaigns
                    campaign_id = campaign.get("id")
                    if campaign_id:
                        try:
                            stats_response = httpx.get(
                                f"https://api.brevo.com/v3/emailCampaigns/{campaign_id}/statistics",
                                headers=headers,
                                timeout=30.0
                            )
                            if stats_response.status_code == 200:
                                stats_data = stats_response.json()
                                
                                sent = stats_data.get("globalStats", {}).get("sent", 0)
                                delivered = stats_data.get("globalStats", {}).get("delivered", 0)
                                opened = stats_data.get("globalStats", {}).get("opened", 0)
                                unique_opens = stats_data.get("globalStats", {}).get("uniqueOpens", 0)
                                clicked = stats_data.get("globalStats", {}).get("clicks", 0)
                                unique_clicks = stats_data.get("globalStats", {}).get("uniqueClicks", 0)
                                bounced = stats_data.get("globalStats", {}).get("bounces", 0)
                                unsubscribed = stats_data.get("globalStats", {}).get("unsubscribed", 0)
                                spam_reports = stats_data.get("globalStats", {}).get("spamReports", 0)
                                
                                # Calculate rates
                                open_rate = (unique_opens / sent * 100) if sent > 0 else 0.0
                                click_rate = (unique_clicks / sent * 100) if sent > 0 else 0.0
                                bounce_rate = (bounced / sent * 100) if sent > 0 else 0.0
                                
                                campaign_stats = BrevoCampaignStatistics(
                                    campaignId=campaign_id,
                                    campaignName=campaign.get("name", ""),
                                    sent=sent,
                                    delivered=delivered,
                                    opened=opened,
                                    uniqueOpens=unique_opens,
                                    clicked=clicked,
                                    uniqueClicks=unique_clicks,
                                    bounced=bounced,
                                    unsubscribed=unsubscribed,
                                    spamReports=spam_reports,
                                    openRate=round(open_rate, 2),
                                    clickRate=round(click_rate, 2),
                                    bounceRate=round(bounce_rate, 2),
                                    createdAt=campaign.get("createdAt")
                                )
                                campaigns_list.append(campaign_stats)
                                
                                # Aggregate to account stats
                                account_stats.totalSent += sent
                                account_stats.totalDelivered += delivered
                                account_stats.totalOpened += opened
                                account_stats.totalClicked += clicked
                                account_stats.totalBounced += bounced
                                account_stats.totalUnsubscribed += unsubscribed
                        except Exception as e:
                            print(f"[BREVO ANALYTICS] Error fetching stats for campaign {campaign_id}: {str(e)}")
                            continue
        except Exception as e:
            print(f"[BREVO ANALYTICS] Error fetching campaigns: {str(e)}")
        
        # 3. Get transactional email statistics
        try:
            # Try to get transactional statistics
            # Note: Brevo API may have different endpoints for transactional stats
            # This is a common pattern, but may need adjustment based on actual API
            transactional_response = httpx.get(
                "https://api.brevo.com/v3/smtp/statistics",
                headers=headers,
                params={"days": period.replace("days", "") if period.endswith("days") else "30"},
                timeout=30.0
            )
            if transactional_response.status_code == 200:
                trans_data = transactional_response.json()
                
                # Brevo transactional stats structure may vary
                # Adjust based on actual API response
                transactional_stats.sent = trans_data.get("sent", 0)
                transactional_stats.delivered = trans_data.get("delivered", 0)
                transactional_stats.opened = trans_data.get("opened", 0)
                transactional_stats.uniqueOpens = trans_data.get("uniqueOpens", 0)
                transactional_stats.clicked = trans_data.get("clicked", 0)
                transactional_stats.uniqueClicks = trans_data.get("uniqueClicks", 0)
                transactional_stats.bounced = trans_data.get("bounced", 0)
                transactional_stats.spamReports = trans_data.get("spamReports", 0)
                
                # Calculate rates
                if transactional_stats.sent > 0:
                    transactional_stats.openRate = round((transactional_stats.uniqueOpens / transactional_stats.sent) * 100, 2)
                    transactional_stats.clickRate = round((transactional_stats.uniqueClicks / transactional_stats.sent) * 100, 2)
                    transactional_stats.bounceRate = round((transactional_stats.bounced / transactional_stats.sent) * 100, 2)
        except Exception as e:
            print(f"[BREVO ANALYTICS] Error fetching transactional statistics: {str(e)}")
            # Transactional stats endpoint may not exist or have different structure
            # Continue without failing
        
        # Calculate overall rates for account stats
        if account_stats.totalSent > 0:
            account_stats.overallOpenRate = round((account_stats.totalOpened / account_stats.totalSent) * 100, 2)
            account_stats.overallClickRate = round((account_stats.totalClicked / account_stats.totalSent) * 100, 2)
            account_stats.overallBounceRate = round((account_stats.totalBounced / account_stats.totalSent) * 100, 2)
        
        return BrevoAnalyticsResponse(
            account=account_stats,
            transactional=transactional_stats,
            campaigns=campaigns_list,
            lastUpdated=datetime.utcnow().isoformat(),
            period=period
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[BREVO ANALYTICS] Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Brevo analytics: {str(e)}"
        )


@router.get("/calendar/upcoming-summary", response_model=CalendarNotificationsSummary)
def get_calendar_upcoming_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get summary of upcoming calendar appointments with comparisons.
    Works with both Cal.com and Calendly.
    Returns:
    - Count of upcoming appointments
    - Comparison with last week and last month
    - Details of the most upcoming appointment
    """
    from sqlalchemy import text
    
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    print(f"[CALENDAR SUMMARY] Request from user {current_user.id}, org {org_id}")
    
    # Check which calendar provider is connected (use selected org from token)
    calcom_result = db.execute(
        text("""
            SELECT id FROM oauth_tokens 
            WHERE provider = 'calcom'::oauthprovider
            AND org_id = :org_id 
            LIMIT 1
        """),
        {"org_id": org_id}
    ).first()
    
    calendly_result = db.execute(
        text("""
            SELECT id FROM oauth_tokens 
            WHERE provider = 'calendly'::oauthprovider
            AND org_id = :org_id 
            LIMIT 1
        """),
        {"org_id": org_id}
    ).first()
    
    print(f"[CALENDAR SUMMARY] Cal.com connected: {calcom_result is not None}")
    print(f"[CALENDAR SUMMARY] Calendly connected: {calendly_result is not None}")
    
    provider = None
    if calcom_result:
        provider = "calcom"
    elif calendly_result:
        provider = "calendly"
    
    if not provider:
        # No calendar provider connected
        print(f"[CALENDAR SUMMARY] No calendar provider connected, returning empty summary")
        return CalendarNotificationsSummary(
            upcoming_count=0,
            last_week_count=0,
            last_month_count=0,
            last_week_percentage_change=None,
            last_month_percentage_change=None,
            most_upcoming=None,
            provider=None,
            connected=False
        )
    
    try:
        from datetime import timezone
        # Use timezone-aware datetime for proper comparisons
        now = datetime.now(timezone.utc)
        one_week_ago = now - timedelta(days=7)
        one_month_ago = now - timedelta(days=30)
        one_week_before_that = one_week_ago - timedelta(days=7)
        one_month_before_that = one_month_ago - timedelta(days=30)
        
        print(f"[CALENDAR SUMMARY] Date ranges (UTC):")
        print(f"  - Now: {now}")
        print(f"  - One week ago: {one_week_ago}")
        print(f"  - One month ago: {one_month_ago}")
        print(f"  - One week before that: {one_week_before_that}")
        print(f"  - One month before that: {one_month_before_that}")
        
        # Fetch appointments based on provider - use same logic as existing endpoints
        all_bookings = []
        
        if provider == "calcom":
            # Use selected org_id from outer scope (set in get_calendar_upcoming_summary)
            headers = get_calcom_auth_headers(db, org_id, current_user.id)
            
            # Fetch bookings with pagination - get more to ensure we have enough data
            skip = 0
            take = 100
            total_fetched = 0
            max_fetches = 2  # Limit to 200 bookings max to prevent timeout
            
            while skip < max_fetches * take:
                print(f"[CALENDAR SUMMARY] Fetching Cal.com bookings: skip={skip}, take={take}")
                try:
                    response = httpx.get(
                        "https://api.cal.com/v2/bookings",
                        headers=headers,
                        params={"take": take, "skip": skip},
                        timeout=10.0  # Restored original timeout
                    )
                except httpx.TimeoutException:
                    print(f"[CALENDAR SUMMARY] Cal.com API timeout at skip={skip}")
                    break
                except Exception as e:
                    print(f"[CALENDAR SUMMARY] Cal.com API error: {str(e)}")
                    break
                
                if response.status_code != 200:
                    print(f"[CALENDAR SUMMARY] Cal.com API error: {response.status_code}")
                    break
                
                api_response = response.json()
                if api_response.get("status") != "success":
                    print(f"[CALENDAR SUMMARY] Cal.com API returned error status")
                    break
                
                bookings_data = api_response.get("data", [])
                print(f"[CALENDAR SUMMARY] Fetched {len(bookings_data)} Cal.com bookings")
                
                if not bookings_data:
                    break
                
                # Parse bookings using same logic as get_calcom_bookings
                for booking in bookings_data:
                    try:
                        start_str = booking.get("start")
                        if not start_str:
                            continue
                        
                        # Parse the start time
                        start_time = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                        
                        # Create unique ID for deduplication
                        booking_id = str(booking.get("id")) or booking.get("uid", "")
                        
                        all_bookings.append({
                            "id": booking_id,
                            "title": booking.get("title") or booking.get("eventType", {}).get("title") or "Untitled Event",
                            "start_time": start_str,
                            "end_time": booking.get("end"),
                            "link": f"https://cal.com/bookings/{booking.get('uid', booking.get('id'))}",
                            "attendees": booking.get("attendees", []),
                            "location": booking.get("location"),
                            "start_datetime": start_time  # Store parsed datetime for filtering
                        })
                    except Exception as e:
                        print(f"[CALENDAR SUMMARY] Error parsing Cal.com booking: {e}")
                        import traceback
                        traceback.print_exc()
                        continue
                
                total_fetched += len(bookings_data)
                
                # Check if there are more pages
                pagination = api_response.get("pagination", {})
                if not pagination.get("nextCursor") or len(bookings_data) < take:
                    break
                
                skip += take
            
            print(f"[CALENDAR SUMMARY] Total Cal.com bookings fetched: {len(all_bookings)}")
        
        else:  # calendly
            # Use selected org_id from outer scope (set in get_calendar_upcoming_summary)
            headers, stored_user_uri = get_calendly_auth_headers(db, org_id, current_user.id)
            
            if not stored_user_uri:
                # Fetch user URI if not stored
                user_info_response = httpx.get(
                    "https://api.calendly.com/users/me",
                    headers=headers,
                    timeout=10.0  # Restored original timeout
                )
                if user_info_response.status_code == 200:
                    user_info = user_info_response.json()
                    stored_user_uri = user_info.get("resource", {}).get("uri")
            
            if not stored_user_uri:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to get user URI from Calendly"
                )
            
            # Fetch scheduled events with pagination
            page_token = None
            count = 100
            max_pages = 2  # Limit to 200 events max to prevent timeout
            
            for page_num in range(max_pages):
                params = {
                    "count": count,
                    "sort": "start_time:asc",
                    "user": stored_user_uri
                }
                if page_token:
                    params["page_token"] = page_token
                
                print(f"[CALENDAR SUMMARY] Fetching Calendly events: page={page_num + 1}")
                try:
                    response = httpx.get(
                        "https://api.calendly.com/scheduled_events",
                        headers=headers,
                        params=params,
                        timeout=10.0  # Restored original timeout
                    )
                except httpx.TimeoutException:
                    print(f"[CALENDAR SUMMARY] Calendly API timeout at page={page_num + 1}")
                    break
                except Exception as e:
                    print(f"[CALENDAR SUMMARY] Calendly API error: {str(e)}")
                    break
                
                if response.status_code != 200:
                    print(f"[CALENDAR SUMMARY] Calendly API error: {response.status_code}")
                    break
                
                api_response = response.json()
                events_data = api_response.get("collection", [])
                print(f"[CALENDAR SUMMARY] Fetched {len(events_data)} Calendly events")
                
                if not events_data:
                    break
                
                # Parse events using same logic as get_calendly_scheduled_events
                for event in events_data:
                    try:
                        start_str = event.get("start_time")
                        if not start_str:
                            continue
                        
                        # Parse the start time
                        start_time = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                        
                        # Create unique ID for deduplication
                        event_uri = event.get("uri", "")
                        event_id = event_uri.split("/")[-1] if event_uri else None
                        
                        all_bookings.append({
                            "id": event_id,
                            "title": event.get("name") or "Untitled Event",
                            "start_time": start_str,
                            "end_time": event.get("end_time"),
                            "link": event_uri,
                            "attendees": [],  # Would need separate API call for invitees
                            "location": event.get("location", {}).get("location") if isinstance(event.get("location"), dict) else event.get("location"),
                            "start_datetime": start_time  # Store parsed datetime for filtering
                        })
                    except Exception as e:
                        print(f"[CALENDAR SUMMARY] Error parsing Calendly event: {e}")
                        import traceback
                        traceback.print_exc()
                        continue
                
                # Check for next page
                pagination = api_response.get("pagination", {})
                page_token = pagination.get("next_page_token")
                if not page_token:
                    break
            
            print(f"[CALENDAR SUMMARY] Total Calendly events fetched: {len(all_bookings)}")
        
        # Fetch manual check-ins from database and add to all_bookings
        from app.models.client_checkin import ClientCheckIn
        from sqlalchemy.orm import joinedload
        manual_check_ins = db.query(ClientCheckIn).options(
            joinedload(ClientCheckIn.client)
        ).filter(
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.completed == False,
            ClientCheckIn.cancelled == False
        ).order_by(ClientCheckIn.start_time).all()
        
        print(f"[CALENDAR SUMMARY] Found {len(manual_check_ins)} manual check-ins")
        
        # Convert manual check-ins to booking format
        for check_in in manual_check_ins:
            if check_in.start_time:
                start_time_aware = check_in.start_time
                if start_time_aware.tzinfo is None:
                    from datetime import timezone
                    start_time_aware = start_time_aware.replace(tzinfo=timezone.utc)
                
                # Get client name if available
                client_name = None
                if check_in.client:
                    client_name = f"{check_in.client.first_name or ''} {check_in.client.last_name or ''}".strip()
                    if not client_name:
                        client_name = check_in.client.email
                elif check_in.attendee_name:
                    client_name = check_in.attendee_name
                
                all_bookings.append({
                    "id": f"manual_{check_in.id}",
                    "title": check_in.title or "Manual Check-In",
                    "start_time": check_in.start_time.isoformat(),
                    "end_time": check_in.end_time.isoformat() if check_in.end_time else None,
                    "link": None,  # Manual check-ins don't have external links
                    "attendees": [{"email": check_in.attendee_email, "name": check_in.attendee_name}] if check_in.attendee_email else [],
                    "location": check_in.location or check_in.meeting_url,
                    "start_datetime": start_time_aware,
                    "provider": "manual",
                    "client_name": client_name
                })
        
        print(f"[CALENDAR SUMMARY] Total bookings (including manual): {len(all_bookings)}")
        
        # Deduplicate bookings by ID (in case we fetched duplicates)
        seen_ids = set()
        unique_bookings = []
        for booking in all_bookings:
            booking_id = booking.get("id")
            if booking_id and booking_id not in seen_ids:
                seen_ids.add(booking_id)
                unique_bookings.append(booking)
            elif not booking_id:
                # If no ID, use start_time as fallback
                unique_bookings.append(booking)
        
        print(f"[CALENDAR SUMMARY] Unique bookings after deduplication: {len(unique_bookings)}")
        
        # Filter bookings by date ranges using the stored datetime
        upcoming_bookings = []
        last_week_bookings = []
        last_month_bookings = []
        last_week_previous_period = []
        last_month_previous_period = []
        
        for booking in unique_bookings:
            try:
                # Use the stored datetime if available, otherwise parse
                start_time = booking.get("start_datetime")
                if not start_time:
                    start_time = datetime.fromisoformat(booking["start_time"].replace('Z', '+00:00'))
                
                # Make timezone-aware if needed
                if start_time.tzinfo is None:
                    from datetime import timezone
                    start_time = start_time.replace(tzinfo=timezone.utc)
                
                # Upcoming (from now onwards)
                if start_time >= now:
                    upcoming_bookings.append(booking)
                
                # Last week (7 days ago to now)
                if one_week_ago <= start_time < now:
                    last_week_bookings.append(booking)
                
                # Last month (30 days ago to now)
                if one_month_ago <= start_time < now:
                    last_month_bookings.append(booking)
                
                # Previous week (for comparison)
                if one_week_before_that <= start_time < one_week_ago:
                    last_week_previous_period.append(booking)
                
                # Previous month (for comparison)
                if one_month_before_that <= start_time < one_month_ago:
                    last_month_previous_period.append(booking)
            except Exception as e:
                print(f"[CALENDAR SUMMARY] Error filtering booking: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"[CALENDAR SUMMARY] Filtered counts:")
        print(f"  - Upcoming: {len(upcoming_bookings)}")
        print(f"  - Last week: {len(last_week_bookings)}")
        print(f"  - Last month: {len(last_month_bookings)}")
        print(f"  - Previous week: {len(last_week_previous_period)}")
        print(f"  - Previous month: {len(last_month_previous_period)}")
        
        # Calculate percentage changes
        last_week_change = None
        if len(last_week_previous_period) > 0:
            change = ((len(last_week_bookings) - len(last_week_previous_period)) / len(last_week_previous_period)) * 100
            last_week_change = round(change, 1)
        elif len(last_week_bookings) > 0:
            last_week_change = 100.0  # 100% increase (from 0)
        
        last_month_change = None
        if len(last_month_previous_period) > 0:
            change = ((len(last_month_bookings) - len(last_month_previous_period)) / len(last_month_previous_period)) * 100
            last_month_change = round(change, 1)
        elif len(last_month_bookings) > 0:
            last_month_change = 100.0  # 100% increase (from 0)
        
        # Sort all upcoming bookings by start_time
        upcoming_bookings.sort(key=lambda b: b.get("start_datetime") or datetime.fromisoformat(b["start_time"].replace('Z', '+00:00')))
        
        # Find most upcoming appointment and get up to 3 upcoming appointments
        most_upcoming = None
        upcoming_appointments_list = []
        
        if upcoming_bookings:
            # Get up to 3 upcoming appointments
            top_3_upcoming = upcoming_bookings[:3]
            
            for booking in top_3_upcoming:
                appointment = CalendarUpcomingAppointment(
                    id=booking.get("id"),
                    title=booking.get("title", "Untitled Event"),
                    start_time=booking.get("start_time"),
                    end_time=booking.get("end_time"),
                    link=booking.get("link"),
                    provider=booking.get("provider", provider),  # Use booking's provider (could be "manual")
                    attendees=booking.get("attendees"),
                    location=booking.get("location"),
                    client_name=booking.get("client_name")  # Include client name for manual check-ins
                )
                upcoming_appointments_list.append(appointment)
            
            # Most upcoming is the first one
            most_upcoming = upcoming_appointments_list[0] if upcoming_appointments_list else None
        
        result = CalendarNotificationsSummary(
            upcoming_count=len(upcoming_bookings),
            last_week_count=len(last_week_bookings),
            last_month_count=len(last_month_bookings),
            last_week_percentage_change=last_week_change,
            last_month_percentage_change=last_month_change,
            most_upcoming=most_upcoming,
            upcoming_appointments=upcoming_appointments_list if upcoming_appointments_list else None,
            provider=provider,
            connected=True
        )
        
        print(f"[CALENDAR SUMMARY] Returning summary:")
        print(f"  - Upcoming: {result.upcoming_count}")
        print(f"  - Last week: {result.last_week_count} ({result.last_week_percentage_change}% change)")
        print(f"  - Last month: {result.last_month_count} ({result.last_month_percentage_change}% change)")
        print(f"  - Most upcoming: {result.most_upcoming.title if result.most_upcoming else 'None'}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[CALENDAR SUMMARY] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching calendar summary: {str(e)}"
        )


@router.get("/calendly/event/{event_uri:path}")
def get_calendly_event_details(
    event_uri: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    """
    Get detailed information for a specific Calendly scheduled event, including invitee form responses.
    The event_uri can be:
    - Full URI: https://api.calendly.com/scheduled_events/ABC123
    - Just the UUID: ABC123
    - URL-encoded full URI
    """
    from urllib.parse import unquote
    # Get selected org_id from user object (set by get_current_user)
    org_id = getattr(current_user, 'selected_org_id', current_user.org_id)
    
    headers, stored_user_uri = get_calendly_auth_headers(db, org_id, current_user.id)
    
    try:
        # Decode URL encoding if present
        event_uri = unquote(event_uri)
        
        # Construct full URI if needed
        if not event_uri.startswith("http"):
            # If just the ID/UUID is provided, construct the full URI
            event_uri = f"https://api.calendly.com/scheduled_events/{event_uri}"
        
        # Fetch the event details
        response = httpx.get(
            event_uri,
            headers=headers,
            timeout=30.0
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail="Failed to fetch Calendly event details"
            )
        
        event_data = response.json()
        event_resource = event_data.get("resource", {})
        event_uri_path = event_resource.get("uri", event_uri)
        
        # Fetch invitees for this event (they contain form responses)
        # Calendly API: GET /event_invitees?event={event_uri}
        invitees = []
        invitee_emails = []
        try:
            # Extract event UUID from URI
            event_uuid = event_uri_path.split("/")[-1] if "/" in event_uri_path else event_uri_path
            
            # Fetch invitees using the event URI
            invitees_response = httpx.get(
                f"https://api.calendly.com/event_invitees",
                headers=headers,
                params={"event": event_uri_path},
                timeout=30.0
            )
            
            if invitees_response.status_code == 200:
                invitees_data = invitees_response.json()
                invitees = invitees_data.get("collection", [])
                # Extract emails for routing form matching
                invitee_emails = [inv.get("email") for inv in invitees if inv.get("email")]
                print(f"[CALENDLY EVENT DETAILS] Fetched {len(invitees)} invitees for event")
            else:
                print(f"[CALENDLY EVENT DETAILS] Failed to fetch invitees: {invitees_response.status_code}")
        except Exception as e:
            print(f"[CALENDLY EVENT DETAILS] Warning: Failed to fetch invitees: {e}")
            import traceback
            traceback.print_exc()
        
        # Fetch routing form submissions
        # Calendly API: GET /routing_form_submissions
        # Match by event URI or invitee email
        routing_form_submissions = []
        try:
            # Get organization URI from user info
            user_info_response = httpx.get(
                "https://api.calendly.com/users/me",
                headers=headers,
                timeout=10.0
            )
            
            if user_info_response.status_code == 200:
                user_info = user_info_response.json()
                user_resource = user_info.get("resource", {})
                current_org_uri = user_resource.get("current_organization") or user_resource.get("organization")
                
                if current_org_uri:
                    print(f"[CALENDLY EVENT DETAILS] Found org URI: {current_org_uri}, fetching routing form submissions...")
                    
                    # Fetch routing form submissions
                    # Filter by event or email
                    submissions_params = {}
                    if event_uri_path:
                        submissions_params["event"] = event_uri_path
                    
                    submissions_response = httpx.get(
                        "https://api.calendly.com/routing_form_submissions",
                        headers=headers,
                        params=submissions_params,
                        timeout=30.0
                    )
                    
                    if submissions_response.status_code == 200:
                        submissions_data = submissions_response.json()
                        submissions_collection = submissions_data.get("collection", [])
                        
                        # Filter submissions by event URI or invitee email
                        for submission in submissions_collection:
                            submission_event_uri = submission.get("event")
                            submission_email = submission.get("submitter_email") or submission.get("email")
                            
                            # Match by event URI or email
                            if (submission_event_uri and submission_event_uri == event_uri_path) or \
                               (submission_email and submission_email in invitee_emails):
                                routing_form_submissions.append(submission)
                                print(f"[CALENDLY EVENT DETAILS] Matched routing form submission for {submission_email or 'event'}")
                    else:
                        print(f"[CALENDLY EVENT DETAILS] Failed to fetch routing form submissions: {submissions_response.status_code}")
                else:
                    print(f"[CALENDLY EVENT DETAILS] Could not find organization URI")
        except Exception as e:
            print(f"[CALENDLY EVENT DETAILS] Warning: Failed to fetch routing form submissions: {e}")
            import traceback
            traceback.print_exc()
        
        # Add invitees and routing form submissions to event data
        event_resource["invitees"] = invitees
        event_resource["routingFormSubmissions"] = routing_form_submissions
        
        # Transform to our schema format
        transformed_event = {
            **event_resource,
            "start_time": event_resource.get("start_time"),
            "end_time": event_resource.get("end_time"),
        }
        
        return CalendlyScheduledEvent(**transformed_event)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[CALENDLY EVENT DETAILS] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Calendly event details: {str(e)}"
        )

