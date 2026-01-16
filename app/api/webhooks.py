"""
Stripe webhook handler.
Verifies webhook signatures and enqueues events for background processing.
"""
from fastapi import APIRouter, Request, HTTPException, status, Header, Depends
from fastapi.responses import Response
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.core.config import settings
from app.models.stripe_event import StripeEvent
from app.models.oauth_token import OAuthToken, OAuthProvider
from datetime import datetime
from typing import Optional
import uuid

router = APIRouter()

# Import stripe only when needed (don't fail on import if stripe package has issues)
try:
    import stripe
    STRIPE_AVAILABLE = True
except ImportError:
    STRIPE_AVAILABLE = False
    stripe = None

# Initialize Stripe with secret key for webhook verification
# Only set if secret key is available (don't fail on import if not set)
if STRIPE_AVAILABLE:
    try:
        if settings.STRIPE_SECRET_KEY:
            stripe.api_key = settings.STRIPE_SECRET_KEY
    except Exception:
        # Settings might not be loaded yet, that's okay
        pass


@router.post("/stripe")
async def stripe_webhook(
    request: Request,
    db: Session = Depends(get_db),
    stripe_signature: Optional[str] = Header(None, alias="stripe-signature")
):
    """
    Handle Stripe webhook events.
    
    - Verifies webhook signature using Stripe signing secret
    - Stores raw event in database immediately
    - Returns 200 quickly to Stripe
    - Background job processes the event asynchronously
    """
    
    print(f"[WEBHOOK] Received webhook request")
    print(f"[WEBHOOK] Has signature header: {stripe_signature is not None}")
    
    # Always return 200 to Stripe CLI to prevent websocket connection from closing
    # Log errors instead of raising exceptions
    
    if not STRIPE_AVAILABLE or stripe is None:
        print(f"[WEBHOOK] ❌ ERROR: Stripe library not available")
        return Response(status_code=200, content="Stripe library not available")
    
    if not settings.STRIPE_WEBHOOK_SECRET:
        print(f"[WEBHOOK] ❌ ERROR: STRIPE_WEBHOOK_SECRET not configured in .env")
        print(f"[WEBHOOK] For local development: Run 'stripe listen --forward-to localhost:8000/webhooks/stripe'")
        print(f"[WEBHOOK] Then copy the webhook signing secret (whsec_...) from the output and add it to .env")
        return Response(status_code=200, content="Webhook secret not configured")
    
    # Get raw body for signature verification
    body = await request.body()
    
    try:
        # Verify webhook signature
        event = stripe.Webhook.construct_event(
            body,
            stripe_signature,
            settings.STRIPE_WEBHOOK_SECRET
        )
    except ValueError as e:
        # Invalid payload
        print(f"[WEBHOOK] ❌ Invalid payload: {str(e)}")
        return Response(status_code=200, content="Invalid payload")
    except stripe.error.SignatureVerificationError as e:
        # Invalid signature
        print(f"[WEBHOOK] ❌ Signature verification failed: {str(e)}")
        print(f"[WEBHOOK] Make sure STRIPE_WEBHOOK_SECRET is set correctly in .env")
        print(f"[WEBHOOK] For local development with 'stripe listen', copy the webhook secret from the CLI output")
        return Response(status_code=200, content="Invalid signature")
    
    # Wrap the rest in try-except to ensure we always return 200
    try:
        # Determine org_id from Stripe event account_id
        # Stripe webhook events include an "account" field that identifies which Stripe account sent the event
        # Match this to the OAuth token's account_id to find the correct org
        event_account_id = event.get("account")
        
        if event_account_id:
            # Find the org that has this Stripe account connected
            stripe_oauth = db.query(OAuthToken).filter(
                OAuthToken.provider == OAuthProvider.STRIPE,
                OAuthToken.account_id == event_account_id
            ).first()
            
            if stripe_oauth:
                org_id = stripe_oauth.org_id
                print(f"[WEBHOOK] Matched event account {event_account_id} to org {org_id}")
            else:
                # Fallback: use first Stripe connection (for backward compatibility)
                stripe_oauth = db.query(OAuthToken).filter(
                    OAuthToken.provider == OAuthProvider.STRIPE
                ).first()
                if stripe_oauth:
                    org_id = stripe_oauth.org_id
                    print(f"[WEBHOOK] WARNING: Event account {event_account_id} not found, using first Stripe connection (org {org_id})")
                else:
                    # No Stripe connection - use default org for v1
                    org_id = uuid.UUID("00000000-0000-0000-0000-000000000001")
                    print(f"[WEBHOOK] WARNING: No Stripe connection found, using default org {org_id}")
        else:
            # Event doesn't have account field (shouldn't happen for Connect events, but handle gracefully)
            stripe_oauth = db.query(OAuthToken).filter(
                OAuthToken.provider == OAuthProvider.STRIPE
            ).first()
            if stripe_oauth:
                org_id = stripe_oauth.org_id
                print(f"[WEBHOOK] WARNING: Event missing account field, using first Stripe connection (org {org_id})")
            else:
                # No Stripe connection - use default org for v1
                org_id = uuid.UUID("00000000-0000-0000-0000-000000000001")
                print(f"[WEBHOOK] WARNING: No Stripe connection found, using default org {org_id}")
        
        # Check if event already processed (idempotency) - check by org_id too
        existing_event = db.query(StripeEvent).filter(
            StripeEvent.stripe_event_id == event["id"],
            StripeEvent.org_id == org_id
        ).first()
        
        if existing_event:
            # Event already processed, return 200
            return Response(status_code=200, content="Event already processed")
        
        # Store raw event in database with org_id
        stripe_event = StripeEvent(
            org_id=org_id,
            stripe_event_id=event["id"],
            type=event["type"],
            payload=event,
            processed=False,
            received_at=datetime.utcnow()
        )
        db.add(stripe_event)
        db.commit()
        
        # Trigger background processing (for now, we'll process synchronously in a simple way)
        # In production, use a proper job queue (Celery, RQ, etc.)
        try:
            from app.services.stripe_processor import process_stripe_event
            print(f"[WEBHOOK] Processing Stripe event: {event.get('type')} (ID: {event.get('id')}) for org {org_id}")
            print(f"[WEBHOOK] Event structure: type={event.get('type')}, has_data={bool(event.get('data'))}")
            
            # Process the event (this may modify the database) - pass org_id
            process_stripe_event(db, event, org_id)
            
            # Mark as processed
            stripe_event.processed = True
            stripe_event.processed_at = datetime.utcnow()
            db.commit()
            print(f"[WEBHOOK] ✅ Successfully processed and committed event {event.get('id')} ({event.get('type')})")
        except ImportError as e:
            # Service not available, log and continue
            print(f"[WEBHOOK] ❌ ERROR: Stripe processor not available: {str(e)}")
            import traceback
            print(traceback.format_exc())
            db.rollback()
        except Exception as e:
            # Log error but don't fail webhook - we'll retry later
            # In production, use proper error handling and retry logic
            import traceback
            print(f"[WEBHOOK] ❌ ERROR processing Stripe event {event.get('id')} ({event.get('type')}): {str(e)}")
            print(f"[WEBHOOK] Full traceback:")
            print(traceback.format_exc())
            # Don't rollback the event storage, just mark it as unprocessed
            stripe_event.processed = False
            db.commit()  # Commit the event record even if processing failed
        
        # Always return 200 to Stripe (even if processing failed, we'll retry)
        return Response(status_code=200, content="Webhook received")
    except Exception as e:
        # Catch any unexpected errors and still return 200 to prevent websocket closure
        import traceback
        print(f"[WEBHOOK] ❌ UNEXPECTED ERROR in webhook handler: {str(e)}")
        print(f"[WEBHOOK] Full traceback:")
        print(traceback.format_exc())
        return Response(status_code=200, content="Webhook received (error logged)")

