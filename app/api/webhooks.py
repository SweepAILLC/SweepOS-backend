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


# Per-org webhook route (used when connecting via API key - each org has its own webhook endpoint)
@router.post("/stripe/org/{org_id}")
async def stripe_webhook_per_org(
    org_id: str,
    request: Request,
    db: Session = Depends(get_db),
    stripe_signature: Optional[str] = Header(None, alias="stripe-signature"),
):
    """
    Handle Stripe webhook events for a specific org (per-org webhook created on API key connect).
    Verifies signature using org-specific webhook secret.
    """
    if not STRIPE_AVAILABLE or stripe is None:
        return Response(status_code=200, content="Stripe library not available")

    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        print(f"[WEBHOOK] Invalid org_id in path: {org_id}")
        return Response(status_code=200, content="Invalid org")

    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == org_uuid,
        OAuthToken.webhook_secret.isnot(None),
    ).first()

    if not oauth_token or not oauth_token.webhook_secret:
        print(f"[WEBHOOK] No webhook secret for org {org_id}")
        return Response(status_code=200, content="Webhook not configured for org")

    from app.core.encryption import decrypt_token
    webhook_secret = decrypt_token(oauth_token.webhook_secret)

    body = await request.body()
    if not stripe_signature:
        print(f"[WEBHOOK] Missing Stripe-Signature header for org {org_id}")
        return Response(status_code=200, content="Missing signature")

    try:
        event = stripe.Webhook.construct_event(body, stripe_signature, webhook_secret)
    except ValueError as e:
        print(f"[WEBHOOK] Invalid payload for org {org_id}: {e}")
        return Response(status_code=200, content="Invalid payload")
    except stripe.error.SignatureVerificationError as e:
        print(f"[WEBHOOK] Signature verification failed for org {org_id}: {e}")
        return Response(status_code=200, content="Invalid signature")

    return _process_stripe_event_internal(db, event, org_uuid)


def _process_stripe_event_internal(db: Session, event: dict, org_id: uuid.UUID):
    """Shared logic for processing Stripe webhook events (sync for now)."""
    import asyncio
    # Run sync DB code in executor to avoid blocking
    try:
        # Check idempotency
        existing_event = db.query(StripeEvent).filter(
            StripeEvent.stripe_event_id == event["id"],
            StripeEvent.org_id == org_id
        ).first()

        if existing_event:
            return Response(status_code=200, content="Event already processed")

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

        from app.services.stripe_processor import process_stripe_event
        print(f"[WEBHOOK] Processing Stripe event: {event.get('type')} (ID: {event.get('id')}) for org {org_id}")
        process_stripe_event(db, event, org_id)
        stripe_event.processed = True
        stripe_event.processed_at = datetime.utcnow()
        # Mark org's Stripe data as updated so terminal tab can refetch only when webhook fired
        token = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.STRIPE,
            OAuthToken.org_id == org_id,
        ).first()
        if token:
            token.last_webhook_processed_at = datetime.utcnow()
        db.commit()
        print(f"[WEBHOOK] ✅ Processed event {event.get('id')} ({event.get('type')})")
    except Exception as e:
        import traceback
        print(f"[WEBHOOK] ❌ ERROR processing event: {e}")
        print(traceback.format_exc())
        try:
            stripe_event.processed = False
            db.commit()
        except Exception:
            db.rollback()

    return Response(status_code=200, content="Webhook received")


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
        
        return _process_stripe_event_internal(db, event, org_id)
    except Exception as e:
        # Catch any unexpected errors and still return 200 to prevent websocket closure
        import traceback
        print(f"[WEBHOOK] ❌ UNEXPECTED ERROR in webhook handler: {str(e)}")
        print(f"[WEBHOOK] Full traceback:")
        print(traceback.format_exc())
        return Response(status_code=200, content="Webhook received (error logged)")

