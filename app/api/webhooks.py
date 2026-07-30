"""
Stripe webhook handler.
Verifies webhook signatures and processes events immediately in-request.
Non-2xx responses are intentional so Stripe retries delivery when we cannot process.
"""
from fastapi import APIRouter, Request, Header, Depends
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


def _json_response(status_code: int, content: str) -> Response:
    return Response(status_code=status_code, content=content)


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
        print(f"[WEBHOOK] Stripe library not available for org {org_id}")
        return _json_response(503, "Stripe library not available")

    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        print(f"[WEBHOOK] Invalid org_id in path: {org_id}")
        # Permanent bad URL — ack so Stripe stops retrying this endpoint.
        return _json_response(400, "Invalid org")

    oauth_token = db.query(OAuthToken).filter(
        OAuthToken.provider == OAuthProvider.STRIPE,
        OAuthToken.org_id == org_uuid,
        OAuthToken.webhook_secret.isnot(None),
    ).first()

    if not oauth_token or not oauth_token.webhook_secret:
        print(f"[WEBHOOK] No webhook secret for org {org_id}")
        # Retryable: repair endpoint / reconnect may restore the secret.
        return _json_response(500, "Webhook not configured for org")

    from app.core.encryption import decrypt_token
    webhook_secret = decrypt_token(oauth_token.webhook_secret)

    body = await request.body()
    if not stripe_signature:
        print(f"[WEBHOOK] Missing Stripe-Signature header for org {org_id}")
        return _json_response(400, "Missing signature")

    try:
        event = stripe.Webhook.construct_event(body, stripe_signature, webhook_secret)
    except ValueError as e:
        print(f"[WEBHOOK] Invalid payload for org {org_id}: {e}")
        return _json_response(400, "Invalid payload")
    except stripe.error.SignatureVerificationError as e:
        print(f"[WEBHOOK] Signature verification failed for org {org_id}: {e}")
        # Wrong/rotated secret — retry so a repaired endpoint can succeed.
        return _json_response(400, "Invalid signature")

    return _process_stripe_event_internal(db, event, org_uuid)


def _process_stripe_event_internal(db: Session, event: dict, org_id: uuid.UUID):
    """Shared logic for processing Stripe webhook events.

    Idempotency: skip only when the event was already processed successfully.
    Failed prior attempts are retried. Processing failures return 500 so Stripe retries.
    """
    stripe_event = None
    try:
        existing_event = db.query(StripeEvent).filter(
            StripeEvent.stripe_event_id == event["id"],
            StripeEvent.org_id == org_id
        ).first()

        if existing_event and existing_event.processed:
            return _json_response(200, "Event already processed")

        if existing_event:
            stripe_event = existing_event
            stripe_event.payload = event
            stripe_event.type = event["type"]
            print(f"[WEBHOOK] Retrying previously failed event {event.get('id')}")
        else:
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
        return _json_response(200, "Webhook received")
    except Exception as e:
        import traceback
        print(f"[WEBHOOK] ❌ ERROR processing event: {e}")
        print(traceback.format_exc())
        try:
            if stripe_event is not None:
                stripe_event.processed = False
                db.commit()
        except Exception:
            db.rollback()
        # Non-2xx so Stripe retries delivery instead of permanently dropping the event.
        return _json_response(500, "Webhook processing failed")


@router.post("/stripe")
async def stripe_webhook(
    request: Request,
    db: Session = Depends(get_db),
    stripe_signature: Optional[str] = Header(None, alias="stripe-signature")
):
    """
    Platform Stripe webhook (Connect / single-account).
    Prefer per-org endpoints at /webhooks/stripe/org/{org_id} for API-key multi-tenant.
    """
    print(f"[WEBHOOK] Received platform webhook request")
    print(f"[WEBHOOK] Has signature header: {stripe_signature is not None}")

    if not STRIPE_AVAILABLE or stripe is None:
        print(f"[WEBHOOK] ❌ ERROR: Stripe library not available")
        return _json_response(503, "Stripe library not available")

    if not settings.STRIPE_WEBHOOK_SECRET:
        print(f"[WEBHOOK] ❌ ERROR: STRIPE_WEBHOOK_SECRET not configured in .env")
        print(f"[WEBHOOK] For local development: Run 'stripe listen --forward-to localhost:8000/webhooks/stripe'")
        print(f"[WEBHOOK] Then copy the webhook signing secret (whsec_...) from the output and add it to .env")
        return _json_response(500, "Webhook secret not configured")

    body = await request.body()
    if not stripe_signature:
        print(f"[WEBHOOK] ❌ Missing Stripe-Signature header")
        return _json_response(400, "Missing signature")

    try:
        event = stripe.Webhook.construct_event(
            body,
            stripe_signature,
            settings.STRIPE_WEBHOOK_SECRET
        )
    except ValueError as e:
        print(f"[WEBHOOK] ❌ Invalid payload: {str(e)}")
        return _json_response(400, "Invalid payload")
    except stripe.error.SignatureVerificationError as e:
        print(f"[WEBHOOK] ❌ Signature verification failed: {str(e)}")
        print(f"[WEBHOOK] Make sure STRIPE_WEBHOOK_SECRET is set correctly in .env")
        return _json_response(400, "Invalid signature")

    try:
        event_account_id = event.get("account")

        if event_account_id:
            stripe_oauth = db.query(OAuthToken).filter(
                OAuthToken.provider == OAuthProvider.STRIPE,
                OAuthToken.account_id == event_account_id
            ).first()

            if stripe_oauth:
                org_id = stripe_oauth.org_id
                print(f"[WEBHOOK] Matched event account {event_account_id} to org {org_id}")
            else:
                # Unknown Connect account — do not ack as success or we lose the event forever
                # if the org connects later in the retry window.
                print(f"[WEBHOOK] No org matched for Stripe account {event_account_id}")
                return _json_response(404, "No matching Stripe account")
        else:
            # Direct (non-Connect) platform webhook: only safe when exactly one Stripe token exists.
            stripe_tokens = db.query(OAuthToken).filter(
                OAuthToken.provider == OAuthProvider.STRIPE
            ).all()
            if len(stripe_tokens) == 1:
                org_id = stripe_tokens[0].org_id
                print(f"[WEBHOOK] Event missing account field; using sole Stripe connection (org {org_id})")
            else:
                print(
                    f"[WEBHOOK] Event missing account field and {len(stripe_tokens)} Stripe connections; "
                    "use per-org webhook /webhooks/stripe/org/{org_id}"
                )
                return _json_response(409, "Ambiguous org for event")

        return _process_stripe_event_internal(db, event, org_id)
    except Exception as e:
        import traceback
        print(f"[WEBHOOK] ❌ UNEXPECTED ERROR in webhook handler: {str(e)}")
        print(f"[WEBHOOK] Full traceback:")
        print(traceback.format_exc())
        return _json_response(500, "Webhook processing failed")
