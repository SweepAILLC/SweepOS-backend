"""
Brevo webhook handler for contact creation events.
When new contacts are added to Brevo, this processes them and creates clients
in the database if they pass spam detection.

Org-scoped like every other provider's webhook: configure this URL in Brevo's
webhook settings (per org, since Brevo has no concept of our multi-tenancy) as
{BACKEND_PUBLIC_URL}/webhooks/brevo/{org_id}, with a custom "X-Brevo-Signature"
header set to a shared secret (Brevo can't HMAC-sign the body, only send a
static custom header — same trust model as the GHL webhook). Set the secret
via BREVO_WEBHOOK_SECRET (env, org-wide) or per-org on oauth_tokens.webhook_secret.
An org with a webhook already configured against the old un-scoped
/brevo/webhook URL must update it to include their org_id.
"""
from fastapi import APIRouter, Depends, HTTPException, Request, status, Header
from fastapi.responses import Response
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.encryption import decrypt_token
from app.db.session import get_db
from app.models.client import Client, LifecycleState, find_client_by_email
from app.services.email_spam_detector import detect_spam_email, SpamDetectionResult
from app.models.oauth_token import OAuthToken, OAuthProvider
from app.api.calendar_webhooks import _parse_org, _read_body_async
from typing import Optional, Dict, Any
import uuid
import json
import hmac
import logging
from datetime import datetime

LOG = logging.getLogger(__name__)
router = APIRouter()


def get_brevo_auth_headers(db: Session, org_id: uuid.UUID, user_id: uuid.UUID):
    """Get Brevo authentication headers for API calls"""
    # Import here to avoid circular dependency
    from app.api.integrations import get_brevo_auth_headers as _get_brevo_auth_headers
    return _get_brevo_auth_headers(db, org_id, user_id)


def _resolve_brevo_webhook_secret(db: Session, org_id: uuid.UUID, token: Optional[OAuthToken]) -> Optional[str]:
    """Per-org shared secret on the Brevo oauth_tokens row, else the env-wide fallback."""
    if token and token.webhook_secret:
        try:
            return decrypt_token(token.webhook_secret)
        except Exception:
            LOG.warning("brevo webhook: secret decrypt failed org=%s", org_id)
    return getattr(settings, "BREVO_WEBHOOK_SECRET", None) or None


def _verify_brevo_shared_secret(secret: Optional[str], header_value: str) -> None:
    """Static equality check (constant-time) — Brevo custom webhook headers are a fixed
    value set in their dashboard, not a per-request HMAC of the body."""
    if not secret:
        # Matches the Fathom/Calendly/Cal.com/GHL posture: warn-and-accept when unset,
        # so local dev / first-time connection still works; set the env var in prod.
        LOG.warning("brevo webhook: no shared secret configured; accepting")
        return
    if not header_value or not hmac.compare_digest(header_value.strip(), secret.strip()):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook signature")


@router.post("/brevo/{org_id}")
async def brevo_webhook(
    org_id: str,
    request: Request,
    db: Session = Depends(get_db),
    x_brevo_signature: Optional[str] = Header(None, alias="X-Brevo-Signature")
):
    """
    Handle Brevo webhook events for contact creation, scoped to one org.

    When a new contact is created in Brevo, this webhook is triggered.
    The system will:
    1. Run spam detection on the contact
    2. If it's a real person: Create a client in the database with lifecycle_state = "cold_lead"
    3. If it's spam: Log it but don't create a client

    Brevo webhook payload format:
    {
        "event": "contact_added",
        "email": "contact@example.com",
        "attributes": {
            "FIRSTNAME": "John",
            "LASTNAME": "Doe",
            ...
        },
        "listid": 123,
        "blacklisted": false,
        ...
    }
    """
    org_uuid = _parse_org(org_id)
    token = (
        db.query(OAuthToken)
        .filter(OAuthToken.provider == OAuthProvider.BREVO, OAuthToken.org_id == org_uuid)
        .first()
    )
    if not token:
        # Don't process a contact for an org that never connected Brevo — this is exactly
        # the wrong-org data leak this endpoint used to have when it guessed instead.
        LOG.warning("brevo webhook: org=%s has no Brevo connection; ignoring", org_uuid)
        return Response(status_code=200, content="Org not connected")

    # Verify the shared secret before doing anything else, and outside the broad
    # try/except below — HTTPException is an Exception too, and that handler's job
    # is to turn processing errors into a 200-with-error-body for Brevo, not to
    # swallow an intentional 403 rejection into a false "success" response.
    body = await _read_body_async(request)
    secret = _resolve_brevo_webhook_secret(db, org_uuid, token)
    _verify_brevo_shared_secret(secret, x_brevo_signature or "")

    try:
        payload = json.loads(body.decode('utf-8'))

        print(f"[BREVO_WEBHOOK] org={org_uuid} received webhook: {payload.get('event', 'unknown')}")

        # Only process contact creation events
        event_type = payload.get("event", "")
        if event_type not in ["contact_added", "contact_created"]:
            print(f"[BREVO_WEBHOOK] Ignoring event type: {event_type}")
            return Response(status_code=200, content="Event ignored")
        
        # Extract contact information
        email_address = payload.get("email", "").strip()
        if not email_address:
            print(f"[BREVO_WEBHOOK] No email address in payload")
            return Response(status_code=200, content="No email address")
        
        # Check if contact is blacklisted
        if payload.get("blacklisted", False):
            print(f"[BREVO_WEBHOOK] Contact {email_address} is blacklisted, skipping")
            return Response(status_code=200, content="Contact blacklisted")
        
        # Extract attributes
        attributes = payload.get("attributes", {})
        first_name = attributes.get("FIRSTNAME") or attributes.get("FIRST_NAME") or attributes.get("firstName")
        last_name = attributes.get("LASTNAME") or attributes.get("LAST_NAME") or attributes.get("lastName")
        sender_name = f"{first_name} {last_name}".strip() if (first_name or last_name) else None
        
        # Detect spam
        spam_result: SpamDetectionResult = detect_spam_email(
            email_address=email_address,
            sender_name=sender_name,
            subject=None,  # Brevo webhook doesn't include email subject
            body=None,     # Brevo webhook doesn't include email body
            use_ai=False
        )
        
        # If spam, log but don't create client
        if spam_result.is_spam:
            print(f"[BREVO_WEBHOOK] Contact {email_address} detected as spam (score: {spam_result.score:.2f})")
            print(f"[BREVO_WEBHOOK] Reasons: {', '.join(spam_result.reasons)}")
            return Response(
                status_code=200,
                content=json.dumps({
                    "success": True,
                    "is_spam": True,
                    "message": "Contact filtered as spam"
                })
            )
        
        # org_uuid was already resolved from the URL path and verified connected, above —
        # no more scanning every Brevo connection in the database to guess.

        # Check if client already exists
        # Any email on the profile (primary or merged-in) so combined contacts stay combined.
        existing_client = find_client_by_email(db, org_uuid, email_address)
        
        if existing_client:
            # Merge: Update existing client with Brevo contact data
            print(f"[BREVO_WEBHOOK] Client already exists for {email_address}, merging data...")
            
            # Update fields if they're missing or empty in existing client
            updated_fields = []
            
            if first_name and (not existing_client.first_name or existing_client.first_name.strip() == ""):
                existing_client.first_name = first_name
                updated_fields.append("first_name")
            
            if last_name and (not existing_client.last_name or existing_client.last_name.strip() == ""):
                existing_client.last_name = last_name
                updated_fields.append("last_name")
            
            # Update lifecycle_state to cold_lead if it's currently null or if we want to reset it
            # Only update if it's not already in a more advanced state
            if existing_client.lifecycle_state is None or existing_client.lifecycle_state == LifecycleState.COLD_LEAD:
                existing_client.lifecycle_state = LifecycleState.COLD_LEAD
                if "lifecycle_state" not in updated_fields:
                    updated_fields.append("lifecycle_state")
            
            # Add note about merge
            merge_note = f"Merged with Brevo contact (ID: {payload.get('id', 'N/A')}) on {datetime.utcnow().isoformat()}"
            if updated_fields:
                merge_note += f". Updated fields: {', '.join(updated_fields)}"
            
            # Append to existing notes or create new
            if existing_client.notes:
                existing_client.notes = f"{existing_client.notes}\n{merge_note}"
            else:
                existing_client.notes = merge_note
            
            # Update source if not already set. Client has no `source` column —
            # this used to reference one that doesn't exist, which threw on every
            # merge and was silently swallowed by the blanket except below.
            meta = dict(existing_client.meta) if isinstance(existing_client.meta, dict) else {}
            if not meta.get("source"):
                meta["source"] = "brevo_webhook"
                existing_client.meta = meta

            db.commit()
            db.refresh(existing_client)
            
            print(f"[BREVO_WEBHOOK] ✅ Merged client {existing_client.id} for contact {email_address}")
            
            return Response(
                status_code=200,
                content=json.dumps({
                    "success": True,
                    "client_id": str(existing_client.id),
                    "is_spam": False,
                    "merged": True,
                    "updated_fields": updated_fields,
                    "message": "Client merged successfully"
                })
            )
        
        # Create new client in database
        client = Client(
            org_id=org_uuid,
            email=email_address,
            first_name=first_name,
            last_name=last_name,
            lifecycle_state=LifecycleState.COLD_LEAD,
            meta={"source": "brevo_webhook"},
            notes=f"Added via Brevo webhook. Contact ID: {payload.get('id', 'N/A')}"
        )
        
        db.add(client)
        db.commit()
        db.refresh(client)
        
        print(f"[BREVO_WEBHOOK] ✅ Created client {client.id} for contact {email_address}")
        
        return Response(
            status_code=200,
            content=json.dumps({
                "success": True,
                "client_id": str(client.id),
                "is_spam": False,
                "merged": False,
                "message": "Client created successfully"
            })
        )
        
    except json.JSONDecodeError as e:
        print(f"[BREVO_WEBHOOK] ❌ Invalid JSON: {str(e)}")
        return Response(status_code=400, content="Invalid JSON")
    except Exception as e:
        db.rollback()
        print(f"[BREVO_WEBHOOK] ❌ Error processing webhook: {str(e)}")
        import traceback
        print(traceback.format_exc())
        # Always return 200 to Brevo to prevent retries
        return Response(status_code=200, content=f"Error: {str(e)}")

