"""
Brevo webhook handler for contact creation events.
When new contacts are added to Brevo, this processes them and creates clients
in the database if they pass spam detection.
"""
from fastapi import APIRouter, Depends, HTTPException, Request, status, Header
from fastapi.responses import Response
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.models.client import Client, LifecycleState
from app.services.email_spam_detector import detect_spam_email, SpamDetectionResult
from app.models.oauth_token import OAuthToken, OAuthProvider
from typing import Optional, Dict, Any
import uuid
import json
import hmac
import hashlib
from datetime import datetime

router = APIRouter()


def get_brevo_auth_headers(db: Session, org_id: uuid.UUID, user_id: uuid.UUID):
    """Get Brevo authentication headers for API calls"""
    # Import here to avoid circular dependency
    from app.api.integrations import get_brevo_auth_headers as _get_brevo_auth_headers
    return _get_brevo_auth_headers(db, org_id, user_id)


@router.post("/brevo/webhook")
async def brevo_webhook(
    request: Request,
    db: Session = Depends(get_db),
    x_brevo_signature: Optional[str] = Header(None, alias="X-Brevo-Signature")
):
    """
    Handle Brevo webhook events for contact creation.
    
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
    
    Note: In production, verify webhook signature using X-Brevo-Signature header.
    """
    try:
        # Get raw body for signature verification (if needed)
        body = await request.body()
        payload = json.loads(body.decode('utf-8'))
        
        print(f"[BREVO_WEBHOOK] Received webhook: {payload.get('event', 'unknown')}")
        
        # Verify webhook signature (optional but recommended)
        # TODO: Implement signature verification if Brevo provides a secret
        # if x_brevo_signature:
        #     expected_signature = hmac.new(
        #         settings.BREVO_WEBHOOK_SECRET.encode(),
        #         body,
        #         hashlib.sha256
        #     ).hexdigest()
        #     if not hmac.compare_digest(x_brevo_signature, expected_signature):
        #         raise HTTPException(status_code=401, detail="Invalid webhook signature")
        
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
        
        # Find which org this contact belongs to
        # We need to determine org_id from the contact's list or other metadata
        # For now, we'll try to find the org by checking all Brevo connections
        brevo_tokens = db.query(OAuthToken).filter(
            OAuthToken.provider == OAuthProvider.BREVO
        ).all()
        
        if not brevo_tokens:
            print(f"[BREVO_WEBHOOK] No Brevo connections found")
            return Response(status_code=200, content="No Brevo connections")
        
        # Get the list ID from the webhook (if available)
        list_id = payload.get("listid")
        
        # Try to match org by list ID or use first available org
        # In a multi-tenant setup, you might want to include org_id in webhook metadata
        org_id = None
        for token in brevo_tokens:
            # If list_id is provided, we could check which org owns that list
            # For now, we'll use the first org (you may want to improve this logic)
            if not org_id:
                org_id = token.org_id
                break
        
        if not org_id:
            print(f"[BREVO_WEBHOOK] Could not determine org_id")
            return Response(status_code=200, content="Could not determine org")
        
        # Check if client already exists
        existing_client = db.query(Client).filter(
            Client.email == email_address,
            Client.org_id == org_id
        ).first()
        
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
            
            # Update source if not already set
            if not existing_client.source:
                existing_client.source = "brevo_webhook"
            
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
            org_id=org_id,
            email=email_address,
            first_name=first_name,
            last_name=last_name,
            lifecycle_state=LifecycleState.COLD_LEAD,
            source="brevo_webhook",
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

