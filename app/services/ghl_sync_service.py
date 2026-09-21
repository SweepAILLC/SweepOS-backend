"""GHL contact sync: match/create Sweep clients from GHL contact payloads.

Mirrors the identity-matching shape in app.services.whop_sync (email match, then
phone match, then create) rather than inventing a new merge strategy. Commits per
contact so a single bad record can't fail the whole sync and so the transaction
never holds row locks on `clients` across the full paginated pull (a location can
have thousands of contacts).
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.orm import Session

from app.models.client import Client, LifecycleState, find_client_by_email, find_client_by_phone

LOG = logging.getLogger(__name__)


def apply_ghl_identity_to_client(
    client: Client,
    email: Optional[str],
    phone: Optional[str],
    first_name: Optional[str],
    last_name: Optional[str],
) -> None:
    """Fill only empty fields on an existing client; never overwrite a coach-edited value."""
    if email:
        email = email.strip()
        current = (client.email or "").strip()
        if not current:
            client.email = email
        elif email.lower() != current.lower():
            extras = list(client.emails) if isinstance(client.emails, list) else []
            seen = {e.lower() for e in extras if isinstance(e, str)}
            if email.lower() not in seen:
                extras.append(email)
                client.emails = extras
    if phone and not (client.phone or "").strip():
        client.phone = phone
    if first_name and not (client.first_name or "").strip():
        client.first_name = first_name
    if last_name and not (client.last_name or "").strip():
        client.last_name = last_name
    client.updated_at = datetime.utcnow()


def _stamp_ghl_contact_id(client: Client, ghl_contact_id: Optional[str]) -> None:
    if not ghl_contact_id:
        return
    meta = dict(client.meta) if isinstance(client.meta, dict) else {}
    if meta.get("ghl_contact_id") != ghl_contact_id:
        meta["ghl_contact_id"] = ghl_contact_id
        client.meta = meta


def ensure_client_for_ghl_contact(
    db: Session, org_id: uuid.UUID, contact: Dict[str, Optional[str]]
) -> Tuple[Client, bool]:
    """
    Find or create a Sweep client for a normalized GHL contact dict (see
    app.services.ghl_client.normalize_ghl_contact). Returns (client, created).

    Prefers an email match, then a phone match. A contact with neither is still
    created (never dropped) but flagged meta.ghl_unmatched=True so a coach can
    merge it manually later instead of it silently vanishing.
    """
    email = contact.get("email")
    phone = contact.get("phone")
    first_name = contact.get("first_name")
    last_name = contact.get("last_name")
    ghl_contact_id = contact.get("ghl_contact_id")

    existing = find_client_by_email(db, org_id, email) if email else None
    if not existing and phone:
        existing = find_client_by_phone(db, org_id, phone)

    if existing:
        apply_ghl_identity_to_client(existing, email, phone, first_name, last_name)
        _stamp_ghl_contact_id(existing, ghl_contact_id)
        return existing, False

    meta: Dict[str, Any] = {}
    if ghl_contact_id:
        meta["ghl_contact_id"] = ghl_contact_id
    if not email and not phone:
        meta["ghl_unmatched"] = True

    now = datetime.utcnow()
    client = Client(
        org_id=org_id,
        email=email,
        phone=phone,
        first_name=first_name,
        last_name=last_name,
        lifecycle_state=LifecycleState.COLD_LEAD,
        meta=meta or None,
        created_at=now,
        updated_at=now,
    )
    db.add(client)
    db.flush()
    return client, True


def sync_ghl_contacts(
    db: Session,
    org_id: uuid.UUID,
    headers: Dict[str, str],
    location_id: str,
) -> Dict[str, int]:
    """Pull every GHL contact for the location and upsert into `clients`."""
    from app.services.ghl_client import iter_ghl_contacts, normalize_ghl_contact

    counts = {"created": 0, "matched": 0, "unmatched": 0, "errors": 0}
    for raw in iter_ghl_contacts(headers, location_id):
        try:
            contact = normalize_ghl_contact(raw)
            _client, created = ensure_client_for_ghl_contact(db, org_id, contact)
            counts["created" if created else "matched"] += 1
            if not contact.get("email") and not contact.get("phone"):
                counts["unmatched"] += 1
            db.commit()
        except Exception:
            db.rollback()
            counts["errors"] += 1
            LOG.exception("ghl sync: failed to upsert contact %s", raw.get("id"))
    return counts


def run_ghl_contact_sync_background(org_id_str: str) -> None:
    """Entry point for schedule_background_work: own DB session + own GHL connection,
    since a background job runs off the request thread."""
    from app.db.session import SessionLocal
    from app.models.oauth_token import OAuthProvider, OAuthToken
    from app.services.ghl_client import GhlApiError, GhlNotConnectedError, get_ghl_connection

    db = SessionLocal()
    try:
        org_id = uuid.UUID(org_id_str)
        headers, location_id = get_ghl_connection(db, org_id)
        counts = sync_ghl_contacts(db, org_id, headers, location_id)

        token_row = (
            db.query(OAuthToken)
            .filter(OAuthToken.provider == OAuthProvider.GHL, OAuthToken.org_id == org_id)
            .first()
        )
        if token_row is not None:
            token_row.last_sync_at = datetime.utcnow()
            db.commit()

        LOG.info(
            "ghl contact sync done org=%s created=%s matched=%s unmatched=%s errors=%s",
            org_id, counts["created"], counts["matched"], counts["unmatched"], counts["errors"],
        )
    except GhlNotConnectedError:
        LOG.info("ghl contact sync skipped org=%s: not connected", org_id_str)
    except GhlApiError:
        LOG.exception("ghl contact sync failed org=%s: upstream API error", org_id_str)
    except Exception:
        LOG.exception("ghl contact sync failed org=%s", org_id_str)
    finally:
        db.close()
