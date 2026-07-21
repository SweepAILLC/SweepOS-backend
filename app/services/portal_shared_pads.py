"""Shared-pad helpers for org portal + admin cross-org routes."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from app.models.portal_shared_pad import (
    DEFAULT_SHARED_PAD_CONTENT,
    DEFAULT_SHARED_PAD_TITLE,
    MAX_SHARED_PADS_PER_ORG,
    PortalSharedPad,
)
from app.models.user import User
from app.schemas.portal import PortalSharedPadResponse, PortalSharedPadSummary


def display_name(user: User, *, fallback: str = "Someone") -> str:
    email = (getattr(user, "email", None) or "").strip()
    if email and "@" in email:
        return email.split("@")[0]
    return email or fallback


def pad_response(pad: PortalSharedPad, *, unchanged: bool = False) -> PortalSharedPadResponse:
    return PortalSharedPadResponse(
        id=pad.id,
        org_id=pad.org_id,
        title=pad.title or DEFAULT_SHARED_PAD_TITLE,
        sort_order=int(pad.sort_order or 0),
        content="" if unchanged else (pad.content or ""),
        revision=int(pad.revision or 0),
        updated_by=pad.updated_by,
        updated_by_name=pad.updated_by_name,
        created_at=pad.created_at,
        updated_at=pad.updated_at,
        unchanged=unchanged,
    )


def pad_summary(pad: PortalSharedPad) -> PortalSharedPadSummary:
    return PortalSharedPadSummary(
        id=pad.id,
        org_id=pad.org_id,
        title=pad.title or DEFAULT_SHARED_PAD_TITLE,
        sort_order=int(pad.sort_order or 0),
        revision=int(pad.revision or 0),
        updated_by_name=pad.updated_by_name,
        updated_at=pad.updated_at,
    )


def list_pads(db: Session, org_id: UUID) -> List[PortalSharedPad]:
    return (
        db.query(PortalSharedPad)
        .filter(PortalSharedPad.org_id == org_id)
        .order_by(PortalSharedPad.sort_order.asc(), PortalSharedPad.created_at.asc())
        .all()
    )


def ensure_default_pad(db: Session, org_id: UUID) -> PortalSharedPad:
    pads = list_pads(db, org_id)
    if pads:
        first = pads[0]
        if (first.content or "").strip() == "" and first.updated_by is None:
            first.content = DEFAULT_SHARED_PAD_CONTENT
            if not (first.title or "").strip():
                first.title = DEFAULT_SHARED_PAD_TITLE
            first.updated_at = datetime.utcnow()
            db.commit()
            db.refresh(first)
        return first
    now = datetime.utcnow()
    pad = PortalSharedPad(
        org_id=org_id,
        title=DEFAULT_SHARED_PAD_TITLE,
        sort_order=0,
        content=DEFAULT_SHARED_PAD_CONTENT,
        revision=1,
        created_at=now,
        updated_at=now,
    )
    db.add(pad)
    db.commit()
    db.refresh(pad)
    return pad


def get_pad(db: Session, org_id: UUID, pad_id: UUID) -> PortalSharedPad:
    pad = (
        db.query(PortalSharedPad)
        .filter(PortalSharedPad.id == pad_id, PortalSharedPad.org_id == org_id)
        .first()
    )
    if not pad:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Shared space not found")
    return pad


def create_pad(
    db: Session,
    org_id: UUID,
    *,
    title: Optional[str] = None,
    user: Optional[User] = None,
) -> PortalSharedPad:
    ensure_default_pad(db, org_id)
    pads = list_pads(db, org_id)
    if len(pads) >= MAX_SHARED_PADS_PER_ORG:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Maximum of {MAX_SHARED_PADS_PER_ORG} shared spaces reached",
        )
    clean_title = (title or "").strip() or f"Shared space {len(pads) + 1}"
    if len(clean_title) > 120:
        clean_title = clean_title[:120]
    now = datetime.utcnow()
    next_order = (max((p.sort_order or 0) for p in pads) + 1) if pads else 0
    pad = PortalSharedPad(
        org_id=org_id,
        title=clean_title,
        sort_order=next_order,
        content="",
        revision=1,
        updated_by=user.id if user else None,
        updated_by_name=display_name(user) if user else None,
        created_at=now,
        updated_at=now,
    )
    db.add(pad)
    db.commit()
    db.refresh(pad)
    return pad


def rename_pad(db: Session, pad: PortalSharedPad, title: str, user: User) -> PortalSharedPad:
    clean = (title or "").strip()
    if not clean:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Title is required")
    if len(clean) > 120:
        clean = clean[:120]
    pad.title = clean
    pad.updated_by = user.id
    pad.updated_by_name = display_name(user)
    pad.updated_at = datetime.utcnow()
    # Title-only edits do not bump revision (content sync key).
    db.commit()
    db.refresh(pad)
    return pad


def write_pad_content(
    db: Session,
    pad: PortalSharedPad,
    content: str,
    user: User,
) -> PortalSharedPad:
    if len(content) > 200_000:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Content exceeds maximum length",
        )
    pad.content = content
    pad.revision = int(pad.revision or 0) + 1
    pad.updated_by = user.id
    pad.updated_by_name = display_name(user)
    pad.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(pad)
    return pad


def delete_pad(db: Session, org_id: UUID, pad: PortalSharedPad) -> None:
    remaining = len(list_pads(db, org_id))
    if remaining <= 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete the last shared space",
        )
    db.delete(pad)
    db.commit()
