"""Pipeline Grid view extras: UTM, funnel metadata, and booking status per client.

One org-scoped call for the whole grid (two queries total), so the Grid view
never fans out per row. Data comes from fields that already exist:
- UTM: ``clients.meta.prospect.utm`` (stamped at funnel lead capture)
- Metadata: the same flattened quiz/opt-in answers the funnel Leads tab shows
- Booking: the client's most recent sales-call check-in
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.clients.helpers import scope_org_id
from app.api.deps import get_current_user
from app.db.session import get_db
from app.models.client import Client
from app.models.client_checkin import ClientCheckIn
from app.models.user import User

router = APIRouter()


class ClientGridDetail(BaseModel):
    client_id: UUID
    utm: Optional[Dict[str, str]] = None
    answers: Dict[str, Any] = {}
    # "booked" (booking_at is the call time), "not_yet", "closed", "canceled", "no_show"
    booking_status: str = "not_yet"
    booking_at: Optional[datetime] = None


def booking_status_for_checkin(checkin: Optional[ClientCheckIn]) -> str:
    """Status of a client's most recent sales call — cancel/no-show outrank closed."""
    if checkin is None:
        return "not_yet"
    if checkin.cancelled:
        return "canceled"
    if checkin.no_show:
        return "no_show"
    if checkin.sale_closed:
        return "closed"
    return "booked"


@router.get("/grid-details", response_model=List[ClientGridDetail])
def get_client_grid_details(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    from app.api.funnels import _flatten_answers, normalize_utm

    org_id = scope_org_id(current_user)

    latest_call: Dict[UUID, ClientCheckIn] = {}
    checkins = (
        db.query(ClientCheckIn)
        .filter(ClientCheckIn.org_id == org_id, ClientCheckIn.is_sales_call.is_(True))
        .order_by(ClientCheckIn.start_time.asc())
        .all()
    )
    for c in checkins:
        latest_call[c.client_id] = c  # ascending order, so the last write is the most recent

    out: List[ClientGridDetail] = []
    for client_id, meta in db.query(Client.id, Client.meta).filter(Client.org_id == org_id).all():
        prospect = meta.get("prospect") if isinstance(meta, dict) and isinstance(meta.get("prospect"), dict) else {}
        checkin = latest_call.get(client_id)
        out.append(
            ClientGridDetail(
                client_id=client_id,
                utm=normalize_utm(prospect.get("utm")),
                answers=_flatten_answers(prospect) if prospect else {},
                booking_status=booking_status_for_checkin(checkin),
                booking_at=checkin.start_time if checkin is not None else None,
            )
        )
    return out
