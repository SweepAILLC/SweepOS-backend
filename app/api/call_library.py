"""Call Library: AI reports for Fathom calls."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import check_tab_access, get_current_user, get_db
from app.models.user import User
from app.services import call_library_service as cls

router = APIRouter()


class PatchCallLibraryBody(BaseModel):
    call_title: str = Field(..., min_length=1, max_length=500)


def _require_call_library_tab(db: Session, current_user: User) -> None:
    if not check_tab_access("call_library", current_user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Call Library is not enabled for your organization.",
        )


def _org_id(user: User) -> uuid.UUID:
    raw = getattr(user, "selected_org_id", None) or user.org_id
    return raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))


@router.get("")
def list_call_library(
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_call_library_tab(db, current_user)
    return cls.get_call_library_for_org(db, _org_id(current_user), limit=limit, offset=offset)


@router.patch("/{report_id}")
def patch_call_library_report(
    report_id: str,
    body: PatchCallLibraryBody,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_call_library_tab(db, current_user)
    try:
        rid = uuid.UUID(report_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid report id")
    row = cls.update_call_library_title(db, _org_id(current_user), rid, body.call_title)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found")
    return {"ok": True, "id": str(row.id)}


@router.post("/retry-llm-failed")
def retry_llm_failed_call_reports(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Re-queue report generation for calls where the LLM previously failed (still showing as non-complete / analyzing)."""
    _require_call_library_tab(db, current_user)
    n = cls.requeue_llm_failed_reports(db, _org_id(current_user), background_tasks)
    return {"requeued": n}
