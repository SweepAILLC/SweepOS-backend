"""
Org portal API — consulting program shared pads (and legacy todos).
"""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional
from uuid import UUID
from datetime import datetime

from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User
from app.models.portal_todo import PortalTodo
from app.models.portal_shared_pad import MAX_SHARED_PADS_PER_ORG
from app.schemas.portal import (
    PortalTodoCreate,
    PortalTodoUpdate,
    PortalTodoResponse,
    PortalSharedPadCreate,
    PortalSharedPadRename,
    PortalSharedPadUpdate,
    PortalSharedPadResponse,
    PortalSharedPadSummary,
)
from app.services import portal_shared_pads as pads_svc

router = APIRouter()


def _org_id(current_user: User) -> UUID:
    return UUID(str(getattr(current_user, "selected_org_id", None) or current_user.org_id))


# ----- Shared pads (multi-tab live notepad) -----------------------------------


@router.get("/shared-pads", response_model=List[PortalSharedPadSummary])
def list_shared_pads(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List named shared-space tabs for the current organization (max 10)."""
    org_id = _org_id(current_user)
    pads_svc.ensure_default_pad(db, org_id)
    return [pads_svc.pad_summary(p) for p in pads_svc.list_pads(db, org_id)]


@router.post(
    "/shared-pads",
    response_model=PortalSharedPadResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_shared_pad(
    body: PortalSharedPadCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create a new shared-space tab (max 10 per org)."""
    org_id = _org_id(current_user)
    pad = pads_svc.create_pad(db, org_id, title=body.title, user=current_user)
    return pads_svc.pad_response(pad)


@router.get("/shared-pads/{pad_id}", response_model=PortalSharedPadResponse)
def get_shared_pad_by_id(
    pad_id: UUID,
    since_revision: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get one shared-space tab (supports since_revision for cheap live polls)."""
    org_id = _org_id(current_user)
    pad = pads_svc.get_pad(db, org_id, pad_id)
    if since_revision is not None and int(since_revision) == int(pad.revision or 0):
        return pads_svc.pad_response(pad, unchanged=True)
    return pads_svc.pad_response(pad)


@router.put("/shared-pads/{pad_id}", response_model=PortalSharedPadResponse)
def put_shared_pad_by_id(
    pad_id: UUID,
    body: PortalSharedPadUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Replace content for one shared-space tab."""
    org_id = _org_id(current_user)
    pad = pads_svc.get_pad(db, org_id, pad_id)
    content = body.content if body.content is not None else ""
    pad = pads_svc.write_pad_content(db, pad, content, current_user)
    return pads_svc.pad_response(pad)


@router.patch("/shared-pads/{pad_id}", response_model=PortalSharedPadResponse)
def rename_shared_pad(
    pad_id: UUID,
    body: PortalSharedPadRename,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Rename a shared-space tab (double-click rename in UI)."""
    org_id = _org_id(current_user)
    pad = pads_svc.get_pad(db, org_id, pad_id)
    pad = pads_svc.rename_pad(db, pad, body.title, current_user)
    return pads_svc.pad_response(pad)


@router.delete("/shared-pads/{pad_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_shared_pad(
    pad_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a shared-space tab (cannot delete the last one)."""
    org_id = _org_id(current_user)
    pad = pads_svc.get_pad(db, org_id, pad_id)
    pads_svc.delete_pad(db, org_id, pad)
    return None


# Legacy single-pad routes (first tab) ----------------------------------------


@router.get("/shared-pad", response_model=PortalSharedPadResponse)
def get_shared_pad(
    since_revision: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Legacy: get the first shared pad for the current org."""
    org_id = _org_id(current_user)
    pad = pads_svc.ensure_default_pad(db, org_id)
    if since_revision is not None and int(since_revision) == int(pad.revision or 0):
        return pads_svc.pad_response(pad, unchanged=True)
    return pads_svc.pad_response(pad)


@router.put("/shared-pad", response_model=PortalSharedPadResponse)
def put_shared_pad(
    body: PortalSharedPadUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Legacy: update the first shared pad for the current org."""
    org_id = _org_id(current_user)
    pad = pads_svc.ensure_default_pad(db, org_id)
    content = body.content if body.content is not None else ""
    pad = pads_svc.write_pad_content(db, pad, content, current_user)
    return pads_svc.pad_response(pad)


@router.get("/shared-pads-limit")
def shared_pads_limit():
    return {"max": MAX_SHARED_PADS_PER_ORG}


# ----- Legacy todos (kept for API compatibility) -----------------------------


@router.get("/todos", response_model=List[PortalTodoResponse])
def list_portal_todos(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List to-dos for the current organization."""
    org_id = _org_id(current_user)
    rows = (
        db.query(PortalTodo)
        .filter(PortalTodo.org_id == org_id)
        .order_by(PortalTodo.completed.asc(), PortalTodo.created_at.desc())
        .all()
    )
    return rows


@router.post("/todos", response_model=PortalTodoResponse, status_code=status.HTTP_201_CREATED)
def create_portal_todo(
    body: PortalTodoCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create a to-do for the current organization."""
    title = (body.title or "").strip()
    if not title:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Title is required")

    org_id = _org_id(current_user)
    now = datetime.utcnow()
    todo = PortalTodo(
        org_id=org_id,
        title=title,
        description=(body.description or None),
        completed=False,
        due_date=body.due_date,
        created_by=current_user.id,
        created_at=now,
        updated_at=now,
    )
    db.add(todo)
    db.commit()
    db.refresh(todo)
    return todo


@router.patch("/todos/{todo_id}", response_model=PortalTodoResponse)
def update_portal_todo(
    todo_id: UUID,
    body: PortalTodoUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update a to-do (title, description, completed, due_date)."""
    org_id = _org_id(current_user)
    todo = (
        db.query(PortalTodo)
        .filter(PortalTodo.id == todo_id, PortalTodo.org_id == org_id)
        .first()
    )
    if not todo:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="To-do not found")

    if body.title is not None:
        title = body.title.strip()
        if not title:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Title is required")
        todo.title = title
    if body.description is not None:
        todo.description = body.description or None
    if body.completed is not None:
        todo.completed = body.completed
    if body.due_date is not None:
        todo.due_date = body.due_date

    todo.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(todo)
    return todo


@router.delete("/todos/{todo_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_portal_todo(
    todo_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a to-do for the current organization."""
    org_id = _org_id(current_user)
    todo = (
        db.query(PortalTodo)
        .filter(PortalTodo.id == todo_id, PortalTodo.org_id == org_id)
        .first()
    )
    if not todo:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="To-do not found")

    db.delete(todo)
    db.commit()
    return None
