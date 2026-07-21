"""
Resources tab — org-scoped resource documents and library.

Built-in docs (SOPs + AI skills) ship with default markdown. All org members may
read docs and manage org library items; only system owners may create or edit docs.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
import logging

from app.db.session import get_db
from app.api.deps import get_current_user, user_is_system_owner
from app.services.resource_documents import (
    ensure_resource_documents_table,
    list_docs,
    get_doc,
    upsert_doc,
    create_doc,
    delete_doc,
    reorder_docs,
    BUILTIN_DOCS,
)
from app.services.resource_library import (
    ensure_resource_library_table,
    list_library_items,
    get_library_item,
    upsert_library_item,
    delete_library_item,
)

_BUILTIN_IDS = {s["resource_id"] for s in BUILTIN_DOCS}

router = APIRouter()
_log = logging.getLogger(__name__)


def _org_id(current_user) -> str:
    return str(getattr(current_user, "selected_org_id", None) or current_user.org_id)


def _require_system_owner(current_user, db: Session) -> None:
    if not user_is_system_owner(current_user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only system owners can edit platform resource documents.",
        )


@router.get("/docs")
def get_docs(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """List all docs for the current org (metadata only, no content)."""
    from uuid import UUID

    ensure_resource_documents_table(db)
    items = list_docs(db, UUID(_org_id(current_user)))
    return [
        {
            "resource_id": i["resource_id"],
            "category": i.get("category") or "SOP",
            "sop_category": i.get("sop_category"),
            "title": i["title"],
            "description": i["description"],
            "powered_by": i.get("powered_by"),
            "video_url": i.get("video_url"),
            "is_custom": i.get("is_custom", False),
            "is_builtin": i.get("is_builtin", False),
            "updated_at": i.get("updated_at"),
            "sort_order": i.get("sort_order"),
        }
        for i in items
    ]


@router.get("/docs/{resource_id}")
def get_doc_document(
    resource_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Return full doc including markdown content."""
    from uuid import UUID
    doc = get_doc(db, UUID(_org_id(current_user)), resource_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")
    return doc


@router.put("/docs/{resource_id}")
def update_doc_document(
    resource_id: str,
    body: dict,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Create or update a document. System owners only."""
    from uuid import UUID

    _require_system_owner(current_user, db)
    ensure_resource_documents_table(db)
    org = UUID(_org_id(current_user))
    category = str(body.get("category") or "SOP").strip() or "SOP"
    sop_category = body.get("sop_category")
    if sop_category is not None:
        sop_category = str(sop_category).strip() or None
    title = str(body.get("title") or "").strip()
    description = str(body.get("description") or "").strip()
    content = str(body.get("content") or "")
    powered_by = body.get("powered_by")
    if powered_by is not None:
        powered_by = str(powered_by).strip() or None
    video_url = body.get("video_url")
    if video_url is not None:
        video_url = str(video_url).strip() or None

    if not title:
        raise HTTPException(status_code=400, detail="Title is required.")

    existing = get_doc(db, org, resource_id)
    is_custom = bool(existing and existing.get("is_custom")) or (
        resource_id not in _BUILTIN_IDS
    )

    try:
        doc = upsert_doc(
            db,
            org,
            resource_id,
            category=category,
            sop_category=sop_category,
            title=title,
            description=description,
            content=content,
            powered_by=powered_by,
            video_url=video_url,
            user_id=current_user.id,
            is_custom=is_custom,
        )
    except ValueError as e:
        if str(e) == "invalid_video_url":
            raise HTTPException(
                status_code=400,
                detail="Embed URL must be a valid http or https URL (video or Figma).",
            )
        if str(e) == "invalid_sop_category":
            raise HTTPException(status_code=400, detail="SOP category must be foundations, marketing, sales, operations, or fulfillment.")
        raise HTTPException(status_code=400, detail="Invalid document.")
    except Exception as e:
        _log.error("resource_documents upsert failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save document.")

    return doc


@router.post("/docs")
def create_doc_document(
    body: dict,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Create a new custom doc. System owners only."""
    from uuid import UUID

    _require_system_owner(current_user, db)
    ensure_resource_documents_table(db)
    category = str(body.get("category") or "SOP").strip() or "SOP"
    sop_category = body.get("sop_category")
    if sop_category is not None:
        sop_category = str(sop_category).strip() or None
    title = str(body.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title is required.")

    description = str(body.get("description") or "").strip()
    content = str(body.get("content") or "")
    powered_by = body.get("powered_by")
    if powered_by is not None:
        powered_by = str(powered_by).strip() or None
    video_url = body.get("video_url")
    if video_url is not None:
        video_url = str(video_url).strip() or None

    try:
        doc = create_doc(
            db,
            UUID(_org_id(current_user)),
            category=category,
            sop_category=sop_category,
            title=title,
            description=description,
            content=content,
            powered_by=powered_by,
            video_url=video_url,
            user_id=current_user.id,
        )
    except ValueError as e:
        if str(e) == "invalid_video_url":
            raise HTTPException(
                status_code=400,
                detail="Embed URL must be a valid http or https URL (video or Figma).",
            )
        if str(e) == "invalid_sop_category":
            raise HTTPException(status_code=400, detail="SOP category must be foundations, marketing, sales, operations, or fulfillment.")
        raise HTTPException(status_code=400, detail="Invalid document.")
    except Exception as e:
        _log.error("resource_documents create failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to create document.")

    return doc


@router.post("/docs/reorder")
def reorder_doc_documents(
    body: dict,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Persist SOP / doc display order. System owners only."""
    from uuid import UUID

    _require_system_owner(current_user, db)
    ensure_resource_documents_table(db)
    raw_ids = body.get("resource_ids")
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="resource_ids must be a list.")

    try:
        items = reorder_docs(
            db,
            UUID(_org_id(current_user)),
            [str(x) for x in raw_ids],
            user_id=current_user.id,
        )
    except ValueError as e:
        msg = str(e)
        if msg == "empty_reorder":
            raise HTTPException(status_code=400, detail="resource_ids cannot be empty.")
        if msg.startswith("unknown_resource:"):
            raise HTTPException(status_code=404, detail=f"Document not found: {msg.split(':', 1)[1]}")
        raise HTTPException(status_code=400, detail="Invalid reorder request.")
    except Exception as e:
        _log.error("resource_documents reorder failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to reorder documents.")

    return [
        {
            "resource_id": i["resource_id"],
            "category": i.get("category") or "SOP",
            "sop_category": i.get("sop_category"),
            "title": i["title"],
            "description": i["description"],
            "powered_by": i.get("powered_by"),
            "video_url": i.get("video_url"),
            "is_custom": i.get("is_custom", False),
            "is_builtin": i.get("is_builtin", False),
            "updated_at": i.get("updated_at"),
            "sort_order": i.get("sort_order"),
        }
        for i in items
    ]


@router.delete("/docs/{resource_id}")
def remove_doc_document(
    resource_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Delete a custom doc, or reset a built-in doc to its default. System owners only."""
    from uuid import UUID

    _require_system_owner(current_user, db)
    ok = delete_doc(db, UUID(_org_id(current_user)), resource_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Document not found or cannot be deleted.")
    return {"resource_id": resource_id, "deleted": True}


# Re-export for main.py startup
__all__ = ["router", "ensure_resource_documents_table"]


# ---------------------------------------------------------------------------
# Org resource library (uploads/urls)
# ---------------------------------------------------------------------------

@router.get("/library")
def list_org_library(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    from uuid import UUID
    ensure_resource_library_table(db)
    return list_library_items(db, UUID(_org_id(current_user)))


@router.get("/library/{item_id}")
def get_org_library_item(
    item_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    from uuid import UUID
    ensure_resource_library_table(db)
    doc = get_library_item(db, UUID(_org_id(current_user)), UUID(item_id))
    if not doc:
        raise HTTPException(status_code=404, detail="Library item not found.")
    return doc


@router.post("/library")
def create_org_library_item(
    body: dict,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    from uuid import UUID
    ensure_resource_library_table(db)
    try:
        return upsert_library_item(
            db,
            org_id=UUID(_org_id(current_user)),
            item_id=None,
            kind=str(body.get("kind") or ""),
            title=str(body.get("title") or ""),
            description=str(body.get("description") or ""),
            tags=body.get("tags") or [],
            content_text=body.get("content_text"),
            content_url=body.get("content_url"),
            content_b64=body.get("content_b64"),
            content_mime=body.get("content_mime"),
            user_id=current_user.id,
        )
    except ValueError as e:
        if str(e) == "title_required":
            raise HTTPException(status_code=400, detail="Title is required.")
        if str(e) == "url_required":
            raise HTTPException(status_code=400, detail="URL is required.")
        if str(e) == "image_required":
            raise HTTPException(status_code=400, detail="Image data is required.")
        raise HTTPException(status_code=400, detail="Invalid library item.")
    except Exception as e:
        _log.error("create_org_library_item failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save library item.")


@router.put("/library/{item_id}")
def update_org_library_item(
    item_id: str,
    body: dict,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    from uuid import UUID
    ensure_resource_library_table(db)
    try:
        return upsert_library_item(
            db,
            org_id=UUID(_org_id(current_user)),
            item_id=UUID(item_id),
            kind=str(body.get("kind") or ""),
            title=str(body.get("title") or ""),
            description=str(body.get("description") or ""),
            tags=body.get("tags") or [],
            content_text=body.get("content_text"),
            content_url=body.get("content_url"),
            content_b64=body.get("content_b64"),
            content_mime=body.get("content_mime"),
            user_id=current_user.id,
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid library item.")
    except Exception as e:
        _log.error("update_org_library_item failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save library item.")


@router.delete("/library/{item_id}")
def delete_org_library_item(
    item_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    from uuid import UUID
    ensure_resource_library_table(db)
    ok = delete_library_item(db, org_id=UUID(_org_id(current_user)), item_id=UUID(item_id))
    if not ok:
        raise HTTPException(status_code=404, detail="Library item not found.")
    return {"id": item_id, "deleted": True}

