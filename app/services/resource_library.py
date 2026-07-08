"""Org-scoped library for storing internal resources (text/md/images/video URLs)."""
from __future__ import annotations

import base64
import json
import logging
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

_log = logging.getLogger(__name__)


ALLOWED_KINDS = {"text", "markdown", "image", "video_url", "url"}
ALLOWED_TAGS = {"testimonials", "case_studies", "value", "SOP", "ai", "other"}


def ensure_resource_library_table(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS org_resource_library (
                id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id        UUID NOT NULL,
                kind          TEXT NOT NULL,
                title         TEXT NOT NULL,
                description   TEXT NOT NULL DEFAULT '',
                tags          JSONB NOT NULL DEFAULT '[]'::jsonb,
                content_text  TEXT,
                content_url   TEXT,
                content_b64   TEXT,
                content_mime  TEXT,
                created_by    UUID,
                updated_by    UUID,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_org_resource_library_org_id ON org_resource_library (org_id)"))
    db.commit()


def _sanitize_tags(tags: Any) -> List[str]:
    if not isinstance(tags, list):
        return []
    out: List[str] = []
    for t in tags[:12]:
        s = str(t).strip()
        if not s:
            continue
        # Keep user casing for display, but constrain to known buckets
        if s in ALLOWED_TAGS:
            out.append(s)
        elif s.lower() in {x.lower() for x in ALLOWED_TAGS}:
            # map case-insensitively
            for x in ALLOWED_TAGS:
                if x.lower() == s.lower():
                    out.append(x)
                    break
    # stable unique
    uniq: List[str] = []
    for x in out:
        if x not in uniq:
            uniq.append(x)
    return uniq


def _row_to_dict(r) -> Dict[str, Any]:
    raw_tags = r[4]
    if isinstance(raw_tags, list):
        tags_out = raw_tags
    elif isinstance(raw_tags, str):
        try:
            parsed = json.loads(raw_tags)
            tags_out = parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            tags_out = []
    else:
        tags_out = []

    return {
        "id": str(r[0]),
        "kind": r[1],
        "title": r[2],
        "description": r[3] or "",
        "tags": tags_out,
        "content_text": r[5],
        "content_url": r[6],
        "content_b64": r[7],
        "content_mime": r[8],
        "updated_at": r[9].isoformat() if r[9] else None,
        "created_at": r[10].isoformat() if r[10] else None,
    }


def list_library_items(db: Session, org_id: uuid.UUID) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT id, kind, title, description, tags, content_text, content_url, content_b64, content_mime, updated_at, created_at
            FROM org_resource_library
            WHERE org_id = :org_id
            ORDER BY updated_at DESC
            LIMIT 500
            """
        ),
        {"org_id": str(org_id)},
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_library_item(db: Session, org_id: uuid.UUID, item_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    r = db.execute(
        text(
            """
            SELECT id, kind, title, description, tags, content_text, content_url, content_b64, content_mime, updated_at, created_at
            FROM org_resource_library
            WHERE org_id = :org_id AND id = :id
            """
        ),
        {"org_id": str(org_id), "id": str(item_id)},
    ).fetchone()
    return _row_to_dict(r) if r else None


def upsert_library_item(
    db: Session,
    *,
    org_id: uuid.UUID,
    item_id: Optional[uuid.UUID],
    kind: str,
    title: str,
    description: str,
    tags: Any,
    content_text: Optional[str],
    content_url: Optional[str],
    content_b64: Optional[str],
    content_mime: Optional[str],
    user_id: uuid.UUID,
) -> Dict[str, Any]:
    kind_norm = str(kind or "").strip()
    if kind_norm not in ALLOWED_KINDS:
        raise ValueError("invalid_kind")
    title_norm = str(title or "").strip()
    if not title_norm:
        raise ValueError("title_required")

    tags_norm = _sanitize_tags(tags)

    ct = (content_text or None) if content_text is None else str(content_text)
    cu = (content_url or None) if content_url is None else str(content_url).strip() or None
    cb = (content_b64 or None) if content_b64 is None else str(content_b64).strip() or None
    cm = (content_mime or None) if content_mime is None else str(content_mime).strip() or None

    # Validate per kind
    if kind_norm in {"text", "markdown"}:
        cb = None
        cm = None
        cu = None
        ct = (ct or "").strip()
    elif kind_norm in {"video_url", "url"}:
        ct = None
        cb = None
        cm = None
        if not cu:
            raise ValueError("url_required")
    elif kind_norm == "image":
        ct = None
        cu = None
        if not cb or not cm:
            raise ValueError("image_required")
        # lightweight sanity check base64
        try:
            base64.b64decode(cb[:2000] + "==", validate=False)
        except Exception:
            raise ValueError("invalid_image")

    tags_json = json.dumps(tags_norm)
    new_id = item_id or uuid.uuid4()
    try:
        db.execute(
            text(
                """
                INSERT INTO org_resource_library
                    (id, org_id, kind, title, description, tags, content_text, content_url, content_b64, content_mime, created_by, updated_by, created_at, updated_at)
                VALUES
                    (:id, :org_id, :kind, :title, :description, CAST(:tags AS JSONB), :content_text, :content_url, :content_b64, :content_mime, :user_id, :user_id, now(), now())
                ON CONFLICT (id)
                DO UPDATE SET
                    kind = EXCLUDED.kind,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    tags = EXCLUDED.tags,
                    content_text = EXCLUDED.content_text,
                    content_url = EXCLUDED.content_url,
                    content_b64 = EXCLUDED.content_b64,
                    content_mime = EXCLUDED.content_mime,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = now()
                """
            ),
            {
                "id": str(new_id),
                "org_id": str(org_id),
                "kind": kind_norm,
                "title": title_norm[:240],
                "description": str(description or "")[:800],
                "tags": tags_json,
                "content_text": ct,
                "content_url": cu,
                "content_b64": cb,
                "content_mime": cm,
                "user_id": str(user_id),
            },
        )
        db.commit()
    except Exception as e:
        db.rollback()
        _log.error("org_resource_library upsert failed: %s", e)
        raise
    doc = get_library_item(db, org_id, new_id)
    return doc or {}


def delete_library_item(db: Session, *, org_id: uuid.UUID, item_id: uuid.UUID) -> bool:
    res = db.execute(
        text("DELETE FROM org_resource_library WHERE org_id = :org_id AND id = :id"),
        {"org_id": str(org_id), "id": str(item_id)},
    )
    db.commit()
    return res.rowcount > 0

