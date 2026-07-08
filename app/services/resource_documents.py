"""Org-scoped resource document storage with built-in defaults."""
from __future__ import annotations

import hashlib
import logging
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

_log = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "resources"

BUILTIN_DOCS: List[Dict[str, Any]] = [
    # --- AI Skills (editable by owners) ---
    {
        "resource_id": "instagram-content-audit",
        "category": "AI Skill",
        "title": "Instagram Content Audit",
        "description": (
            "Analyze a client's Instagram Reels using TokScript MCP. Surfaces hook quality, re-hooks, "
            "content formats, TOF/MOF/BOF balance, credibility signals, lead gen quality, and algorithm "
            "traction — in 2 pages, zero fluff."
        ),
        "file_name": "instagram-content-audit.md",
        "powered_by": None,
    },
    {
        "resource_id": "shorts-content-ideation",
        "category": "AI Skill",
        "title": "Shorts Content Ideation",
        "description": (
            "Cross-reference best-performing Instagram content with Fathom sales call data to generate "
            "conversion-engineered short-form ideas. Hooks and CTAs map to real buyer triggers."
        ),
        "file_name": "shorts-content-ideation.md",
        "powered_by": None,
    },
    {
        "resource_id": "sales-call-analysis",
        "category": "AI Skill",
        "title": "Sales Call Analysis",
        "description": (
            "Pull past sales and check-in call transcripts from Fathom MCP to diagnose objection patterns, "
            "discovery quality, pitch effectiveness, and close mechanics — with real quotes."
        ),
        "file_name": "sales-call-analysis.md",
        "powered_by": None,
    },

    # --- SOPs (also editable by owners) ---
    {
        "resource_id": "content-ideation-sop",
        "category": "SOP",
        "title": "Hook & Video Concept Ideation SOP",
        "description": (
            "The universal 3-layer funnel framework (hook → amplifier/filter → CTA) plus 13 "
            "niche-agnostic hook types and a weekly ideation workflow."
        ),
        "file_name": "content-ideation-sop.md",
        "powered_by": (
            "Used as context for Marketing Intel — personalized by your Intelligence profile "
            "(offer, ICP, USP)."
        ),
    },
    {
        "resource_id": "building-an-offer-sop",
        "category": "SOP",
        "title": "Building an Offer",
        "description": (
            "A system for creating an offer your market can't ignore — and can't replicate. "
            "Covers market positioning, the value equation, offer stacking, pricing, guarantees."
        ),
        "file_name": "building-an-offer-sop.md",
        "powered_by": None,
    },
    {
        "resource_id": "discovery-audit-sop",
        "category": "SOP",
        "title": "Discovery Call Audit",
        "description": (
            "A scoring framework for evaluating the discovery portion of a sales call. "
            "Quantifies pain identification, daily-life grounding, tangible and intangible goals, "
            "and rapport/trust/authority."
        ),
        "file_name": "discovery-audit-sop.md",
        "powered_by": (
            "Used as context for Call Library — every synced call is scored against these "
            "5 discovery dimensions."
        ),
    },
    {
        "resource_id": "pitching-sop",
        "category": "SOP",
        "title": "Pitching",
        "description": (
            "How to position deliverables as the natural solution to the prospect's pains — "
            "weaving discovery into the pitch, bridging goals, and framing the offer as inevitable."
        ),
        "file_name": "pitching-sop.md",
        "powered_by": (
            "Used as context for Call Library — audits pitch quality on every synced call."
        ),
    },
    {
        "resource_id": "objection-handling-sop",
        "category": "SOP",
        "title": "Objection Handling",
        "description": (
            "Decision-tree framework for handling objections. All objections are fear or logistics — "
            "handle fear first. Includes paths for think-about-it, price, timing, proof, and partner."
        ),
        "file_name": "objection-handling-sop.md",
        "powered_by": (
            "Used as context for Call Library — audits objection handling on every synced call."
        ),
    },
]

_BUILTIN_IDS = {s["resource_id"] for s in BUILTIN_DOCS}


def ensure_resource_documents_table(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS resource_documents (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id      UUID NOT NULL,
                resource_id TEXT NOT NULL,
                category    TEXT NOT NULL DEFAULT 'SOP',
                title       TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                content     TEXT NOT NULL DEFAULT '',
                powered_by  TEXT,
                is_custom   BOOLEAN NOT NULL DEFAULT false,
                updated_by  UUID,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (org_id, resource_id)
            )
            """
        )
    )
    # Older installs may have created the table before category existed.
    db.execute(text("ALTER TABLE resource_documents ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'SOP'"))
    db.commit()


def _slugify(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return slug[:80] or "sop"


def _load_default_content(file_name: str) -> str:
    path = _DATA_DIR / file_name
    try:
        if path.is_file():
            return path.read_text(encoding="utf-8")
    except OSError as e:
        _log.warning("Failed to read default resource doc %s: %s", file_name, e)
    return ""


def _default_for(resource_id: str) -> Optional[Dict[str, Any]]:
    for doc in BUILTIN_DOCS:
        if doc["resource_id"] == resource_id:
            return doc
    return None


def _row_to_dict(row) -> Dict[str, Any]:
    return {
        "resource_id": row[0],
        "category": row[1],
        "title": row[2],
        "description": row[3] or "",
        "content": row[4] or "",
        "powered_by": row[5],
        "is_custom": bool(row[6]),
        "updated_at": row[7].isoformat() if row[7] else None,
    }


def list_docs(db: Session, org_id: uuid.UUID) -> List[Dict[str, Any]]:
    """Return all resource docs for an org: built-ins (with optional overrides) + custom docs."""
    org_str = str(org_id)
    rows: Dict[str, Any] = {}
    try:
        result = db.execute(
            text(
                """
                SELECT resource_id, category, title, description, content, powered_by, is_custom, updated_at
                FROM resource_documents
                WHERE org_id = :org_id
                ORDER BY updated_at DESC
                """
            ),
            {"org_id": org_str},
        ).fetchall()
        for r in result:
            rows[r[0]] = _row_to_dict(r)
    except Exception as e:
        _log.warning("resource_documents list failed: %s", e)

    out: List[Dict[str, Any]] = []
    for builtin in BUILTIN_DOCS:
        rid = builtin["resource_id"]
        if rid in rows:
            doc = rows.pop(rid)
            doc["is_builtin"] = True
            out.append(doc)
        else:
            out.append(
                {
                    "resource_id": rid,
                    "category": builtin.get("category") or "SOP",
                    "title": builtin["title"],
                    "description": builtin["description"],
                    "content": "",
                    "powered_by": builtin.get("powered_by"),
                    "is_custom": False,
                    "is_builtin": True,
                    "updated_at": None,
                }
            )

    for rid, doc in rows.items():
        if doc.get("is_custom"):
            doc["is_builtin"] = False
            out.append(doc)

    return out


def get_doc(db: Session, org_id: uuid.UUID, resource_id: str) -> Optional[Dict[str, Any]]:
    org_str = str(org_id)
    try:
        row = db.execute(
            text(
                """
                SELECT resource_id, category, title, description, content, powered_by, is_custom, updated_at
                FROM resource_documents
                WHERE org_id = :org_id AND resource_id = :resource_id
                """
            ),
            {"org_id": org_str, "resource_id": resource_id},
        ).fetchone()
    except Exception as e:
        _log.warning("resource_documents get failed: %s", e)
        row = None

    if row:
        doc = _row_to_dict(row)
        doc["is_builtin"] = resource_id in _BUILTIN_IDS
        return doc

    builtin = _default_for(resource_id)
    if not builtin:
        return None

    return {
        "resource_id": resource_id,
        "category": builtin.get("category") or "SOP",
        "title": builtin["title"],
        "description": builtin["description"],
        "content": _load_default_content(builtin["file_name"]),
        "powered_by": builtin.get("powered_by"),
        "is_custom": False,
        "is_builtin": True,
        "updated_at": None,
    }


def get_document_content(db: Session, org_id: uuid.UUID, resource_id: str, *, fallback: str = "") -> str:
    """Return doc markdown for LLM context — DB override, then default file, then fallback."""
    doc = get_doc(db, org_id, resource_id)
    if doc and (doc.get("content") or "").strip():
        return str(doc["content"]).strip()
    return fallback.strip()


def sop_content_fingerprint(db: Session, org_id: uuid.UUID, resource_ids: List[str]) -> str:
    parts = [get_document_content(db, org_id, rid) for rid in resource_ids]
    raw = "\n---\n".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def upsert_doc(
    db: Session,
    org_id: uuid.UUID,
    resource_id: str,
    *,
    category: str,
    title: str,
    description: str,
    content: str,
    powered_by: Optional[str],
    user_id: uuid.UUID,
    is_custom: bool = False,
) -> Dict[str, Any]:
    org_str = str(org_id)
    db.execute(
        text(
            """
            INSERT INTO resource_documents
                (org_id, resource_id, category, title, description, content, powered_by, is_custom, updated_by, updated_at)
            VALUES
                (:org_id, :resource_id, :category, :title, :description, :content, :powered_by, :is_custom, :user_id, now())
            ON CONFLICT (org_id, resource_id)
            DO UPDATE SET
                category = EXCLUDED.category,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                content = EXCLUDED.content,
                powered_by = EXCLUDED.powered_by,
                updated_by = EXCLUDED.updated_by,
                updated_at = now()
            """
        ),
        {
            "org_id": org_str,
            "resource_id": resource_id,
            "category": (category or "SOP")[:32],
            "title": title[:240],
            "description": description[:800],
            "content": content,
            "powered_by": (powered_by or None),
            "is_custom": is_custom,
            "user_id": str(user_id),
        },
    )
    db.commit()
    doc = get_doc(db, org_id, resource_id)
    return doc or {}


def create_doc(
    db: Session,
    org_id: uuid.UUID,
    *,
    category: str,
    title: str,
    description: str,
    content: str,
    powered_by: Optional[str],
    user_id: uuid.UUID,
    resource_id: Optional[str] = None,
) -> Dict[str, Any]:
    base_id = resource_id or _slugify(title)
    if base_id in _BUILTIN_IDS:
        base_id = f"{base_id}-custom"
    candidate = base_id
    n = 1
    while get_sop(db, org_id, candidate) is not None:
        candidate = f"{base_id}-{n}"
        n += 1
        if n > 100:
            candidate = f"{base_id}-{uuid.uuid4().hex[:8]}"
            break

    return upsert_doc(
        db,
        org_id,
        candidate,
        category=category,
        title=title,
        description=description,
        content=content,
        powered_by=powered_by,
        user_id=user_id,
        is_custom=True,
    )


def delete_doc(db: Session, org_id: uuid.UUID, resource_id: str) -> bool:
    if resource_id in _BUILTIN_IDS:
        # Reset built-in to defaults by removing override row
        org_str = str(org_id)
        db.execute(
            text("DELETE FROM resource_documents WHERE org_id = :org_id AND resource_id = :resource_id"),
            {"org_id": org_str, "resource_id": resource_id},
        )
        db.commit()
        return True

    org_str = str(org_id)
    result = db.execute(
        text(
            """
            DELETE FROM resource_documents
            WHERE org_id = :org_id AND resource_id = :resource_id AND is_custom = true
            """
        ),
        {"org_id": org_str, "resource_id": resource_id},
    )
    db.commit()
    return result.rowcount > 0


# Backwards-compatible aliases (older code paths)
list_sops = list_docs
get_sop = get_doc
get_sop_content = get_document_content
upsert_sop = upsert_doc
create_sop = create_doc
delete_sop = delete_doc

