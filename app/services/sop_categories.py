"""Owner-managed SOP categories for the global platform SOP catalog.

Categories live on Sweep Internal (same global org as ``resource_documents``).
System owners can create, rename, recolor, reorder, and delete them; every org
reads the same list. Documents reference a category by its stable ``slug``.
"""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

_log = logging.getLogger(__name__)

# Same id as resource_documents.GLOBAL_RESOURCE_ORG_ID (kept local to avoid a circular import).
GLOBAL_ORG_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")

ALLOWED_COLORS = (
    "amber",
    "pink",
    "emerald",
    "sky",
    "orange",
    "violet",
    "rose",
    "teal",
    "indigo",
    "slate",
)
DEFAULT_COLOR = "sky"
MAX_LABEL_LEN = 40

DEFAULT_CATEGORIES: List[Dict[str, str]] = [
    {"slug": "foundations", "label": "Foundations", "color": "amber"},
    {"slug": "marketing", "label": "Marketing", "color": "pink"},
    {"slug": "sales", "label": "Sales", "color": "emerald"},
    {"slug": "fulfillment", "label": "Fulfillment", "color": "orange"},
    {"slug": "operations", "label": "Operations", "color": "sky"},
]
DEFAULT_SLUGS: Set[str] = {c["slug"] for c in DEFAULT_CATEGORIES}


def ensure_sop_categories_table(db: Session) -> None:
    """Create the table and seed the defaults exactly once (when the table is first created)."""
    exists = db.execute(text("SELECT to_regclass('sop_categories')")).scalar()
    if exists:
        return
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS sop_categories (
                org_id     UUID NOT NULL,
                slug       TEXT NOT NULL,
                label      TEXT NOT NULL,
                color      TEXT NOT NULL DEFAULT 'sky',
                sort_order INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (org_id, slug)
            )
            """
        )
    )
    for idx, cat in enumerate(DEFAULT_CATEGORIES):
        db.execute(
            text(
                """
                INSERT INTO sop_categories (org_id, slug, label, color, sort_order)
                VALUES (:org_id, :slug, :label, :color, :sort_order)
                ON CONFLICT (org_id, slug) DO NOTHING
                """
            ),
            {"org_id": str(GLOBAL_ORG_ID), **cat, "sort_order": idx},
        )
    db.commit()


def _row_to_dict(row) -> Dict[str, Any]:
    return {"slug": row[0], "label": row[1], "color": row[2], "sort_order": row[3]}


def list_categories(db: Session) -> List[Dict[str, Any]]:
    ensure_sop_categories_table(db)
    rows = db.execute(
        text(
            """
            SELECT slug, label, color, sort_order
            FROM sop_categories
            WHERE org_id = :org_id
            ORDER BY sort_order ASC, label ASC
            """
        ),
        {"org_id": str(GLOBAL_ORG_ID)},
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def valid_category_slugs(db: Session) -> Set[str]:
    """Slugs currently defined. Falls back to the defaults if the lookup fails."""
    try:
        return {c["slug"] for c in list_categories(db)}
    except Exception as e:  # defensive; keeps doc listing alive
        _log.warning("sop_categories lookup failed: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        return set(DEFAULT_SLUGS)


def _clean_label(raw: Optional[str]) -> str:
    label = re.sub(r"\s+", " ", str(raw or "")).strip()
    if not label:
        raise ValueError("label_required")
    return label[:MAX_LABEL_LEN]


def _clean_color(raw: Optional[str]) -> str:
    color = str(raw or DEFAULT_COLOR).strip().lower()
    if color not in ALLOWED_COLORS:
        raise ValueError("invalid_color")
    return color


def _slugify(label: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
    return slug[:40] or "category"


def create_category(db: Session, *, label: str, color: Optional[str]) -> Dict[str, Any]:
    ensure_sop_categories_table(db)
    label = _clean_label(label)
    color = _clean_color(color)
    existing = valid_category_slugs(db)
    base = _slugify(label)
    slug = base
    n = 2
    while slug in existing:
        slug = f"{base}-{n}"
        n += 1
    next_order = db.execute(
        text("SELECT COALESCE(MAX(sort_order), -1) + 1 FROM sop_categories WHERE org_id = :org_id"),
        {"org_id": str(GLOBAL_ORG_ID)},
    ).scalar()
    db.execute(
        text(
            """
            INSERT INTO sop_categories (org_id, slug, label, color, sort_order)
            VALUES (:org_id, :slug, :label, :color, :sort_order)
            """
        ),
        {
            "org_id": str(GLOBAL_ORG_ID),
            "slug": slug,
            "label": label,
            "color": color,
            "sort_order": int(next_order or 0),
        },
    )
    db.commit()
    return {"slug": slug, "label": label, "color": color, "sort_order": int(next_order or 0)}


def update_category(
    db: Session,
    slug: str,
    *,
    label: Optional[str] = None,
    color: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    ensure_sop_categories_table(db)
    sets: List[str] = []
    params: Dict[str, Any] = {"org_id": str(GLOBAL_ORG_ID), "slug": slug}
    if label is not None:
        sets.append("label = :label")
        params["label"] = _clean_label(label)
    if color is not None:
        sets.append("color = :color")
        params["color"] = _clean_color(color)
    if not sets:
        raise ValueError("nothing_to_update")
    result = db.execute(
        text(
            f"UPDATE sop_categories SET {', '.join(sets)}, updated_at = now() "
            "WHERE org_id = :org_id AND slug = :slug"
        ),
        params,
    )
    db.commit()
    if result.rowcount == 0:
        return None
    for cat in list_categories(db):
        if cat["slug"] == slug:
            return cat
    return None


def delete_category(db: Session, slug: str) -> Optional[int]:
    """Delete a category. SOPs in it become uncategorized. Returns the SOP count affected, or None if missing."""
    ensure_sop_categories_table(db)
    result = db.execute(
        text("DELETE FROM sop_categories WHERE org_id = :org_id AND slug = :slug"),
        {"org_id": str(GLOBAL_ORG_ID), "slug": slug},
    )
    if result.rowcount == 0:
        db.rollback()
        return None
    affected = db.execute(
        text(
            """
            UPDATE resource_documents
            SET sop_category = NULL, updated_at = now()
            WHERE org_id = :org_id AND sop_category = :slug
            """
        ),
        {"org_id": str(GLOBAL_ORG_ID), "slug": slug},
    ).rowcount
    db.commit()
    return int(affected or 0)


def reorder_categories(db: Session, slugs: List[str]) -> List[Dict[str, Any]]:
    ensure_sop_categories_table(db)
    existing = valid_category_slugs(db)
    cleaned: List[str] = []
    for raw in slugs:
        slug = str(raw or "").strip()
        if slug and slug in existing and slug not in cleaned:
            cleaned.append(slug)
    if not cleaned:
        raise ValueError("empty_reorder")
    # Anything not mentioned keeps its relative order after the listed ones.
    rest = [c["slug"] for c in list_categories(db) if c["slug"] not in cleaned]
    for index, slug in enumerate(cleaned + rest):
        db.execute(
            text(
                "UPDATE sop_categories SET sort_order = :sort_order "
                "WHERE org_id = :org_id AND slug = :slug"
            ),
            {"org_id": str(GLOBAL_ORG_ID), "slug": slug, "sort_order": index},
        )
    db.commit()
    return list_categories(db)
