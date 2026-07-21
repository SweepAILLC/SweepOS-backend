"""Org-scoped resource document storage with built-in defaults."""
from __future__ import annotations

import hashlib
import logging
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

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
            "Thorough content strategy audit via SweepOS remote MCP — buyer objections, wins, "
            "stories, ICP, and TOF/MOF/BOF fit. Optional Fathom for transcript gaps; optional pasted "
            "Reel notes for platform metrics (no TokScript)."
        ),
        "file_name": "instagram-content-audit.md",
        "powered_by": "SweepOS remote MCP (Marketing Intel + call insights)",
    },

    {
        "resource_id": "shorts-content-ideation",
        "category": "AI Skill",
        "title": "Shorts Content Ideation",
        "description": (
            "Generate 10 conversion-engineered short-form ideas from SweepOS Marketing Intel — "
            "themes, clips, wins, and ICP. Optional Fathom only when Sweep quotes are thin."
        ),
        "file_name": "shorts-content-ideation.md",
        "powered_by": "SweepOS remote MCP (get_marketing_intel)",
    },

    {
        "resource_id": "sales-call-analysis",
        "category": "AI Skill",
        "title": "Sales Call Analysis",
        "description": (
            "Complete sales diagnostic from SweepOS call themes, clips, and client insights — with "
            "scores, quote banks, and root-cause losses. Fathom fills full-transcript gaps."
        ),
        "file_name": "sales-call-analysis.md",
        "powered_by": "SweepOS remote MCP (+ optional Fathom)",
    },

    # --- SOPs (also editable by owners) ---
    {
        "resource_id": "understanding-the-business-model",
        "category": "SOP",
        "title": "Understanding the Business Model",
        "description": (
            "Watch this training to understand the Sweep methodology and how the online coaching and "
            "service model works in todays market."
        ),
        "file_name": "understanding-the-business-model.md",
        "sop_category": "foundations",
        "video_url": "https://youtu.be/xOpcWwIxHLA?si=Fyzn5N3zmgwpKDwy",
        "powered_by": None,
    },

    {
        "resource_id": "building-an-offer-sop",
        "category": "SOP",
        "title": "Building an Offer",
        "description": (
            "A system for creating an offer your market can't ignore — and can't replicate. Covers "
            "market positioning, the value equation, offer stacking, pricing, guarantees."
        ),
        "file_name": "building-an-offer-sop.md",
        "sop_category": "foundations",
        "powered_by": None,
    },

    {
        "resource_id": "defining-your-icp",
        "category": "SOP",
        "title": "Defining Your ICP",
        "description": "Identifying your target audience for more effective conversion based marketing",
        "file_name": "defining-your-icp.md",
        "sop_category": "foundations",
        "powered_by": None,
    },

    {
        "resource_id": "market-research",
        "category": "SOP",
        "title": "Market Research",
        "description": (
            "Use this SOP to validate your offer, and use an existing audience or potential customer "
            "base to stress test deliverables and positioning"
        ),
        "file_name": "market-research.md",
        "sop_category": "foundations",
        "powered_by": None,
    },

    {
        "resource_id": "your-content-funnel",
        "category": "SOP",
        "title": "Your Content Funnel",
        "description": (
            "This SOP is designed to help you understand how intentionally created content guides "
            "viewers down a buying journey and when engineered correctly, guarantees conversions "
            "regardless of follower count or vanity metrics"
        ),
        "file_name": "your-content-funnel.md",
        "sop_category": "marketing",
        "video_url": "https://youtu.be/JVsqiaojoP8?si=DcLfpTnKVTjTDFex",
        "powered_by": None,
    },

    {
        "resource_id": "short-form-content-strategy",
        "category": "SOP",
        "title": "Short Form Content Strategy",
        "description": (
            "The SOP for understanding and ideating intentional short form content that nurtures "
            "viewers into quality buyers"
        ),
        "file_name": "short-form-content-strategy.md",
        "sop_category": "marketing",
        "video_url": "https://youtu.be/K8FbAqW8ztc?si=fKyUCS_PnR5y8yE3",
        "powered_by": None,
    },

    {
        "resource_id": "building-your-personal-branding",
        "category": "SOP",
        "title": "Building Your Personal Brand",
        "description": (
            "Use this SOP to understand the science behind engineering a personal brand and what it "
            "means to create intentional associations through content"
        ),
        "file_name": "building-your-personal-branding.md",
        "sop_category": "marketing",
        "video_url": "https://youtu.be/flVNUBoKNws?si=Oq4nn7aSXewECQv9",
        "powered_by": None,
    },

    {
        "resource_id": "content-ideation-sop",
        "category": "SOP",
        "title": "Hook & Video Concept Ideation SOP",
        "description": (
            "The universal 3-layer funnel framework (hook → amplifier/filter → CTA) plus 13 "
            "niche-agnostic hook types and a weekly ideation workflow."
        ),
        "file_name": "content-ideation-sop.md",
        "sop_category": "marketing",
        "video_url": "https://www.figma.com/board/ZbC5T74AXROKCjuWvhS03A/SOP-Board?t=KZ9gXath09nOMPqN-6",
        "powered_by": (
            "Used as context for Marketing Intel — personalized by your Intelligence profile (offer, ICP, USP)."
        ),
    },

    {
        "resource_id": "content-execution",
        "category": "SOP",
        "title": "Content Execution",
        "description": (
            "Use this SOP to understand how to execute both your long form and short form content "
            "strategies for the most optimal results using formats that work"
        ),
        "file_name": "content-execution.md",
        "sop_category": "marketing",
        "video_url": "https://youtu.be/nKsojKARvKg?si=hFD8sd6FT5c-L9oY",
        "powered_by": None,
    },

    {
        "resource_id": "building-brand-associations",
        "category": "SOP",
        "title": "Building Brand Associations",
        "description": (
            "Use this SOP to understand what it means to build intentional associations and create a "
            "presence that actually means something in your market"
        ),
        "file_name": "building-brand-associations.md",
        "sop_category": "marketing",
        "video_url": "https://www.figma.com/board/ZbC5T74AXROKCjuWvhS03A/SOP-Board?t=KZ9gXath09nOMPqN-1",
        "powered_by": None,
    },

    {
        "resource_id": "messaging-and-storytelling",
        "category": "SOP",
        "title": "Messaging and Storytelling",
        "description": (
            "Integrating congruent messaging and storytelling that allows you to align yourself with "
            "the values of your dream follower and ICP while maintaining true to your personal brand"
        ),
        "file_name": "messaging-and-storytelling.md",
        "sop_category": "marketing",
        "video_url": "https://www.figma.com/board/ZbC5T74AXROKCjuWvhS03A/SOP-Board?t=KZ9gXath09nOMPqN-1",
        "powered_by": None,
    },

    {
        "resource_id": "results-based-marketing",
        "category": "SOP",
        "title": "Results Based Marketing",
        "description": (
            "How to turn results and testimonials as social proof and credibility of the "
            "effectiveness of your offer"
        ),
        "file_name": "results-based-marketing.md",
        "sop_category": "marketing",
        "powered_by": None,
    },

    {
        "resource_id": "setting-framework",
        "category": "SOP",
        "title": "Setting Framework",
        "description": (
            "Follow this SOP as a general guideline and framework to map out effective and high "
            "converting appointment setting conversations"
        ),
        "file_name": "setting-framework.md",
        "sop_category": "sales",
        "video_url": "https://www.figma.com/board/ZbC5T74AXROKCjuWvhS03A/SOP-Board?t=KZ9gXath09nOMPqN-6",
        "powered_by": None,
    },

    {
        "resource_id": "discovery-audit-sop",
        "category": "SOP",
        "title": "Sales Discovery SOP",
        "description": (
            "A scoring framework for evaluating the discovery portion of a sales call. Quantifies "
            "pain identification, daily-life grounding, tangible and intangible goals, and "
            "rapport/trust/authority."
        ),
        "file_name": "discovery-audit-sop.md",
        "sop_category": "sales",
        "video_url": "https://www.figma.com/board/ZbC5T74AXROKCjuWvhS03A/SOP-Board?t=KZ9gXath09nOMPqN-6",
        "powered_by": (
            "Used as context for Call Library — every synced call is scored against these 5 discovery dimensions."
        ),
    },

    {
        "resource_id": "pitching-sop",
        "category": "SOP",
        "title": "Sales Pitching SOP",
        "description": (
            "How to position deliverables as the natural solution to the prospect's pains — weaving "
            "discovery into the pitch, bridging goals, and framing the offer as inevitable."
        ),
        "file_name": "pitching-sop.md",
        "sop_category": "sales",
        "powered_by": "Used as context for Call Library — audits pitch quality on every synced call.",
    },

    {
        "resource_id": "objection-handling-sop",
        "category": "SOP",
        "title": "Objection Handling SOP",
        "description": (
            "Decision-tree framework for handling objections. All objections are fear or logistics — "
            "handle fear first. Includes paths for think-about-it, price, timing, proof, and partner."
        ),
        "file_name": "objection-handling-sop.md",
        "sop_category": "sales",
        "video_url": "https://www.figma.com/board/ZbC5T74AXROKCjuWvhS03A/SOP-Board?t=KZ9gXath09nOMPqN-6",
        "powered_by": "Used as context for Call Library — audits objection handling on every synced call.",
    },

    {
        "resource_id": "sales-basics",
        "category": "SOP",
        "title": "Sales Basics",
        "description": (
            "The 3 concepts every salesperson must understand when looking to optimize their offer "
            "for higher close rates on sales calls"
        ),
        "file_name": "sales-basics.md",
        "sop_category": "sales",
        "video_url": "https://youtu.be/eqQcG83bx2w?si=FpFTizB2nB7Pd6zm",
        "powered_by": None,
    },

    {
        "resource_id": "how-to-create-a-vsl",
        "category": "SOP",
        "title": "How To Create a VSL",
        "description": (
            "Reference this to see how to create effective pre and post booking VSLs as sales assets. "
            "My post-booking VSL is embedded"
        ),
        "file_name": "how-to-create-a-vsl.md",
        "sop_category": "marketing",
        "video_url": "https://youtu.be/RP6ocQ5-140",
        "powered_by": None,
    },

    {
        "resource_id": "fulfillment-systems",
        "category": "SOP",
        "title": "Fulfillment Systems",
        "description": (
            "Implement these critical systems in your fulfillment to guarantee higher client LTV and "
            "customer referral rate"
        ),
        "file_name": "fulfillment-systems.md",
        "sop_category": "fulfillment",
        "video_url": "https://youtu.be/bOViWezHetE?si=a_CziPSOg9c64yLr",
        "powered_by": None,
    },

    {
        "resource_id": "5-critical-sales-automations",
        "category": "SOP",
        "title": "5 Critical Sales Automations",
        "description": (
            "Watch this video to understand the 5 main sales operations that I have automated and "
            "allows you to scale a sales team with little to no head count"
        ),
        "file_name": "5-critical-sales-automations.md",
        "sop_category": "operations",
        "video_url": "https://youtu.be/3zDHPBiOX7g?si=B_A_di20GnAYMUwk",
        "powered_by": None,
    },

    {
        "resource_id": "the-pre-selling-funnel",
        "category": "SOP",
        "title": "The Pre-Selling Funnel",
        "description": (
            "Implement this mechanism to increase show-up rate and close rate by nurturing leads "
            "before the call even starts"
        ),
        "file_name": "the-pre-selling-funnel.md",
        "sop_category": "operations",
        "video_url": "https://youtu.be/AWhu43OY33k?si=6i8K8ycOynKwDshJ",
        "powered_by": None,
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
                sop_category TEXT,
                title       TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                content     TEXT NOT NULL DEFAULT '',
                powered_by  TEXT,
                video_url   TEXT,
                sort_order  INTEGER,
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
    db.execute(text("ALTER TABLE resource_documents ADD COLUMN IF NOT EXISTS sop_category TEXT"))
    db.execute(text("ALTER TABLE resource_documents ADD COLUMN IF NOT EXISTS video_url TEXT"))
    db.execute(text("ALTER TABLE resource_documents ADD COLUMN IF NOT EXISTS sort_order INTEGER"))
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


def normalize_video_url(raw: Optional[str]) -> Optional[str]:
    """Accept only absolute HTTP(S) URLs for resource video embeds."""
    value = (raw or "").strip()
    if not value:
        return None
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("invalid_video_url")
    return value[:2048]


SOP_CATEGORIES = {"foundations", "marketing", "sales", "operations", "fulfillment"}


def normalize_sop_category(raw: Optional[str], category: str) -> Optional[str]:
    if (category or "").strip().lower() != "sop":
        return None
    value = (raw or "").strip().lower()
    if not value:
        return None
    if value not in SOP_CATEGORIES:
        raise ValueError("invalid_sop_category")
    return value


def _row_to_dict(row) -> Dict[str, Any]:
    return {
        "resource_id": row[0],
        "category": row[1],
        "sop_category": row[2],
        "title": row[3],
        "description": row[4] or "",
        "content": row[5] or "",
        "powered_by": row[6],
        "video_url": row[7],
        "is_custom": bool(row[8]),
        "updated_at": row[9].isoformat() if row[9] else None,
        "sort_order": row[10] if len(row) > 10 else None,
    }


def list_docs(db: Session, org_id: uuid.UUID) -> List[Dict[str, Any]]:
    """Return all resource docs for an org: built-ins (with optional overrides) + custom docs."""
    org_str = str(org_id)
    rows: Dict[str, Any] = {}
    try:
        result = db.execute(
            text(
                """
                SELECT resource_id, category, sop_category, title, description, content, powered_by, video_url, is_custom, updated_at, sort_order
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
    builtin_index: Dict[str, int] = {}
    for idx, builtin in enumerate(BUILTIN_DOCS):
        rid = builtin["resource_id"]
        builtin_index[rid] = idx
        if rid in rows:
            doc = rows.pop(rid)
            if not doc.get("sop_category"):
                doc["sop_category"] = builtin.get("sop_category")
            if not doc.get("video_url"):
                doc["video_url"] = builtin.get("video_url")
            doc["is_builtin"] = True
            out.append(doc)
        else:
            out.append(
                {
                    "resource_id": rid,
                    "category": builtin.get("category") or "SOP",
                    "sop_category": builtin.get("sop_category"),
                    "title": builtin["title"],
                    "description": builtin["description"],
                    "content": "",
                    "powered_by": builtin.get("powered_by"),
                    "video_url": builtin.get("video_url"),
                    "is_custom": False,
                    "is_builtin": True,
                    "updated_at": None,
                    "sort_order": None,
                }
            )

    for rid, doc in rows.items():
        if doc.get("is_custom"):
            doc["is_builtin"] = False
            out.append(doc)

    def _sort_key(doc: Dict[str, Any]) -> tuple:
        so = doc.get("sort_order")
        rid = doc.get("resource_id") or ""
        if so is not None:
            try:
                return (0, int(so), rid)
            except (TypeError, ValueError):
                pass
        # Unordered: keep built-in catalog order, then customs by title.
        if rid in builtin_index:
            return (1, builtin_index[rid], rid)
        return (2, 0, (doc.get("title") or "").lower())

    out.sort(key=_sort_key)
    return out


def get_doc(db: Session, org_id: uuid.UUID, resource_id: str) -> Optional[Dict[str, Any]]:
    org_str = str(org_id)
    try:
        row = db.execute(
            text(
                """
                SELECT resource_id, category, sop_category, title, description, content, powered_by, video_url, is_custom, updated_at, sort_order
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
        builtin = _default_for(resource_id)
        if builtin and not doc.get("sop_category"):
            doc["sop_category"] = builtin.get("sop_category")
        if builtin and not doc.get("video_url"):
            doc["video_url"] = builtin.get("video_url")
        if builtin and not (doc.get("content") or "").strip():
            doc["content"] = _load_default_content(builtin["file_name"])
        doc["is_builtin"] = builtin is not None
        return doc

    builtin = _default_for(resource_id)
    if not builtin:
        return None

    return {
        "resource_id": resource_id,
        "category": builtin.get("category") or "SOP",
        "sop_category": builtin.get("sop_category"),
        "title": builtin["title"],
        "description": builtin["description"],
        "content": _load_default_content(builtin["file_name"]),
        "powered_by": builtin.get("powered_by"),
        "video_url": builtin.get("video_url"),
        "is_custom": False,
        "is_builtin": True,
        "updated_at": None,
        "sort_order": None,
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
    sop_category: Optional[str],
    title: str,
    description: str,
    content: str,
    powered_by: Optional[str],
    video_url: Optional[str],
    user_id: uuid.UUID,
    is_custom: bool = False,
) -> Dict[str, Any]:
    org_str = str(org_id)
    db.execute(
        text(
            """
            INSERT INTO resource_documents
                (org_id, resource_id, category, sop_category, title, description, content, powered_by, video_url, is_custom, updated_by, updated_at)
            VALUES
                (:org_id, :resource_id, :category, :sop_category, :title, :description, :content, :powered_by, :video_url, :is_custom, :user_id, now())
            ON CONFLICT (org_id, resource_id)
            DO UPDATE SET
                category = EXCLUDED.category,
                sop_category = EXCLUDED.sop_category,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                content = EXCLUDED.content,
                powered_by = EXCLUDED.powered_by,
                video_url = EXCLUDED.video_url,
                updated_by = EXCLUDED.updated_by,
                updated_at = now()
            """
        ),
        {
            "org_id": org_str,
            "resource_id": resource_id,
            "category": (category or "SOP")[:32],
            "sop_category": normalize_sop_category(sop_category, category),
            "title": title[:240],
            "description": description[:800],
            "content": content,
            "powered_by": (powered_by or None),
            "video_url": normalize_video_url(video_url),
            "is_custom": is_custom,
            "user_id": str(user_id),
        },
    )
    db.commit()
    doc = get_doc(db, org_id, resource_id)
    return doc or {}


def _next_sort_order(db: Session, org_id: uuid.UUID) -> int:
    org_str = str(org_id)
    try:
        row = db.execute(
            text(
                """
                SELECT COALESCE(MAX(sort_order), -1) + 1
                FROM resource_documents
                WHERE org_id = :org_id
                """
            ),
            {"org_id": org_str},
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


def reorder_docs(
    db: Session,
    org_id: uuid.UUID,
    resource_ids: List[str],
    *,
    user_id: uuid.UUID,
) -> List[Dict[str, Any]]:
    """Persist display order for the given resource IDs (system-owner managed)."""
    org_str = str(org_id)
    cleaned: List[str] = []
    seen = set()
    for raw in resource_ids:
        rid = str(raw or "").strip()
        if not rid or rid in seen:
            continue
        seen.add(rid)
        cleaned.append(rid)

    if not cleaned:
        raise ValueError("empty_reorder")

    for rid in cleaned:
        existing = get_doc(db, org_id, rid)
        if not existing:
            raise ValueError(f"unknown_resource:{rid}")

    for index, rid in enumerate(cleaned):
        builtin = _default_for(rid)
        existing_row = db.execute(
            text(
                """
                SELECT resource_id FROM resource_documents
                WHERE org_id = :org_id AND resource_id = :resource_id
                """
            ),
            {"org_id": org_str, "resource_id": rid},
        ).fetchone()

        if existing_row:
            db.execute(
                text(
                    """
                    UPDATE resource_documents
                    SET sort_order = :sort_order, updated_by = :user_id, updated_at = now()
                    WHERE org_id = :org_id AND resource_id = :resource_id
                    """
                ),
                {
                    "org_id": org_str,
                    "resource_id": rid,
                    "sort_order": index,
                    "user_id": str(user_id),
                },
            )
            continue

        # Built-in with no override row yet — create a lightweight order stub with default content.
        if not builtin:
            raise ValueError(f"unknown_resource:{rid}")
        db.execute(
            text(
                """
                INSERT INTO resource_documents
                    (org_id, resource_id, category, sop_category, title, description, content, powered_by, video_url, sort_order, is_custom, updated_by, updated_at)
                VALUES
                    (:org_id, :resource_id, :category, :sop_category, :title, :description, :content, :powered_by, :video_url, :sort_order, false, :user_id, now())
                """
            ),
            {
                "org_id": org_str,
                "resource_id": rid,
                "category": (builtin.get("category") or "SOP")[:32],
                "sop_category": normalize_sop_category(builtin.get("sop_category"), builtin.get("category") or "SOP"),
                "title": str(builtin["title"])[:240],
                "description": str(builtin.get("description") or "")[:800],
                "content": _load_default_content(builtin["file_name"]),
                "powered_by": builtin.get("powered_by"),
                "video_url": normalize_video_url(builtin.get("video_url")),
                "sort_order": index,
                "user_id": str(user_id),
            },
        )

    db.commit()
    return list_docs(db, org_id)


def create_doc(
    db: Session,
    org_id: uuid.UUID,
    *,
    category: str,
    sop_category: Optional[str],
    title: str,
    description: str,
    content: str,
    powered_by: Optional[str],
    video_url: Optional[str],
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

    doc = upsert_doc(
        db,
        org_id,
        candidate,
        category=category,
        sop_category=sop_category,
        title=title,
        description=description,
        content=content,
        powered_by=powered_by,
        video_url=video_url,
        user_id=user_id,
        is_custom=True,
    )
    # Append new custom docs to the end of the library order.
    try:
        next_order = _next_sort_order(db, org_id)
        db.execute(
            text(
                """
                UPDATE resource_documents
                SET sort_order = :sort_order
                WHERE org_id = :org_id AND resource_id = :resource_id
                """
            ),
            {"org_id": str(org_id), "resource_id": candidate, "sort_order": next_order},
        )
        db.commit()
    except Exception as e:
        _log.warning("resource_documents sort_order assign failed: %s", e)
    return get_doc(db, org_id, candidate) or doc


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

