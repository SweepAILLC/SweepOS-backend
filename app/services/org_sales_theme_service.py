"""Persist and query org-level recurring objection / circumstance themes from call insights."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.call_library_report import CallLibraryReport
from app.models.client_call_insight import ClientCallInsight
from app.models.org_sales_content_theme import OrgSalesContentTheme
from app.services.org_sales_theme_normalize import (
    theme_key_and_label_from_clip,
    theme_key_and_label_from_phrase,
)

logger = logging.getLogger(__name__)

_TABLES_ENSURED = False


def _quote_for_theme_key(ij: Dict[str, Any], theme_key: str) -> str:
    for c in ij.get("clips") or []:
        if not isinstance(c, dict):
            continue
        parsed = theme_key_and_label_from_clip(c)
        if parsed and parsed[0] == theme_key:
            return str(c.get("quote") or "").strip()[:400]
    return ""


def _avoid_phrase_for_key(ij: Dict[str, Any], theme_key: str) -> str:
    pv = ij.get("prospect_voice")
    if not isinstance(pv, dict):
        return ""
    for ap in pv.get("avoid_phrasing") or []:
        parsed = theme_key_and_label_from_phrase(str(ap))
        if parsed and parsed[0] == theme_key:
            return str(ap).strip()[:400]
    return ""


def ensure_org_sales_content_themes_table(db: Session) -> None:
    global _TABLES_ENSURED
    if _TABLES_ENSURED:
        return
    try:
        db.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS org_sales_content_themes (
                id UUID PRIMARY KEY,
                org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                theme_key VARCHAR(32) NOT NULL,
                source VARCHAR(16) NOT NULL DEFAULT 'check_in',
                kind VARCHAR(16),
                label VARCHAR(220),
                occurrence_count INTEGER NOT NULL DEFAULT 0,
                distinct_client_count INTEGER NOT NULL DEFAULT 0,
                closed_won_count INTEGER NOT NULL DEFAULT 0,
                contributing_client_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                sample_quotes JSONB NOT NULL DEFAULT '[]'::jsonb,
                first_seen_at TIMESTAMPTZ NOT NULL,
                last_seen_at TIMESTAMPTZ NOT NULL,
                CONSTRAINT uq_org_sales_content_theme_org_key_source UNIQUE (org_id, theme_key, source)
            )
            """
            )
        )
        # Pre-existing deployments created the table before source/closed_won_count existed —
        # add them idempotently and swap the old (org_id, theme_key) constraint for the new
        # source-aware one so sales_call and check_in rows for the same theme_key don't collide.
        db.execute(
            text(
                "ALTER TABLE org_sales_content_themes "
                "ADD COLUMN IF NOT EXISTS source VARCHAR(16) NOT NULL DEFAULT 'check_in'"
            )
        )
        db.execute(
            text(
                "ALTER TABLE org_sales_content_themes "
                "ADD COLUMN IF NOT EXISTS closed_won_count INTEGER NOT NULL DEFAULT 0"
            )
        )
        db.execute(
            text(
                "ALTER TABLE org_sales_content_themes ADD COLUMN IF NOT EXISTS kind VARCHAR(16)"
            )
        )
        db.execute(
            text(
                "ALTER TABLE org_sales_content_themes "
                "DROP CONSTRAINT IF EXISTS uq_org_sales_content_theme_org_key"
            )
        )
        # Postgres has no "ADD CONSTRAINT IF NOT EXISTS" — CREATE TABLE already added this
        # constraint on a fresh install, so guard the ALTER path with a catalog check.
        db.execute(
            text(
                """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'uq_org_sales_content_theme_org_key_source'
                ) THEN
                    ALTER TABLE org_sales_content_themes
                    ADD CONSTRAINT uq_org_sales_content_theme_org_key_source
                    UNIQUE (org_id, theme_key, source);
                END IF;
            END $$;
            """
            )
        )
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_org_sales_content_themes_org_id ON org_sales_content_themes (org_id)"
            )
        )
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_org_sales_content_themes_org_last_seen ON org_sales_content_themes (org_id, last_seen_at)"
            )
        )
        db.commit()
    except Exception as e:
        db.rollback()
        logger.warning("ensure_org_sales_content_themes_table: %s", e)
        raise
    _TABLES_ENSURED = True


def record_from_completed_insight(db: Session, row: ClientCallInsight) -> None:
    """Upsert theme rows from a completed ClientCallInsight (idempotent per insight contribution)."""
    if row.status != "complete" or not row.insight_json or not isinstance(row.insight_json, dict):
        return
    ensure_org_sales_content_themes_table(db)

    org_id = row.org_id
    client_id_str = str(row.client_id)
    ij = row.insight_json

    contributions: List[tuple[str, str, str]] = []

    for c in ij.get("clips") or []:
        if not isinstance(c, dict):
            continue
        kind = str(c.get("kind") or "").lower()
        if kind not in ("objection", "question", "other"):
            continue
        parsed = theme_key_and_label_from_clip(c)
        if parsed:
            contributions.append((parsed[0], parsed[1], kind))

    pv = ij.get("prospect_voice")
    if isinstance(pv, dict):
        for ap in pv.get("avoid_phrasing") or []:
            parsed = theme_key_and_label_from_phrase(str(ap))
            if parsed:
                # avoid_phrasing has no clip kind of its own — it's always objection-flavored
                # pushback by construction (words the prospect reacted coolly to).
                contributions.append((parsed[0], parsed[1], "objection"))

    if not contributions:
        return

    now = datetime.now(timezone.utc)
    max_clients = int(getattr(settings, "ORG_SALES_THEME_MAX_CONTRIBUTING_CLIENTS", 500) or 500)
    max_samples = int(getattr(settings, "ORG_SALES_THEME_MAX_SAMPLE_QUOTES", 8) or 8)

    seen_keys: Set[str] = set()
    for theme_key, label, kind in contributions:
        if theme_key in seen_keys:
            continue
        seen_keys.add(theme_key)

        existing = (
            db.query(OrgSalesContentTheme)
            .filter(
                OrgSalesContentTheme.org_id == org_id,
                OrgSalesContentTheme.theme_key == theme_key,
                OrgSalesContentTheme.source == "check_in",
            )
            .first()
        )
        if not existing:
            existing = OrgSalesContentTheme(
                id=uuid.uuid4(),
                org_id=org_id,
                theme_key=theme_key,
                source="check_in",
                kind=kind,
                label=label[:220] if label else None,
                occurrence_count=0,
                distinct_client_count=0,
                contributing_client_ids=[],
                sample_quotes=[],
                first_seen_at=now,
                last_seen_at=now,
            )
            db.add(existing)
            db.flush()

        existing.occurrence_count = int(existing.occurrence_count or 0) + 1
        existing.last_seen_at = now
        if label and (not existing.label or len(label) > len(existing.label or "")):
            existing.label = label[:220]

        ids: List[str] = list(existing.contributing_client_ids or [])
        if not isinstance(ids, list):
            ids = []
        ids = [str(x) for x in ids if x]
        if client_id_str not in ids and len(ids) < max_clients:
            ids.append(client_id_str)
        existing.contributing_client_ids = ids
        existing.distinct_client_count = len(set(ids))

        samples: List[str] = list(existing.sample_quotes or [])
        if not isinstance(samples, list):
            samples = []
        samples = [str(s) for s in samples if s]
        qsnippet = _quote_for_theme_key(ij, theme_key) or _avoid_phrase_for_key(ij, theme_key) or (label or "")[:400]
        if qsnippet and qsnippet not in samples and len(samples) < max_samples:
            samples.append(qsnippet)
        existing.sample_quotes = samples

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.warning("org_sales_content_themes commit failed: %s", e)


def record_from_completed_call_library_report(
    db: Session, row: CallLibraryReport, client_id: Optional[uuid.UUID]
) -> None:
    """Upsert win-weighted theme rows from a completed sales CallLibraryReport.

    Parallel to record_from_completed_insight but sourced from real prospect-facing sales
    calls (report_json.objection_handling_audit.objections[]) rather than post-sale check-ins —
    tagged source="sales_call" so the two pools stay distinguishable (see
    OrgSalesContentTheme.source). No client attribution is possible without a matched client,
    so rows with no client_id are skipped entirely (can't contribute to distinct_client_count).
    """
    if row.status != "complete" or not client_id or not isinstance(row.report_json, dict):
        return
    ensure_org_sales_content_themes_table(db)

    oha = row.report_json.get("objection_handling_audit")
    if not isinstance(oha, dict):
        return
    objections = oha.get("objections")
    if not isinstance(objections, list) or not objections:
        return

    org_id = row.org_id
    client_id_str = str(client_id)
    deal_closed = bool(getattr(row, "deal_closed", False))
    now = datetime.now(timezone.utc)
    max_clients = int(getattr(settings, "ORG_SALES_THEME_MAX_CONTRIBUTING_CLIENTS", 500) or 500)
    max_samples = int(getattr(settings, "ORG_SALES_THEME_MAX_SAMPLE_QUOTES", 8) or 8)

    seen_keys: Set[str] = set()
    for item in objections:
        if not isinstance(item, dict):
            continue
        parsed = theme_key_and_label_from_clip(
            {
                "quote": item.get("quote") or item.get("surface_quote"),
                "label": item.get("objection_label"),
                "rationale": item.get("summary"),
            }
        )
        if not parsed:
            continue
        theme_key, label = parsed
        if theme_key in seen_keys:
            continue
        seen_keys.add(theme_key)

        existing = (
            db.query(OrgSalesContentTheme)
            .filter(
                OrgSalesContentTheme.org_id == org_id,
                OrgSalesContentTheme.theme_key == theme_key,
                OrgSalesContentTheme.source == "sales_call",
            )
            .first()
        )
        if not existing:
            existing = OrgSalesContentTheme(
                id=uuid.uuid4(),
                org_id=org_id,
                theme_key=theme_key,
                source="sales_call",
                kind="objection",
                label=label[:220] if label else None,
                occurrence_count=0,
                distinct_client_count=0,
                closed_won_count=0,
                contributing_client_ids=[],
                sample_quotes=[],
                first_seen_at=now,
                last_seen_at=now,
            )
            db.add(existing)
            db.flush()

        existing.occurrence_count = int(existing.occurrence_count or 0) + 1
        existing.last_seen_at = now
        if deal_closed:
            existing.closed_won_count = int(existing.closed_won_count or 0) + 1
        if label and (not existing.label or len(label) > len(existing.label or "")):
            existing.label = label[:220]

        ids: List[str] = list(existing.contributing_client_ids or [])
        if not isinstance(ids, list):
            ids = []
        ids = [str(x) for x in ids if x]
        if client_id_str not in ids and len(ids) < max_clients:
            ids.append(client_id_str)
        existing.contributing_client_ids = ids
        existing.distinct_client_count = len(set(ids))

        # Prefer the quote from an occurrence the LLM marked handled_well — that's the
        # proven-to-convert framing, not just evidence the objection exists.
        samples: List[str] = list(existing.sample_quotes or [])
        if not isinstance(samples, list):
            samples = []
        samples = [str(s) for s in samples if s]
        qsnippet = str(item.get("quote") or item.get("surface_quote") or label or "")[:400]
        if qsnippet and bool(item.get("handled_well")):
            samples = [qsnippet] + [s for s in samples if s != qsnippet]
        elif qsnippet and qsnippet not in samples and len(samples) < max_samples:
            samples.append(qsnippet)
        existing.sample_quotes = samples[:max_samples]

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.warning("org_sales_content_themes (call_library) commit failed: %s", e)


def _is_validated_row(row: OrgSalesContentTheme, cutoff: datetime) -> bool:
    if row.last_seen_at < cutoff:
        return False
    min_c = int(getattr(settings, "ORG_SALES_THEME_MIN_DISTINCT_CLIENTS", 3) or 3)
    min_o = int(getattr(settings, "ORG_SALES_THEME_MIN_OCCURRENCES", 3) or 3)
    if row.distinct_client_count < min_c or row.occurrence_count < min_o:
        return False
    if getattr(row, "source", "check_in") == "sales_call":
        # An objection pattern that never appears on a won deal isn't "proven," just common.
        return int(getattr(row, "closed_won_count", 0) or 0) >= 1
    return True


def list_validated_theme_keys(
    db: Session, org_id: uuid.UUID, *, kind: Optional[str] = None
) -> List[str]:
    """Theme keys that meet org thresholds within lookback window (check_in pool only).

    `kind` optionally narrows to "objection" | "question" | "other"; default (None) returns
    all kinds, matching the pre-issue-6 behavior for existing callers.
    """
    ensure_org_sales_content_themes_table(db)
    days = int(getattr(settings, "ORG_SALES_THEME_LOOKBACK_DAYS", 120) or 120)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    filters = [
        OrgSalesContentTheme.org_id == org_id,
        OrgSalesContentTheme.source == "check_in",
        OrgSalesContentTheme.last_seen_at >= cutoff,
    ]
    if kind:
        filters.append(OrgSalesContentTheme.kind == kind)
    rows = db.query(OrgSalesContentTheme).filter(*filters).all()
    return [r.theme_key for r in rows if _is_validated_row(r, cutoff)]


def list_validated_themes_payload(
    db: Session, org_id: uuid.UUID, *, kind: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Themes with labels and samples for prompts / UI (check_in pool only).

    `kind` optionally narrows to "objection" | "question" | "other"; default (None) returns
    all kinds, matching the pre-issue-6 behavior for existing callers.
    """
    ensure_org_sales_content_themes_table(db)
    days = int(getattr(settings, "ORG_SALES_THEME_LOOKBACK_DAYS", 120) or 120)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    filters = [
        OrgSalesContentTheme.org_id == org_id,
        OrgSalesContentTheme.source == "check_in",
        OrgSalesContentTheme.last_seen_at >= cutoff,
    ]
    if kind:
        filters.append(OrgSalesContentTheme.kind == kind)
    rows = (
        db.query(OrgSalesContentTheme)
        .filter(*filters)
        .order_by(OrgSalesContentTheme.distinct_client_count.desc(), OrgSalesContentTheme.occurrence_count.desc())
        .limit(80)
        .all()
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not _is_validated_row(r, cutoff):
            continue
        out.append(
            {
                "theme_key": r.theme_key,
                "label": r.label or "",
                "distinct_client_count": r.distinct_client_count,
                "occurrence_count": r.occurrence_count,
                "sample_quotes": (r.sample_quotes or [])[:5],
            }
        )
    return out[:40]


def list_validated_win_themes_payload(db: Session, org_id: uuid.UUID) -> List[Dict[str, Any]]:
    """Win-weighted objection themes from real sales calls (source="sales_call") that hit a
    closed deal at least once — the "proven to convert, not just common" tier. See issue 3/4
    in docs/prd/content-generation-signal-upgrades/ISSUES.md."""
    ensure_org_sales_content_themes_table(db)
    days = int(getattr(settings, "ORG_SALES_THEME_LOOKBACK_DAYS", 120) or 120)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows = (
        db.query(OrgSalesContentTheme)
        .filter(
            OrgSalesContentTheme.org_id == org_id,
            OrgSalesContentTheme.source == "sales_call",
            OrgSalesContentTheme.last_seen_at >= cutoff,
        )
        .order_by(OrgSalesContentTheme.closed_won_count.desc(), OrgSalesContentTheme.occurrence_count.desc())
        .limit(80)
        .all()
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not _is_validated_row(r, cutoff):
            continue
        out.append(
            {
                "theme_key": r.theme_key,
                "label": r.label or "",
                "distinct_client_count": r.distinct_client_count,
                "occurrence_count": r.occurrence_count,
                "closed_won_count": r.closed_won_count,
                "sample_quotes": (r.sample_quotes or [])[:5],
            }
        )
    return out[:40]


def enrich_clips_org_validation(db: Session, org_id: uuid.UUID, clips: List[Dict[str, Any]]) -> None:
    """Mutate clips in place: theme_key, org_validated_pattern."""
    validated = set(list_validated_theme_keys(db, org_id))
    for c in clips:
        if not isinstance(c, dict):
            continue
        parsed = theme_key_and_label_from_clip(c)
        if not parsed:
            c["theme_key"] = None
            c["org_validated_pattern"] = False
            continue
        key, _ = parsed
        c["theme_key"] = key
        kind = str(c.get("kind") or "").lower()
        c["org_validated_pattern"] = kind == "objection" and key in validated
