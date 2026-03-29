"""Persist and query org-level recurring objection / circumstance themes from call insights."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
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
                label VARCHAR(220),
                occurrence_count INTEGER NOT NULL DEFAULT 0,
                distinct_client_count INTEGER NOT NULL DEFAULT 0,
                contributing_client_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                sample_quotes JSONB NOT NULL DEFAULT '[]'::jsonb,
                first_seen_at TIMESTAMPTZ NOT NULL,
                last_seen_at TIMESTAMPTZ NOT NULL,
                CONSTRAINT uq_org_sales_content_theme_org_key UNIQUE (org_id, theme_key)
            )
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

    contributions: List[tuple[str, str]] = []

    for c in ij.get("clips") or []:
        if not isinstance(c, dict):
            continue
        kind = str(c.get("kind") or "").lower()
        if kind not in ("objection", "other"):
            continue
        parsed = theme_key_and_label_from_clip(c)
        if parsed:
            contributions.append(parsed)

    pv = ij.get("prospect_voice")
    if isinstance(pv, dict):
        for ap in pv.get("avoid_phrasing") or []:
            parsed = theme_key_and_label_from_phrase(str(ap))
            if parsed:
                contributions.append(parsed)

    if not contributions:
        return

    now = datetime.now(timezone.utc)
    max_clients = int(getattr(settings, "ORG_SALES_THEME_MAX_CONTRIBUTING_CLIENTS", 500) or 500)
    max_samples = int(getattr(settings, "ORG_SALES_THEME_MAX_SAMPLE_QUOTES", 8) or 8)

    seen_keys: Set[str] = set()
    for theme_key, label in contributions:
        if theme_key in seen_keys:
            continue
        seen_keys.add(theme_key)

        existing = (
            db.query(OrgSalesContentTheme)
            .filter(OrgSalesContentTheme.org_id == org_id, OrgSalesContentTheme.theme_key == theme_key)
            .first()
        )
        if not existing:
            existing = OrgSalesContentTheme(
                id=uuid.uuid4(),
                org_id=org_id,
                theme_key=theme_key,
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


def _is_validated_row(row: OrgSalesContentTheme, cutoff: datetime) -> bool:
    if row.last_seen_at < cutoff:
        return False
    min_c = int(getattr(settings, "ORG_SALES_THEME_MIN_DISTINCT_CLIENTS", 3) or 3)
    min_o = int(getattr(settings, "ORG_SALES_THEME_MIN_OCCURRENCES", 3) or 3)
    return row.distinct_client_count >= min_c and row.occurrence_count >= min_o


def list_validated_theme_keys(db: Session, org_id: uuid.UUID) -> List[str]:
    """Theme keys that meet org thresholds within lookback window."""
    ensure_org_sales_content_themes_table(db)
    days = int(getattr(settings, "ORG_SALES_THEME_LOOKBACK_DAYS", 120) or 120)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows = (
        db.query(OrgSalesContentTheme)
        .filter(OrgSalesContentTheme.org_id == org_id, OrgSalesContentTheme.last_seen_at >= cutoff)
        .all()
    )
    return [r.theme_key for r in rows if _is_validated_row(r, cutoff)]


def list_validated_themes_payload(db: Session, org_id: uuid.UUID) -> List[Dict[str, Any]]:
    """Themes with labels and samples for prompts / UI."""
    ensure_org_sales_content_themes_table(db)
    days = int(getattr(settings, "ORG_SALES_THEME_LOOKBACK_DAYS", 120) or 120)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows = (
        db.query(OrgSalesContentTheme)
        .filter(OrgSalesContentTheme.org_id == org_id, OrgSalesContentTheme.last_seen_at >= cutoff)
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
