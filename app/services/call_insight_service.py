"""Orchestrate call insight generation, persistence, and rollup summary."""
from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
import threading
import time
import uuid
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import desc, nullslast
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.client import Client
from app.models.client_call_insight import ClientCallInsight, ClientInsightSummary
from app.models.client_checkin import ClientCheckIn
from app.models.fathom_call_record import FathomCallRecord
from sqlalchemy.orm.attributes import flag_modified

from app.services.call_insight_ai import compute_call_insight_json, headline_from_insight, validate_and_normalize_insight_json
from app.services.offer_ladder import resolve_org_offer_ladder
from app.services.roi_signal_validation import (
    apply_roi_validation,
    client_has_expansion_win_basis,
    merge_client_roi_meta,
    normalize_display_tags_for_client,
    upsell_referral_testimonial_gate_bypass,
)
from app.services.user_ai_profile_context import resolve_org_sales_lens
from app.services.call_insight_context import assemble_context_pack, is_thin_transcript
from app.services.call_insight_correlation import link_fathom_to_checkin
from app.services.health_score_cache_service import resolve_health_score

logger = logging.getLogger(__name__)

_org_hour_lock = threading.Lock()
_org_hour_counts: Dict[str, int] = {}


def _org_bucket_key(org_id: uuid.UUID) -> str:
    hour = int(time.time() // 3600)
    return f"{org_id}:{hour}"


def check_org_insight_throttle(org_id: uuid.UUID) -> bool:
    """Return True if allowed, False if over hourly cap."""
    max_h = int(getattr(settings, "CALL_INSIGHT_ORG_MAX_PER_HOUR", 40) or 40)
    key = _org_bucket_key(org_id)
    with _org_hour_lock:
        c = _org_hour_counts.get(key, 0)
        if c >= max_h:
            return False
        _org_hour_counts[key] = c + 1
    # prune old keys occasionally
    if len(_org_hour_counts) > 5000:
        cutoff = int(time.time() // 3600) - 2
        stale = [k for k in _org_hour_counts if int(k.split(":")[-1]) < cutoff]
        for k in stale[:1000]:
            _org_hour_counts.pop(k, None)
    return True


def hash_context_pack(pack: Dict[str, Any]) -> str:
    canonical = json.dumps(pack, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _lifecycle_str(client: Client) -> str:
    ls = client.lifecycle_state
    if hasattr(ls, "value"):
        return str(ls.value)
    return str(ls)


def parse_lead_follow_up_due_iso(s: str) -> Optional[datetime]:
    """Parse LLM due_date_iso to UTC datetime for client.meta.follow_up_due_at."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        if "T" not in s and len(s) >= 10:
            d = date.fromisoformat(s[:10])
            return datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def apply_lead_follow_up_from_insight(client: Client, insight_json: Dict[str, Any]) -> None:
    """
    Persist follow-up due date from call-insight lead_follow_up (LLM structured field).

    Only cold_lead / warm_lead: when confirmed_on_call and due_date_iso parse, set meta.follow_up_due_at;
    otherwise clear so the UI falls back to a 14-day window from last activity.
    """
    if _lifecycle_str(client) not in ("cold_lead", "warm_lead"):
        return
    lf = insight_json.get("lead_follow_up")
    if not isinstance(lf, dict):
        return
    meta: Dict[str, Any] = dict(client.meta) if isinstance(client.meta, dict) else {}
    if bool(lf.get("confirmed_on_call")) and lf.get("due_date_iso"):
        parsed = parse_lead_follow_up_due_iso(str(lf.get("due_date_iso")))
        if parsed:
            meta["follow_up_due_at"] = parsed.isoformat().replace("+00:00", "Z")
        else:
            meta.pop("follow_up_due_at", None)
    else:
        meta.pop("follow_up_due_at", None)
    client.meta = meta
    flag_modified(client, "meta")


def _lead_pipeline_snapshot(db: Session, org_id: uuid.UUID, client_id: uuid.UUID) -> Dict[str, Any]:
    """
    Calendar + sales pipeline facts for call-insight LLM and ROI gating (cold/warm leads).
    """
    now = datetime.now(timezone.utc)
    last_sales = (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.client_id == client_id,
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.is_sales_call.is_(True),
            ClientCheckIn.start_time < now,
            ClientCheckIn.cancelled.is_(False),
        )
        .order_by(desc(ClientCheckIn.start_time))
        .first()
    )
    has_past_sales_call = last_sales is not None
    last_sales_dict: Optional[Dict[str, Any]] = None
    open_sales_deal = False
    if last_sales:
        sc = getattr(last_sales, "sale_closed", None)
        open_sales_deal = sc is not True
        last_sales_dict = {
            "start_time": last_sales.start_time.isoformat() if last_sales.start_time else None,
            "sale_closed": sc,
            "event_id": str(last_sales.event_id) if last_sales.event_id else None,
        }

    next_ci = (
        db.query(ClientCheckIn)
        .filter(
            ClientCheckIn.client_id == client_id,
            ClientCheckIn.org_id == org_id,
            ClientCheckIn.cancelled.is_(False),
            ClientCheckIn.start_time > now,
        )
        .order_by(ClientCheckIn.start_time.asc())
        .first()
    )
    has_upcoming = next_ci is not None
    next_iso = next_ci.start_time.isoformat() if next_ci and next_ci.start_time else None
    next_is_sales = bool(getattr(next_ci, "is_sales_call", False)) if next_ci else False

    return {
        "has_past_sales_call": has_past_sales_call,
        "last_sales_call": last_sales_dict,
        "open_sales_deal": bool(open_sales_deal),
        "has_upcoming_check_in": has_upcoming,
        "next_start_time_iso": next_iso,
        "next_is_sales_call": next_is_sales,
    }


def _opportunity_tags_from_insight_row(row: ClientCallInsight) -> List[str]:
    ij = row.insight_json
    if not isinstance(ij, dict):
        return []
    raw = ij.get("opportunity_tags") or []
    return [str(t).lower().strip() for t in raw if str(t).strip()]


def _fallback_opportunity_tags_from_insights(
    db: Session, org_id: uuid.UUID, client_id: uuid.UUID, *, lookback: int = 16
) -> List[str]:
    """Most recent complete insight rows with non-empty server-validated opportunity_tags."""
    rows = (
        db.query(ClientCallInsight)
        .filter(
            ClientCallInsight.org_id == org_id,
            ClientCallInsight.client_id == client_id,
            ClientCallInsight.status == "complete",
        )
        .order_by(desc(ClientCallInsight.computed_at))
        .limit(lookback)
        .all()
    )
    for r in rows:
        tags = _opportunity_tags_from_insight_row(r)
        if tags:
            return tags
    return []


def _batch_fallback_opportunity_tags(
    db: Session, org_id: uuid.UUID, client_ids: List[uuid.UUID]
) -> Dict[uuid.UUID, List[str]]:
    """First non-empty opportunity_tags per client from recent complete insights (global time order)."""
    if not client_ids:
        return {}
    cap = min(4000, max(400, len(client_ids) * 48))
    rows = (
        db.query(ClientCallInsight)
        .filter(
            ClientCallInsight.org_id == org_id,
            ClientCallInsight.client_id.in_(client_ids),
            ClientCallInsight.status == "complete",
        )
        .order_by(desc(ClientCallInsight.computed_at))
        .limit(cap)
        .all()
    )
    by_cid: Dict[uuid.UUID, List[ClientCallInsight]] = defaultdict(list)
    for r in rows:
        lst = by_cid[r.client_id]
        if len(lst) < 20:
            lst.append(r)
    out: Dict[uuid.UUID, List[str]] = {}
    for cid in client_ids:
        for r in by_cid.get(cid, []):
            tags = _opportunity_tags_from_insight_row(r)
            if tags:
                out[cid] = tags
                break
    return out


def run_call_insight_for_fathom_record(
    db: Session,
    org_id: uuid.UUID,
    fathom_call_record_id: uuid.UUID,
    *,
    bypass_cooldown: bool = False,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Returns (status, detail) where status is ok|skipped|failed.
    """
    rec = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.id == fathom_call_record_id,
            FathomCallRecord.org_id == org_id,
        )
        .first()
    )
    if not rec or not rec.client_id:
        logger.info(
            "call_insight skipped no_record_or_client org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "skipped", {"reason": "no_record_or_client"}

    if rec.sentiment_status != "complete":
        logger.info(
            "call_insight skipped sentiment_not_complete org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "skipped", {"reason": "sentiment_not_complete"}

    client = db.query(Client).filter(Client.id == rec.client_id, Client.org_id == org_id).first()
    if not client:
        logger.info(
            "call_insight skipped client_not_found org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "skipped", {"reason": "client_not_found"}

    existing = (
        db.query(ClientCallInsight)
        .filter(ClientCallInsight.fathom_call_record_id == fathom_call_record_id)
        .first()
    )
    if existing and existing.status == "complete" and existing.insight_json:
        return "skipped", {"reason": "already_computed"}

    check_in_id = link_fathom_to_checkin(db, org_id, rec.client_id, rec.meeting_at)

    health = resolve_health_score(
        db, rec.client_id, org_id, brevo_email_stats=None, use_ai=False, persist_cache=False
    )
    if not health:
        return "skipped", {"reason": "no_health"}

    pack = assemble_context_pack(db, client, rec, check_in_id, health)
    pack["pipeline"] = _lead_pipeline_snapshot(db, org_id, rec.client_id)
    input_hash = hash_context_pack(pack)

    if existing and existing.input_hash == input_hash and existing.status == "complete":
        logger.debug("call_insight skipped same_hash record=%s", fathom_call_record_id)
        return "skipped", {"reason": "same_hash"}

    if is_thin_transcript(pack):
        row = existing or ClientCallInsight(
            id=uuid.uuid4(),
            org_id=org_id,
            client_id=rec.client_id,
            fathom_call_record_id=fathom_call_record_id,
            check_in_id=check_in_id,
        )
        row.status = "skipped"
        row.failure_reason = "thin_transcript"
        row.computed_at = datetime.now(timezone.utc)
        row.input_hash = input_hash
        row.lifecycle_at_compute = _lifecycle_str(client)
        row.check_in_id = check_in_id
        if not existing:
            db.add(row)
        db.commit()
        logger.info(
            "call_insight skipped thin_transcript org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "skipped", {"reason": "thin_transcript"}

    # Per-org hourly cap (before LLM). POST refresh may bypass (bypass_cooldown=True).
    if not bypass_cooldown and not check_org_insight_throttle(org_id):
        logger.warning(
            "call_insight org throttle skip org_id=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        row = existing or ClientCallInsight(
            id=uuid.uuid4(),
            org_id=org_id,
            client_id=rec.client_id,
            fathom_call_record_id=fathom_call_record_id,
            check_in_id=check_in_id,
        )
        row.status = "skipped"
        row.failure_reason = "org_throttle"
        row.computed_at = datetime.now(timezone.utc)
        row.input_hash = input_hash
        row.lifecycle_at_compute = _lifecycle_str(client)
        row.check_in_id = check_in_id
        row.insight_json = None
        if not existing:
            db.add(row)
        db.commit()
        return "skipped", {"reason": "org_throttle"}

    org_offer_ladder = resolve_org_offer_ladder(db, org_id)
    org_sales_lens = resolve_org_sales_lens(db, org_id)
    insight_json = compute_call_insight_json(
        context_pack=pack,
        lifecycle=_lifecycle_str(client),
        org_id=org_id,
        offer_ladder=org_offer_ladder,
        sales_lens=org_sales_lens,
    )
    if not insight_json:
        row = existing or ClientCallInsight(
            id=uuid.uuid4(),
            org_id=org_id,
            client_id=rec.client_id,
            fathom_call_record_id=fathom_call_record_id,
            check_in_id=check_in_id,
        )
        row.status = "failed"
        row.failure_reason = "llm_unavailable_or_failed"
        row.computed_at = datetime.now(timezone.utc)
        row.input_hash = input_hash
        row.lifecycle_at_compute = _lifecycle_str(client)
        row.model = getattr(settings, "HEALTH_SCORE_LLM_MODEL", None)
        if not existing:
            db.add(row)
        db.commit()
        logger.warning(
            "call_insight failed llm org=%s record=%s",
            org_id,
            fathom_call_record_id,
        )
        return "failed", {"reason": "llm"}

    insight_json = validate_and_normalize_insight_json(insight_json)

    trans_text = str((pack.get("call_text") or {}).get("transcript") or "")
    prior_roi: Dict[str, Any] = {}
    if isinstance(client.meta, dict):
        pr = client.meta.get("roi_state")
        if isinstance(pr, dict):
            prior_roi = pr
    meeting_iso = rec.meeting_at.isoformat() if rec.meeting_at else None
    pipeline = pack.get("pipeline") if isinstance(pack.get("pipeline"), dict) else {}
    gate_bypass = upsell_referral_testimonial_gate_bypass(client)
    insight_json, roi_delta = apply_roi_validation(
        insight_json,
        trans_text,
        _lifecycle_str(client),
        prior_roi,
        meeting_iso,
        pipeline,
        testimonial_gate_bypass=gate_bypass,
    )
    merge_client_roi_meta(client, roi_delta)
    try:
        flag_modified(client, "meta")
    except Exception:
        pass

    row = existing or ClientCallInsight(
        id=uuid.uuid4(),
        org_id=org_id,
        client_id=rec.client_id,
        fathom_call_record_id=fathom_call_record_id,
        check_in_id=check_in_id,
    )
    row.insight_json = insight_json
    row.status = "complete"
    row.failure_reason = None
    row.computed_at = datetime.now(timezone.utc)
    row.input_hash = input_hash
    row.lifecycle_at_compute = _lifecycle_str(client)
    row.model = getattr(settings, "HEALTH_SCORE_LLM_MODEL", None)
    if not existing:
        db.add(row)

    _upsert_summary(db, org_id, client, rec, insight_json, health)
    db.commit()
    logger.info(
        "call_insight complete org=%s record=%s insight_id=%s",
        org_id,
        fathom_call_record_id,
        row.id,
    )
    try:
        from app.services.org_sales_theme_service import record_from_completed_insight

        record_from_completed_insight(db, row)
    except Exception as e:
        logger.warning("org_sales_theme record_from_completed_insight skipped: %s", e)

    try:
        from app.services.client_ai_recommendations_service import (
            merge_call_insight_actions_for_fathom_record,
            merge_prospect_voice_from_insight_into_client,
        )

        c2 = db.query(Client).filter(Client.id == client.id).first()
        if c2:
            merge_call_insight_actions_for_fathom_record(
                db,
                c2,
                fathom_call_record_id,
                insight_json.get("next_steps") or [],
            )
            merge_prospect_voice_from_insight_into_client(c2, insight_json.get("prospect_voice"))
            apply_lead_follow_up_from_insight(c2, insight_json)
            db.commit()
    except Exception as e:
        logger.exception("post_call_insight_merge failed client=%s: %s", client.id, e)
        db.rollback()

    return "ok", {"insight_id": str(row.id)}


def _upsert_summary(
    db: Session,
    org_id: uuid.UUID,
    client: Client,
    fathom_rec: FathomCallRecord,
    insight: Dict[str, Any],
    health: Dict[str, Any],
) -> None:
    summ = db.query(ClientInsightSummary).filter(ClientInsightSummary.client_id == client.id).first()
    if not summ:
        summ = ClientInsightSummary(client_id=client.id, org_id=org_id, tags=[])
        db.add(summ)

    low = bool(insight.get("low_signal"))
    tags = [] if low else list(dict.fromkeys(insight.get("opportunity_tags") or []))
    summ.headline = headline_from_insight(insight)
    summ.tags = tags[:12]
    summ.last_call_at = fathom_rec.meeting_at or fathom_rec.updated_at
    summ.last_insight_at = datetime.now(timezone.utc)
    summ.last_lifecycle_state = _lifecycle_str(client)
    summ.last_health_grade = str(health.get("grade") or "")[:8]
    try:
        summ.last_health_score = float(health.get("score")) if health.get("score") is not None else None
    except (TypeError, ValueError):
        summ.last_health_score = None


def run_call_insight_background(org_id_str: str, fathom_record_id_str: str) -> None:
    """New DB session for thread/BackgroundTasks."""
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        oid = uuid.UUID(org_id_str)
        rid = uuid.UUID(fathom_record_id_str)
        status, detail = run_call_insight_for_fathom_record(db, oid, rid, bypass_cooldown=False)
        logger.info(
            "call_insight background done record=%s status=%s detail=%s",
            fathom_record_id_str,
            status,
            detail,
        )
    except Exception as e:
        logger.exception("call_insight background failed: %s", e)
    finally:
        db.close()


def refresh_latest_call_insight_background(org_id_str: str, client_id_str: str) -> None:
    """New DB session for thread/RQ after check-in sync."""
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        oid = uuid.UUID(org_id_str)
        cid = uuid.UUID(str(client_id_str))
        status, detail = refresh_latest_call_insight(db, oid, cid)
        logger.info(
            "refresh_latest_call_insight background done client=%s status=%s detail=%s",
            client_id_str,
            status,
            detail,
        )
    except Exception as e:
        logger.exception("refresh_latest_call_insight background failed: %s", e)
    finally:
        db.close()


def refresh_latest_call_insight(
    db: Session, org_id: uuid.UUID, client_id: uuid.UUID
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Re-run LLM for the latest Fathom recording (manual refresh)."""
    rec = (
        db.query(FathomCallRecord)
        .filter(
            FathomCallRecord.org_id == org_id,
            FathomCallRecord.client_id == client_id,
            FathomCallRecord.sentiment_status == "complete",
        )
        .order_by(nullslast(desc(FathomCallRecord.meeting_at)), desc(FathomCallRecord.created_at))
        .first()
    )
    if not rec:
        return "skipped", {"reason": "no_fathom_recording"}
    db.query(ClientCallInsight).filter(ClientCallInsight.fathom_call_record_id == rec.id).delete()
    db.commit()
    return run_call_insight_for_fathom_record(db, org_id, rec.id, bypass_cooldown=True)


def _build_call_insights_rollup(db: Session, client: Client, rows: List[ClientCallInsight]) -> Dict[str, Any]:
    """Aggregate priorities, suggestions, clips, wins across completed insights (no per-call drill-down in UI)."""
    open_priorities: List[str] = []
    open_suggestions: List[Dict[str, str]] = []
    clips: List[Dict[str, Any]] = []
    wins: List[str] = []
    stories: List[str] = []
    roi_testimonials: List[Dict[str, Any]] = []
    seen_roi_q: set = set()
    latest_upsell: Optional[Dict[str, Any]] = None
    latest_referral: Optional[Dict[str, Any]] = None
    latest_revive_playbook: Optional[Dict[str, Any]] = None
    seen_p: set = set()
    seen_s: set = set()
    # Single narrative paragraph: always from the most recent complete insight (rows are newest-first).
    latest_synthesis = ""
    # Latest sales-framework critique (only set when the operator has a sales lens
    # configured AND the most recent call had sales-relevant content).
    latest_framework_review: Dict[str, Any] = {}

    for r in rows:
        if r.status != "complete" or not r.insight_json or not isinstance(r.insight_json, dict):
            continue
        ij = r.insight_json
        if not latest_synthesis:
            s = str(ij.get("client_state_synthesis") or "").strip()
            if s:
                latest_synthesis = s[:2500]
        if not latest_framework_review:
            fr = str(ij.get("framework_review") or "").strip()
            if fr:
                fj_for_fr = r.fathom_call_record
                latest_framework_review = {
                    "summary": fr[:1200],
                    "meeting_at": fj_for_fr.meeting_at.isoformat() if fj_for_fr and fj_for_fr.meeting_at else "",
                }
        fj = r.fathom_call_record
        mat = fj.meeting_at.isoformat() if fj and fj.meeting_at else None
        for p in ij.get("priorities") or []:
            ps = str(p).strip()[:500]
            if ps and ps not in seen_p:
                seen_p.add(ps)
                open_priorities.append(ps)
        for ns in ij.get("next_steps") or []:
            if not isinstance(ns, dict):
                continue
            t = str(ns.get("title") or "").strip()
            d = str(ns.get("detail") or "").strip()
            sig = (t[:120], d[:120])
            if sig in seen_s:
                continue
            seen_s.add(sig)
            open_suggestions.append(
                {
                    "title": t[:300],
                    "detail": d[:1200],
                    "meeting_at": mat or "",
                }
            )
        for c in (ij.get("clips") or [])[:10]:
            if isinstance(c, dict):
                cc = dict(c)
                cc["meeting_at"] = mat
                clips.append(cc)
        for w in ij.get("wins") or []:
            ws = str(w).strip()
            if ws and ws not in wins:
                wins.append(ws[:400])
        for st in ij.get("testimonial_stories") or []:
            ss = str(st).strip()
            if ss and ss not in stories:
                stories.append(ss[:600])

        rs = ij.get("roi_signals")
        if isinstance(rs, dict):
            for m in (rs.get("testimonial_moments") or [])[:5]:
                if not isinstance(m, dict):
                    continue
                qk = str(m.get("quote") or "").strip()[:200]
                if qk and qk not in seen_roi_q:
                    seen_roi_q.add(qk)
                    mm = dict(m)
                    mm["meeting_at"] = mat
                    mm["fathom_call_record_id"] = str(r.fathom_call_record_id) if r.fathom_call_record_id else None
                    roi_testimonials.append(mm)
            up = rs.get("upsell")
            if isinstance(up, dict) and up.get("active") and not latest_upsell:
                latest_upsell = {
                    "rationale": str(up.get("rationale") or "")[:800],
                    "meeting_at": mat or "",
                }
            ref = rs.get("referral")
            if isinstance(ref, dict) and ref.get("active") and not latest_referral:
                latest_referral = {
                    "variant": ref.get("variant"),
                    "rationale": str(ref.get("rationale") or "")[:800],
                    "meeting_at": mat or "",
                }
            rp = rs.get("revive_playbook")
            if isinstance(rp, dict) and str(rp.get("rationale") or "").strip() and not latest_revive_playbook:
                latest_revive_playbook = {
                    "rationale": str(rp.get("rationale") or "")[:1200],
                    "offer_angles": [str(x)[:300] for x in (rp.get("offer_angles") or [])[:8] if str(x).strip()],
                    "outreach_hooks": [str(x)[:300] for x in (rp.get("outreach_hooks") or [])[:8] if str(x).strip()],
                    "meeting_at": mat or "",
                }

    prof: Dict[str, Any] = {}
    if isinstance(client.meta, dict):
        raw = client.meta.get("prospect_voice_profile")
        if isinstance(raw, dict):
            prof = raw

    out = {
        "client_state_synthesis": latest_synthesis,
        "accumulated_priorities": open_priorities[:25],
        "accumulated_call_suggestions": open_suggestions[:40],
        "accumulated_clips": clips[:35],
        "accumulated_wins": wins[:20],
        "accumulated_testimonial_stories": stories[:15],
        "accumulated_roi_testimonials": roi_testimonials[:12],
        "latest_upsell_signal": latest_upsell,
        "latest_referral_signal": latest_referral,
        "latest_revive_playbook": latest_revive_playbook,
        "latest_framework_review": latest_framework_review or None,
        "prospect_voice_profile": prof,
        "org_validated_theme_keys": [],
    }
    try:
        from app.services.org_sales_theme_service import enrich_clips_org_validation, list_validated_theme_keys

        oid = client.org_id
        enrich_clips_org_validation(db, oid, out["accumulated_clips"])
        out["org_validated_theme_keys"] = list_validated_theme_keys(db, oid)
    except Exception:
        pass
    return out


def get_client_insights_response(db: Session, org_id: uuid.UUID, client_id: uuid.UUID, limit: int = 25) -> Dict[str, Any]:
    """Shape for GET /clients/{id}/call-insights."""
    client = db.query(Client).filter(Client.id == client_id, Client.org_id == org_id).first()
    if not client:
        return {}

    pipeline = _lead_pipeline_snapshot(db, org_id, client_id)

    summ = db.query(ClientInsightSummary).filter(ClientInsightSummary.client_id == client_id).first()
    summary_out = None
    if summ:
        raw_tags: List[str] = []
        if summ.tags:
            raw_tags = [str(t).lower().strip() for t in summ.tags if str(t).strip()]
        fb_tags = _fallback_opportunity_tags_from_insights(db, org_id, client_id)
        ls_lc = _lifecycle_str(client).lower().strip()
        if ls_lc in ("active", "offboarding") and not raw_tags and fb_tags:
            raw_tags = list(fb_tags)
        tags = normalize_display_tags_for_client(
            _lifecycle_str(client),
            pipeline,
            raw_tags,
            testimonial_gate_bypass=upsell_referral_testimonial_gate_bypass(client),
            has_expansion_win_basis=client_has_expansion_win_basis(client),
        )
        if ls_lc in ("active", "offboarding") and not tags and fb_tags:
            tags = normalize_display_tags_for_client(
                _lifecycle_str(client),
                pipeline,
                list(fb_tags),
                testimonial_gate_bypass=upsell_referral_testimonial_gate_bypass(client),
                has_expansion_win_basis=client_has_expansion_win_basis(client),
            )
        summary_out = {
            "headline": summ.headline,
            "tags": tags,
            "last_call_at": summ.last_call_at.isoformat() if summ.last_call_at else None,
            "last_insight_at": summ.last_insight_at.isoformat() if summ.last_insight_at else None,
        }

    rows = (
        db.query(ClientCallInsight)
        .filter(ClientCallInsight.client_id == client_id, ClientCallInsight.org_id == org_id)
        .order_by(desc(ClientCallInsight.computed_at))
        .limit(limit)
        .all()
    )

    insights = []
    for r in rows:
        fj = r.fathom_call_record
        insights.append(
            {
                "id": str(r.id),
                "fathom_call_record_id": str(r.fathom_call_record_id),
                "fathom_recording_id": int(fj.fathom_recording_id) if fj else None,
                "meeting_at": fj.meeting_at.isoformat() if fj and fj.meeting_at else None,
                "status": r.status,
                "computed_at": r.computed_at.isoformat() if r.computed_at else None,
                "insight": r.insight_json,
                "failure_reason": r.failure_reason,
            }
        )

    rollup = _build_call_insights_rollup(db, client, rows)

    roi_state_out: Optional[Dict[str, Any]] = None
    if isinstance(client.meta, dict):
        rs = client.meta.get("roi_state")
        if isinstance(rs, dict):
            roi_state_out = dict(rs)

    offer_suggestion: Optional[Dict[str, Any]] = None
    try:
        from app.services.offer_ladder import match_offer_for_client, resolve_org_offer_ladder

        ladder = resolve_org_offer_ladder(db, org_id)
        if ladder:
            tags_for_match: List[str] = []
            if summary_out and isinstance(summary_out.get("tags"), list):
                tags_for_match = [str(t) for t in summary_out["tags"]]
            prospect_voice = None
            if isinstance(client.meta, dict):
                pv = client.meta.get("prospect_voice_profile")
                if isinstance(pv, dict):
                    prospect_voice = pv
            offer_suggestion = match_offer_for_client(
                ladder,
                lifecycle=_lifecycle_str(client),
                roi_tags=tags_for_match,
                headline=str((summary_out or {}).get("headline") or ""),
                prospect_voice=prospect_voice,
                has_testimonial_trigger=client_has_expansion_win_basis(client),
            )
    except Exception:
        offer_suggestion = None

    return {
        "client_id": str(client_id),
        "summary": summary_out,
        "insights": insights,
        "rollup": rollup,
        "roi_state": roi_state_out,
        "pipeline": pipeline,
        "offer_suggestion": offer_suggestion,
    }


def get_call_insight_tags_batch(db: Session, org_id: uuid.UUID, client_ids: List[uuid.UUID]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not client_ids:
        return out
    rows = (
        db.query(ClientInsightSummary)
        .filter(
            ClientInsightSummary.org_id == org_id,
            ClientInsightSummary.client_id.in_(client_ids),
        )
        .all()
    )
    summ_by_cid = {r.client_id: r for r in rows}
    clients = (
        db.query(Client)
        .filter(Client.org_id == org_id, Client.id.in_(client_ids))
        .all()
    )
    client_by_id = {c.id: c for c in clients}
    fallback_map = _batch_fallback_opportunity_tags(db, org_id, client_ids)
    for cid in client_ids:
        c = client_by_id.get(cid)
        if not c:
            continue
        summ = summ_by_cid.get(cid)
        stored_tags: List[str] = []
        if summ and summ.tags:
            stored_tags = [str(t).lower().strip() for t in summ.tags if str(t).strip()]
        ls_lc = _lifecycle_str(c).lower().strip()
        fb_tags = fallback_map.get(cid) or []
        if ls_lc in ("active", "offboarding") and not stored_tags and fb_tags:
            stored_tags = list(fb_tags)
        pipe = _lead_pipeline_snapshot(db, org_id, cid)
        tags = normalize_display_tags_for_client(
            _lifecycle_str(c),
            pipe,
            stored_tags,
            testimonial_gate_bypass=upsell_referral_testimonial_gate_bypass(c),
            has_expansion_win_basis=client_has_expansion_win_basis(c),
        )
        if ls_lc in ("active", "offboarding") and not tags and fb_tags:
            tags = normalize_display_tags_for_client(
                _lifecycle_str(c),
                pipe,
                list(fb_tags),
                testimonial_gate_bypass=upsell_referral_testimonial_gate_bypass(c),
                has_expansion_win_basis=client_has_expansion_win_basis(c),
            )
        headline = (summ.headline or "")[:120] if summ else ""
        out[str(cid)] = {"tags": tags, "headline": headline}
    return out


def _merge_recommendation_actions(keep_actions: Any, remove_actions: Any) -> List[Dict[str, Any]]:
    """Union AI checklist actions by id; if either row marked completed, keep completed."""
    ka = keep_actions if isinstance(keep_actions, list) else []
    ra = remove_actions if isinstance(remove_actions, list) else []
    by_id: Dict[str, Dict[str, Any]] = {}
    for a in ka:
        if isinstance(a, dict) and a.get("id") is not None:
            by_id[str(a["id"])] = dict(a)
    for a in ra:
        if not isinstance(a, dict) or a.get("id") is None:
            continue
        k = str(a["id"])
        if k not in by_id:
            by_id[k] = dict(a)
        else:
            ex = by_id[k]
            if a.get("completed") and not ex.get("completed"):
                ex["completed"] = True
                ex["completed_at"] = a.get("completed_at") or ex.get("completed_at")
    return list(by_id.values())


def reconcile_call_insights_for_client_merge(
    db: Session,
    org_id: uuid.UUID,
    keep_id: uuid.UUID,
    remove_ids: List[uuid.UUID],
) -> None:
    """
    Move per-call insights and merge 1:1 tables before merged client rows are deleted.
    Avoids PK collisions on client_ai_recommendation_states / client_insight_summaries / health cache
    and prevents CASCADE from dropping ClientCallInsight rows for the merged-away profile.
    """
    if not remove_ids:
        return
    from app.models.client_ai_recommendation_state import ClientAIRecommendationState
    from app.models.client_health_score_cache import ClientHealthScoreCache

    db.query(ClientCallInsight).filter(
        ClientCallInsight.org_id == org_id,
        ClientCallInsight.client_id.in_(remove_ids),
    ).update({ClientCallInsight.client_id: keep_id}, synchronize_session=False)

    keep_summ = db.query(ClientInsightSummary).filter(ClientInsightSummary.client_id == keep_id).first()
    for rid in remove_ids:
        r_summ = db.query(ClientInsightSummary).filter(ClientInsightSummary.client_id == rid).first()
        if not r_summ:
            continue
        if keep_summ is None:
            r_summ.client_id = keep_id
            keep_summ = r_summ
        else:
            kt = list(keep_summ.tags) if isinstance(keep_summ.tags, list) else []
            rt = list(r_summ.tags) if isinstance(r_summ.tags, list) else []
            tag_union: List[str] = []
            seen_l = set()
            for t in kt + rt:
                s = str(t).strip()
                if not s:
                    continue
                sl = s.lower()
                if sl in seen_l:
                    continue
                seen_l.add(sl)
                tag_union.append(s)
            keep_summ.tags = tag_union[:12]
            kh, rh = (keep_summ.headline or "").strip(), (r_summ.headline or "").strip()
            if not kh and rh:
                keep_summ.headline = r_summ.headline
            elif rh and len(rh) > len(kh):
                keep_summ.headline = r_summ.headline
            for attr in ("last_call_at", "last_insight_at"):
                a, b = getattr(keep_summ, attr), getattr(r_summ, attr)
                if b is not None and (a is None or b > a):
                    setattr(keep_summ, attr, b)
            if not (keep_summ.last_lifecycle_state or "").strip() and (r_summ.last_lifecycle_state or "").strip():
                keep_summ.last_lifecycle_state = r_summ.last_lifecycle_state
            if not (keep_summ.last_health_grade or "").strip() and (r_summ.last_health_grade or "").strip():
                keep_summ.last_health_grade = r_summ.last_health_grade
            if keep_summ.last_health_score is None and r_summ.last_health_score is not None:
                keep_summ.last_health_score = r_summ.last_health_score
            db.delete(r_summ)
    db.flush()

    keep_rec = db.query(ClientAIRecommendationState).filter(
        ClientAIRecommendationState.org_id == org_id,
        ClientAIRecommendationState.client_id == keep_id,
    ).first()
    for rid in remove_ids:
        r_rec = db.query(ClientAIRecommendationState).filter(
            ClientAIRecommendationState.org_id == org_id,
            ClientAIRecommendationState.client_id == rid,
        ).first()
        if not r_rec:
            continue
        if keep_rec is None:
            r_rec.client_id = keep_id
            keep_rec = r_rec
        else:
            merged = _merge_recommendation_actions(keep_rec.actions, r_rec.actions)
            keep_rec.actions = merged
            if not (keep_rec.headline or "").strip() and (r_rec.headline or "").strip():
                keep_rec.headline = r_rec.headline
            db.delete(r_rec)
        flag_modified(keep_rec, "actions")
    db.flush()

    db.query(ClientHealthScoreCache).filter(ClientHealthScoreCache.client_id.in_(remove_ids)).delete(
        synchronize_session=False
    )


def refresh_insight_summary_from_latest_stored_insight(
    db: Session,
    org_id: uuid.UUID,
    client_id: uuid.UUID,
) -> None:
    """Rebuild board headline/tags from the newest stored complete insight (no LLM, no Fathom re-ingest)."""
    client = db.query(Client).filter(Client.id == client_id, Client.org_id == org_id).first()
    if not client:
        return
    row = (
        db.query(ClientCallInsight)
        .filter(
            ClientCallInsight.org_id == org_id,
            ClientCallInsight.client_id == client_id,
            ClientCallInsight.status == "complete",
        )
        .order_by(desc(ClientCallInsight.computed_at))
        .first()
    )
    if not row or not row.insight_json or not isinstance(row.insight_json, dict):
        return
    fj = db.query(FathomCallRecord).filter(FathomCallRecord.id == row.fathom_call_record_id).first()
    if not fj:
        return
    from app.models.client_health_score_cache import ClientHealthScoreCache

    cache_row = db.query(ClientHealthScoreCache).filter(ClientHealthScoreCache.client_id == client_id).first()
    health: Dict[str, Any] = {
        "grade": (cache_row.grade if cache_row else "") or "",
        "score": float(cache_row.score) if cache_row and cache_row.score is not None else None,
    }
    _upsert_summary(db, org_id, client, fj, row.insight_json, health)
