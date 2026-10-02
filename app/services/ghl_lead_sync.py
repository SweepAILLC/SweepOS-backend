"""GHL lead reconcile pull: form + survey submissions -> leads on GHL-paired funnels.

The safety net under the real-time opt-in webhook, and the only lead path when an
org has no webhook. One run per org pulls every submission since the cursor (one
pull serves every paired funnel), routes each to the funnel whose step paths match
its page URL (or whose extra-form list holds its form), and tags the lead through
the same upsert as POST /funnels/leads.

Each submission is recorded in the inbound webhook inbox (provider "ghl_sync",
event id "<kind>:<submission id>"): the inbox's unique key makes re-runs and
overlap with the webhook safe, and a failed submission is retried with backoff by
the worker's inbox flush (see inbound_webhook_inbox.flush_due_inbound_webhooks).

A funnel's first run reaches back FIRST_RUN_DAYS and sends no lead notifications,
so pairing a funnel never floods the coach with months-old leads.
"""
from __future__ import annotations

import logging
import uuid
import zlib
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from app.models.funnel import Funnel
from app.schemas.funnel import FunnelLeadIn
from app.services import ghl_client as gc
from app.services.ghl_funnels import GHL_SOURCE

if TYPE_CHECKING:
    from app.services.funnel_leads import FunnelLeadResult

LOG = logging.getLogger(__name__)

PROVIDER = "ghl_sync"
FIRST_RUN_DAYS = 90
# The submissions API filters by whole day, so each run re-reads the cursor day too.
REREAD_DAYS = 1
WEBHOOK_FRESH_FOR = timedelta(days=7)
DAILY = timedelta(hours=24)
KINDS = ("forms", "surveys")
_ITERATORS = {"forms": gc.iter_ghl_form_submissions, "surveys": gc.iter_ghl_survey_submissions}


# --- pure helpers -------------------------------------------------------------------


def _step_paths(funnel: Funnel) -> set[str]:
    cfg = funnel.ghl_config if isinstance(funnel.ghl_config, dict) else {}
    paths = {s.get("path") for s in cfg.get("steps") or [] if isinstance(s, dict) and s.get("path")}
    if cfg.get("path"):
        paths.add(cfg["path"])
    return paths


def route_submission(sub: Dict[str, Any], funnels: List[Funnel]) -> Optional[Funnel]:
    """The paired funnel a normalized submission belongs to, or None.

    Page path wins over the extra-forms list; ties go to the earliest-paired funnel
    so routing is stable across runs."""
    ordered = sorted(funnels, key=lambda f: str((f.ghl_config or {}).get("paired_at") or ""))
    path = sub.get("page_path")
    if path:
        for f in ordered:
            if path in _step_paths(f):
                return f
    form_id = sub.get("form_id")
    if form_id:
        for f in ordered:
            if form_id in ((f.ghl_config or {}).get("extra_form_ids") or []):
                return f
    return None


def lead_from_submission(sub: Dict[str, Any], funnel_id: uuid.UUID) -> FunnelLeadIn:
    """FunnelLeadIn for a normalized submission. Oversized answers are dropped rather
    than failing the lead (FunnelLeadIn caps prospect JSON size)."""
    fields = dict(
        funnel_id=funnel_id,
        email=sub.get("email"),
        phone=sub.get("phone"),
        first_name=sub.get("first_name"),
        last_name=sub.get("last_name"),
        name=sub.get("name") if not (sub.get("first_name") or sub.get("last_name")) else None,
        source=f"ghl_{sub.get('kind', 'forms').rstrip('s')}",
        funnel_step_reached=sub.get("page_path"),
    )
    answers = sub.get("answers") or None
    try:
        return FunnelLeadIn(**fields, opt_in_data=answers)
    except ValidationError:
        LOG.warning("ghl lead sync: answers too large for submission %s; kept the lead", sub.get("submission_id"))
        return FunnelLeadIn(**fields)


def _json_safe(sub: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(sub)
    if isinstance(out.get("created_at"), datetime):
        out["created_at"] = out["created_at"].isoformat()
    return out


def _sync_state(funnel: Funnel) -> Dict[str, Any]:
    cfg = funnel.ghl_config if isinstance(funnel.ghl_config, dict) else {}
    state = cfg.get("sync")
    return state if isinstance(state, dict) else {}


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _webhook_is_live(funnel: Funnel, now: datetime) -> bool:
    webhook = (funnel.ghl_config or {}).get("webhook") or {}
    last_hook = _parse_dt(webhook.get("last_received_at")) if isinstance(webhook, dict) else None
    return bool(last_hook and now - last_hook <= WEBHOOK_FRESH_FOR)


def is_due(funnels: List[Funnel], now: datetime, interval: timedelta) -> bool:
    """Daily when a paired funnel has a live webhook (seen within WEBHOOK_FRESH_FOR),
    else every `interval`. A funnel that never ran is always due."""
    for f in funnels:
        state = _sync_state(f)
        last_run = _parse_dt(state.get("last_run_at"))
        if last_run is None:
            return True
        every = DAILY if _webhook_is_live(f, now) else interval
        if now - last_run >= every:
            return True
    return False


def window_for(funnels: List[Funnel], today: date) -> tuple[date, bool]:
    """(start date, any funnel on its first run). One window covers every funnel."""
    starts: List[date] = []
    first_run = False
    for f in funnels:
        cursor = _sync_state(f).get("cursor")
        try:
            starts.append(date.fromisoformat(cursor) - timedelta(days=REREAD_DAYS))
        except (TypeError, ValueError):
            first_run = True
            starts.append(today - timedelta(days=FIRST_RUN_DAYS))
    return min(starts), first_run


# --- inbox processor ----------------------------------------------------------------


def process_submission_payload(db: Session, org_id: uuid.UUID, payload: Dict[str, Any]) -> Optional["FunnelLeadResult"]:
    """Inbox processor (also used for retries): tag one submission's lead.
    Returns the upsert result, or None when the submission was dropped."""
    from app.api.funnels import normalize_utm
    from app.services.funnel_leads import upsert_funnel_lead

    sub = payload.get("submission") if isinstance(payload.get("submission"), dict) else {}
    try:
        funnel_id = uuid.UUID(str(payload.get("funnel_id")))
    except ValueError:
        return None
    funnel = (
        db.query(Funnel)
        .filter(Funnel.id == funnel_id, Funnel.org_id == org_id, Funnel.source == GHL_SOURCE)
        .first()
    )
    if funnel is None:  # unpaired or deleted since the submission was recorded
        return None
    if not (sub.get("email") or sub.get("phone") or sub.get("name") or sub.get("first_name")):
        return None  # nothing to identify a person by
    return upsert_funnel_lead(
        db,
        funnel,
        lead_from_submission(sub, funnel.id),
        utm=normalize_utm(sub.get("utm_raw")),
        opted_in_at=_parse_dt(sub.get("created_at")),
        ghl_contact_id=sub.get("contact_id"),
        reattribute=True,
        notify=bool(payload.get("notify", True)),
    )


# --- the run -----------------------------------------------------------------------


@contextmanager
def _org_lock(org_id: uuid.UUID) -> Iterator[bool]:
    """Postgres session advisory lock on a dedicated connection, so a scheduled run
    and "Sync now" never pull the same org at once. Yields False when held elsewhere."""
    from app.db.session import engine

    key = zlib.crc32(f"ghl_lead_sync:{org_id}".encode())
    with engine.connect() as conn:
        got = bool(conn.execute(select(func.pg_try_advisory_lock(key))).scalar())
        try:
            yield got
        finally:
            if got:
                conn.execute(select(func.pg_advisory_unlock(key)))
            conn.commit()


def paired_funnels(db: Session, org_id: uuid.UUID) -> List[Funnel]:
    return db.query(Funnel).filter(Funnel.org_id == org_id, Funnel.source == GHL_SOURCE).all()


def _save_state(
    db: Session,
    funnels: List[Funnel],
    update: Dict[str, Any],
    per_funnel: Optional[Dict[uuid.UUID, Dict[str, Any]]] = None,
) -> None:
    for f in funnels:
        cfg = dict(f.ghl_config or {})
        cfg["sync"] = {**_sync_state(f), **update, **((per_funnel or {}).get(f.id) or {})}
        f.ghl_config = cfg
        flag_modified(f, "ghl_config")
    db.commit()


def _after_run_side_effects(db: Session, org_id: uuid.UUID, touched_days: set[date]) -> None:
    """Batched once per run (the upsert already invalidates each client's health score)."""
    if not touched_days:
        return
    from app.services.kpi_integration_sync import sync_kpi_for_datetime
    from app.services.terminal_metrics_service import invalidate_terminal_monthly_trends_cache

    invalidate_terminal_monthly_trends_cache(org_id)
    for day in sorted(touched_days):
        try:
            sync_kpi_for_datetime(db, org_id, datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc), commit=True)
        except Exception:
            LOG.exception("ghl lead sync: KPI sync failed org=%s day=%s", org_id, day)
            db.rollback()


def sync_org(db: Session, org_id: uuid.UUID, *, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Pull and tag one org's GHL submissions. Returns counts; never raises for GHL errors."""
    from app.services.inbound_webhook_inbox import STATUS_DONE, process_recorded_event, record_inbound_event

    now = now or datetime.now(timezone.utc)
    funnels = paired_funnels(db, org_id)
    counts = {"seen": 0, "routed": 0, "processed": 0, "skipped": 0, "failed": 0, "new": 0}
    if not funnels:
        return counts
    try:
        headers, location_id = gc.get_ghl_connection(db, org_id)
    except gc.GhlNotConnectedError:
        _save_state(db, funnels, {"last_error": "GoHighLevel is not connected", "last_attempt_at": now.isoformat()})
        return counts

    start, first_run = window_for(funnels, now.date())
    new_funnel_ids = {f.id for f in funnels if not _sync_state(f).get("cursor")}
    live_webhook_ids = {f.id for f in funnels if _webhook_is_live(f, now)}
    # Per funnel: leads this run created or tagged, and how many of those a live
    # webhook should have delivered first (recent, past the funnel's first run).
    new_leads: Dict[uuid.UUID, int] = {}
    missed: Dict[uuid.UUID, int] = {}
    touched_days: set[date] = set()
    error: Optional[str] = None
    try:
        for kind in KINDS:
            for raw in _ITERATORS[kind](headers, location_id, start, now.date()):
                counts["seen"] += 1
                sub = gc.normalize_ghl_submission(raw, kind)
                funnel = route_submission(sub, funnels) if sub else None
                if funnel is None:
                    counts["skipped"] += 1
                    continue
                counts["routed"] += 1
                row, _ = record_inbound_event(
                    db,
                    org_id=org_id,
                    provider=PROVIDER,
                    event_id=f"{kind}:{sub['submission_id']}",
                    event_type=kind,
                    payload={
                        "funnel_id": str(funnel.id),
                        "submission": _json_safe(sub),
                        "notify": funnel.id not in new_funnel_ids,
                    },
                )
                if row.status == STATUS_DONE:
                    continue
                outcome: List[Any] = []

                def _capture(d: Session, o: uuid.UUID, p: Dict[str, Any], _out: List[Any] = outcome) -> None:
                    _out.append(process_submission_payload(d, o, p))

                if process_recorded_event(db, row, _capture):
                    counts["processed"] += 1
                    if sub.get("created_at"):
                        touched_days.add(sub["created_at"].date())
                    result = outcome[0] if outcome else None
                    if result is not None and not result.duplicate:
                        counts["new"] += 1
                        new_leads[funnel.id] = new_leads.get(funnel.id, 0) + 1
                        created = sub.get("created_at")
                        if (
                            funnel.id in live_webhook_ids
                            and funnel.id not in new_funnel_ids
                            and created is not None
                            and now - created <= WEBHOOK_FRESH_FOR
                        ):
                            missed[funnel.id] = missed.get(funnel.id, 0) + 1
                else:
                    counts["failed"] += 1
    except gc.GhlApiError as e:
        db.rollback()
        if e.status_code == 401:
            error = "GoHighLevel rejected the token (401). Reconnect GHL in Integrations."
        elif e.status_code == 429:
            error = "GoHighLevel rate limit reached; resuming next run."
        else:
            error = f"GoHighLevel request failed ({e.status_code or 'network'})."
        LOG.warning("ghl lead sync org=%s stopped: %s", org_id, error)

    state: Dict[str, Any] = {"last_attempt_at": now.isoformat(), "last_error": error, "last_counts": counts}
    if error is None:
        # Cursor only moves on a complete run, so a partial run is re-read next time.
        state.update({"cursor": now.date().isoformat(), "last_run_at": now.isoformat()})
    per_funnel: Dict[uuid.UUID, Dict[str, Any]] = {}
    for f in funnels:
        extra: Dict[str, Any] = {"missed_by_webhook": missed.get(f.id, 0)}
        if new_leads.get(f.id):
            extra["last_lead_at"] = now.isoformat()
        per_funnel[f.id] = extra
    _save_state(db, funnels, state, per_funnel)
    _after_run_side_effects(db, org_id, touched_days)
    LOG.info("ghl lead sync org=%s first_run=%s %s", org_id, first_run, counts)
    return counts


def run_ghl_lead_sync_job(org_id_str: str) -> None:
    """Entry point for "Sync now" (schedule_background_work): own session, org lock."""
    from app.db.session import SessionLocal

    org_id = uuid.UUID(org_id_str)
    with _org_lock(org_id) as got:
        if not got:
            LOG.info("ghl lead sync org=%s skipped: already running", org_id)
            return
        db = SessionLocal()
        try:
            sync_org(db, org_id)
        except Exception:
            LOG.exception("ghl lead sync failed org=%s", org_id)
            db.rollback()
        finally:
            db.close()


def catchup_ghl_leads_for_all_orgs() -> Dict[str, int]:
    """Worker entry: run every org with a paired GHL funnel that is due."""
    from app.core.config import settings
    from app.db.session import SessionLocal

    interval = timedelta(seconds=int(getattr(settings, "GHL_LEAD_SYNC_INTERVAL_SEC", 900) or 900))
    stats = {"orgs": 0, "synced": 0, "failed": 0}
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        org_ids = [r[0] for r in db.query(Funnel.org_id).filter(Funnel.source == GHL_SOURCE).distinct().all()]
    for org_id in org_ids:
        stats["orgs"] += 1
        with _org_lock(org_id) as got:
            if not got:
                continue
            db = SessionLocal()
            try:
                if not is_due(paired_funnels(db, org_id), now, interval):
                    continue
                sync_org(db, org_id, now=now)
                stats["synced"] += 1
            except Exception:
                stats["failed"] += 1
                LOG.exception("ghl lead sync failed org=%s", org_id)
                db.rollback()
            finally:
                db.close()
    return stats
