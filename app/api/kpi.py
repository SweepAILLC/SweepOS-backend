"""KPI Command Center API — daily entries, rollups, benchmarks, bottleneck flags."""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.config import settings
from app.db.session import get_db
from app.models.org_kpi_benchmark import OrgKpiBenchmark
from app.models.org_kpi_daily_entry import OrgKpiDailyEntry
from app.models.user import User
from app.schemas.kpi import (
    DEFAULT_CONTENT_TYPE_TAGS,
    DEFAULT_KPI_THRESHOLDS,
    KpiAutopopulateStatusResponse,
    KpiBenchmarks,
    KpiBenchmarksUpdate,
    KpiBulkImportRequest,
    KpiBulkImportResponse,
    KpiDailyEntryRead,
    KpiEntryLinkResponse,
    KpiDailyEntryUpdate,
    KpiFlagsResponse,
    KpiMonthlyRollup,
    KpiSnapshotResponse,
    compute_rates,
)
from app.services.kpi_bottleneck_service import detect_bottlenecks, utcnow
from app.services.kpi_compute import (
    build_kpi_snapshot,
    build_monthly_rollups,
    normalize_thresholds,
    thresholds_as_dict,
)
from app.services.kpi_integration_sync import (
    compute_live_fields_for_day,
    has_calendar_source as _has_calendar_source,
    has_payment_source as _has_payment_source,
    refresh_kpi_live_fields_for_range,
)

router = APIRouter()

UPSERT_FIELDS = (
    "total_followers",
    "new_followers",
    "content_posted",
    "best_content_type",
    "inboxes_checked",
    "outreach_sent",
    "respondents",
    "inbound_icp_leads",
    "followups_sent",
    "new_conversations",
    "conversations_nurtured",
    "calls_pitched",
    "inbound_bookings",
    "outbound_bookings",
    "calls_booked",
    "calls_taken",
    "offers_made",
    "no_shows",
    "closes",
    "cash_collected",
    "revenue",
    "setter_context",
)

# Survey submissions merge-add these into the day's existing totals.
ADDITIVE_INT_FIELDS = frozenset(
    {
        "inboxes_checked",
        "outreach_sent",
        "respondents",
        "inbound_icp_leads",
        "followups_sent",
        "new_conversations",
        "conversations_nurtured",
        "calls_pitched",
        "inbound_bookings",
        "outbound_bookings",
        "offers_made",
        "calls_taken",
        "no_shows",
        "closes",
        "new_followers",
    }
)
ADDITIVE_DECIMAL_FIELDS = frozenset({"cash_collected", "revenue"})


def _org_id(user: User) -> uuid.UUID:
    return getattr(user, "selected_org_id", user.org_id)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid date '{value}'. Use YYYY-MM-DD.",
        ) from e


def _compute_new_followers_from_previous_day(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
    total_followers: Optional[int],
) -> Optional[int]:
    """Auto-fill new_followers from the previous day's total_followers."""
    if total_followers is None:
        return None
    prev = (
        db.query(OrgKpiDailyEntry)
        .filter(
            OrgKpiDailyEntry.org_id == org_id,
            OrgKpiDailyEntry.entry_date < entry_day,
            OrgKpiDailyEntry.total_followers.isnot(None),
        )
        .order_by(OrgKpiDailyEntry.entry_date.desc())
        .first()
    )
    if not prev or prev.total_followers is None:
        return None
    try:
        return int(total_followers) - int(prev.total_followers)
    except (TypeError, ValueError):
        return None


def _get_or_seed_benchmarks(db: Session, org_id: uuid.UUID) -> OrgKpiBenchmark:
    row = db.query(OrgKpiBenchmark).filter(OrgKpiBenchmark.org_id == org_id).first()
    if row:
        return row
    row = OrgKpiBenchmark(
        org_id=org_id,
        thresholds=dict(DEFAULT_KPI_THRESHOLDS),
        content_type_tags=list(DEFAULT_CONTENT_TYPE_TAGS),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _benchmarks_response(row: OrgKpiBenchmark) -> KpiBenchmarks:
    thresholds = normalize_thresholds(row.thresholds if isinstance(row.thresholds, dict) else None)
    tags = row.content_type_tags if isinstance(row.content_type_tags, list) else list(DEFAULT_CONTENT_TYPE_TAGS)
    return KpiBenchmarks(
        org_id=row.org_id,
        thresholds=thresholds,
        content_type_tags=tags,
        updated_at=row.updated_at,
        entry_form_token=str(row.entry_form_token) if row.entry_form_token else None,
    )


def _autopopulate_from_integrations(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Force-refresh live auto fields from integrations; never writes revenue (manual)."""
    out = dict(payload)
    # Drop stale revenue keys from auto path — revenue is manual-only.
    live = compute_live_fields_for_day(db, org_id, entry_day)
    # Always overwrite live auto fields from source of truth when available.
    for field, value in live.items():
        out[field] = value

    # Through today: remaining auto defaults (new_followers) when unset
    if entry_day <= date.today():
        if "new_followers" not in out:
            out["new_followers"] = 0
        calendar_available = _has_calendar_source(db, org_id)
        payments_available = _has_payment_source(db, org_id)
        if calendar_available:
            for field in ("calls_booked", "calls_taken", "closes", "no_shows"):
                if field not in out:
                    out[field] = 0
        if payments_available and "cash_collected" not in out:
            out["cash_collected"] = 0.0

    return out


def _with_auto_zero_defaults(
    read: KpiDailyEntryRead,
    entry_day: date,
    *,
    calendar_available: bool,
    payments_available: bool,
) -> KpiDailyEntryRead:
    """Coerce null auto fields to 0 for days through today (display / API consistency)."""
    if entry_day > date.today():
        return read
    data = read.model_dump()
    changed = False
    if data.get("new_followers") is None:
        data["new_followers"] = 0
        changed = True
    if calendar_available:
        for field in ("calls_booked", "calls_taken", "closes", "no_shows"):
            if data.get(field) is None:
                data[field] = 0
                changed = True
    if payments_available and data.get("cash_collected") is None:
        data["cash_collected"] = 0.0
        changed = True
    if not changed:
        return read
    data.update(compute_rates(data))
    return KpiDailyEntryRead(**data)


# ---------------------------------------------------------------------------
# Entries
# ---------------------------------------------------------------------------

@router.get("/entries", response_model=List[KpiDailyEntryRead])
def list_kpi_entries(
    start: Optional[str] = Query(None, description="YYYY-MM-DD inclusive"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD inclusive"),
    sync: bool = Query(
        True,
        description="When true, refresh live calendar/payment fields before returning. "
        "Pass false for fast month navigation (cached rows only).",
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    today = date.today()
    range_start = _parse_date(start) if start else today - timedelta(days=90)
    range_end = _parse_date(end) if end else today
    # Keep no_shows / closes / cash_collected in sync with calendar + payments.
    # Skip on fast month-nav fetches so 2-month compare stays snappy.
    if sync:
        try:
            refresh_kpi_live_fields_for_range(
                db,
                org_id,
                range_start,
                min(range_end, today),
            )
        except Exception:
            db.rollback()

    q = db.query(OrgKpiDailyEntry).filter(OrgKpiDailyEntry.org_id == org_id)
    q = q.filter(OrgKpiDailyEntry.entry_date >= range_start)
    q = q.filter(OrgKpiDailyEntry.entry_date <= range_end)
    rows = q.order_by(OrgKpiDailyEntry.entry_date.asc()).all()
    calendar_available = _has_calendar_source(db, org_id)
    payments_available = _has_payment_source(db, org_id)
    dirty = False
    for r in rows:
        if r.entry_date > today:
            continue
        if r.new_followers is None:
            r.new_followers = 0
            dirty = True
        if calendar_available:
            for field in ("calls_booked", "calls_taken", "closes", "no_shows"):
                if getattr(r, field) is None:
                    setattr(r, field, 0)
                    dirty = True
        if payments_available and r.cash_collected is None:
            r.cash_collected = 0.0
            dirty = True
    if dirty:
        db.commit()
        for r in rows:
            db.refresh(r)
    return [
        _with_auto_zero_defaults(
            KpiDailyEntryRead.from_orm_row(r),
            r.entry_date,
            calendar_available=calendar_available,
            payments_available=payments_available,
        )
        for r in rows
    ]


@router.put("/entries/{entry_date}", response_model=KpiDailyEntryRead)
def upsert_kpi_entry(
    entry_date: str,
    body: KpiDailyEntryUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Upsert a single day's metrics (inline autosave)."""
    org_id = _org_id(current_user)
    d = _parse_date(entry_date)
    return _upsert_kpi_entry_for_org(
        db=db,
        org_id=org_id,
        entry_day=d,
        payload=body.model_dump(exclude_unset=True),
    )


@router.post("/entries/bulk", response_model=KpiBulkImportResponse)
def bulk_import_kpi_entries(
    body: KpiBulkImportRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Bulk upsert daily KPI rows (CSV import). Max 500 rows."""
    from sqlalchemy.exc import DataError, IntegrityError

    org_id = _org_id(current_user)
    if len(body.entries) > 500:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Maximum 500 rows per import",
        )
    imported: List[KpiDailyEntryRead] = []
    # Process oldest-first so new_followers can use previous-day totals within the batch.
    ordered = sorted(body.entries, key=lambda e: e.entry_date)
    try:
        for item in ordered:
            payload = item.model_dump(exclude_unset=True, exclude={"entry_date"})
            imported.append(
                _upsert_kpi_entry_for_org(
                    db=db,
                    org_id=org_id,
                    entry_day=item.entry_date,
                    payload=payload,
                )
            )
    except (DataError, IntegrityError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Import failed on {ordered[len(imported)].entry_date if len(imported) < len(ordered) else 'a row'}: "
                "a field value is too long or invalid. "
                "best_content_type max length is 512 characters."
            ),
        ) from exc
    return KpiBulkImportResponse(imported=len(imported), entries=imported)


def _merge_additive_payload(row: OrgKpiDailyEntry, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Turn survey deltas into absolute values by adding onto the existing day row."""
    merged: Dict[str, Any] = {}
    for field, incoming in payload.items():
        if field not in UPSERT_FIELDS or incoming is None:
            continue
        if field in ADDITIVE_INT_FIELDS:
            existing = getattr(row, field, None)
            merged[field] = int(existing or 0) + int(incoming)
        elif field in ADDITIVE_DECIMAL_FIELDS:
            existing = getattr(row, field, None)
            try:
                base = float(existing) if existing is not None else 0.0
                merged[field] = base + float(incoming)
            except (TypeError, ValueError):
                merged[field] = incoming
        elif field == "content_posted":
            merged[field] = bool(getattr(row, field, None)) or bool(incoming)
        elif field == "setter_context":
            existing = (getattr(row, field, None) or "").strip()
            text = str(incoming).strip()
            if existing and text:
                merged[field] = f"{existing}\n{text}"
            else:
                merged[field] = text or existing or None
        else:
            # Absolute set: total_followers, best_content_type, calls_booked (rare on survey)
            merged[field] = incoming
    return merged


def _sync_calls_booked_from_booking_split(
    db: Session,
    org_id: uuid.UUID,
    row: OrgKpiDailyEntry,
    payload: Dict[str, Any],
    *,
    additive: bool,
) -> None:
    """When calendar is offline, keep calls_booked = inbound + outbound bookings."""
    if _has_calendar_source(db, org_id):
        return
    # Absolute writes may still set calls_booked directly (CSV / grid).
    if not additive and "calls_booked" in payload:
        return
    if "inbound_bookings" not in payload and "outbound_bookings" not in payload:
        return
    inbound = int(row.inbound_bookings or 0)
    outbound = int(row.outbound_bookings or 0)
    row.calls_booked = inbound + outbound


def _upsert_kpi_entry_for_org(
    db: Session,
    org_id: uuid.UUID,
    entry_day: date,
    payload: Dict[str, Any],
    *,
    additive: bool = False,
) -> KpiDailyEntryRead:
    row = (
        db.query(OrgKpiDailyEntry)
        .filter(OrgKpiDailyEntry.org_id == org_id, OrgKpiDailyEntry.entry_date == entry_day)
        .first()
    )
    if row is None:
        row = OrgKpiDailyEntry(org_id=org_id, entry_date=entry_day)
        db.add(row)

    payload = dict(payload)
    if additive:
        payload = _merge_additive_payload(row, payload)
    # Auto-fill new_followers from previous day's total if not explicitly provided.
    if "total_followers" in payload and "new_followers" not in payload:
        payload["new_followers"] = _compute_new_followers_from_previous_day(
            db=db,
            org_id=org_id,
            entry_day=entry_day,
            total_followers=payload.get("total_followers"),
        )

    payload = _autopopulate_from_integrations(
        db=db,
        org_id=org_id,
        entry_day=entry_day,
        payload=payload,
    )

    for field in UPSERT_FIELDS:
        if field in payload:
            setattr(row, field, payload[field])

    _sync_calls_booked_from_booking_split(
        db, org_id, row, payload, additive=additive
    )

    row.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return _with_auto_zero_defaults(
        KpiDailyEntryRead.from_orm_row(row),
        entry_day,
        calendar_available=_has_calendar_source(db, org_id),
        payments_available=_has_payment_source(db, org_id),
    )


@router.delete("/entries/{entry_date}", status_code=status.HTTP_204_NO_CONTENT)
def delete_kpi_entry(
    entry_date: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    d = _parse_date(entry_date)
    row = (
        db.query(OrgKpiDailyEntry)
        .filter(OrgKpiDailyEntry.org_id == org_id, OrgKpiDailyEntry.entry_date == d)
        .first()
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entry not found")
    db.delete(row)
    db.commit()
    return None


# ---------------------------------------------------------------------------
# Rollups
# ---------------------------------------------------------------------------

@router.get("/rollups", response_model=List[KpiMonthlyRollup])
def list_kpi_rollups(
    months: int = Query(12, ge=1, le=36),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    # Load enough history for the requested month count
    start = date.today().replace(day=1) - timedelta(days=31 * months)
    rows = (
        db.query(OrgKpiDailyEntry)
        .filter(OrgKpiDailyEntry.org_id == org_id, OrgKpiDailyEntry.entry_date >= start)
        .order_by(OrgKpiDailyEntry.entry_date.asc())
        .all()
    )
    return build_monthly_rollups(rows, months=months)


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

@router.get("/benchmarks", response_model=KpiBenchmarks)
def get_kpi_benchmarks(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    row = _get_or_seed_benchmarks(db, org_id)
    return _benchmarks_response(row)


@router.put("/benchmarks", response_model=KpiBenchmarks)
def update_kpi_benchmarks(
    body: KpiBenchmarksUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    row = _get_or_seed_benchmarks(db, org_id)
    if body.thresholds is not None:
        current = normalize_thresholds(row.thresholds if isinstance(row.thresholds, dict) else None)
        for key, mt in body.thresholds.items():
            current[key] = mt
        row.thresholds = thresholds_as_dict(current)
    if body.content_type_tags is not None:
        row.content_type_tags = body.content_type_tags
    row.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return _benchmarks_response(row)


@router.get("/entry-link", response_model=KpiEntryLinkResponse)
def get_kpi_entry_link(
    regenerate: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    row = _get_or_seed_benchmarks(db, org_id)
    if regenerate or not row.entry_form_token:
        row.entry_form_token = uuid.uuid4()
        row.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(row)
    base = str(getattr(settings, "FRONTEND_URL", "") or "http://localhost:3002").rstrip("/")
    token = str(row.entry_form_token)
    return KpiEntryLinkResponse(
        token=token,
        url=f"{base}/kpi-entry/{token}",
    )


@router.get("/autopopulate-status", response_model=KpiAutopopulateStatusResponse)
def get_kpi_autopopulate_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    calendar_available = _has_calendar_source(db, org_id)
    payments_available = _has_payment_source(db, org_id)
    cols = ["new_followers"]
    if calendar_available:
        cols.extend(["calls_booked", "calls_taken", "closes", "no_shows"])
    if payments_available:
        cols.append("cash_collected")
    return KpiAutopopulateStatusResponse(
        calendar_available=calendar_available,
        payments_available=payments_available,
        autopopulated_columns=cols,
    )


# ---------------------------------------------------------------------------
# Snapshot (cross-tab essentials)
# ---------------------------------------------------------------------------

@router.get("/snapshot", response_model=KpiSnapshotResponse)
def get_kpi_snapshot(
    days: int = Query(30, ge=1, le=365, description="Trailing window length in days"),
    start: Optional[str] = Query(None, description="YYYY-MM-DD inclusive (overrides days)"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD inclusive"),
    include_flags: bool = Query(True),
    include_series: bool = Query(True),
    sync: bool = Query(
        False,
        description="Refresh live calendar/payment fields before building the snapshot",
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Essential KPI cards, series, rollup, and bottleneck flags for other tabs."""
    org_id = _org_id(current_user)
    today = date.today()
    range_end = _parse_date(end) if end else today
    range_start = _parse_date(start) if start else range_end - timedelta(days=days - 1)
    if range_end < range_start:
        raise HTTPException(status_code=400, detail="end must be on or after start")

    if sync:
        try:
            refresh_kpi_live_fields_for_range(
                db, org_id, range_start, min(range_end, today)
            )
        except Exception:
            db.rollback()

    rows = (
        db.query(OrgKpiDailyEntry)
        .filter(
            OrgKpiDailyEntry.org_id == org_id,
            OrgKpiDailyEntry.entry_date >= range_start,
            OrgKpiDailyEntry.entry_date <= range_end,
        )
        .order_by(OrgKpiDailyEntry.entry_date.asc())
        .all()
    )
    bench = _get_or_seed_benchmarks(db, org_id)
    flags = []
    if include_flags:
        flags = detect_bottlenecks(
            db,
            org_id,
            thresholds_raw=bench.thresholds if isinstance(bench.thresholds, dict) else None,
        )
    return build_kpi_snapshot(
        rows,
        range_start=range_start,
        range_end=range_end,
        thresholds_raw=bench.thresholds if isinstance(bench.thresholds, dict) else None,
        flags=flags,
        include_series=include_series,
        calendar_available=_has_calendar_source(db, org_id),
        payments_available=_has_payment_source(db, org_id),
        generated_at=utcnow(),
    )


# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

@router.get("/flags", response_model=KpiFlagsResponse)
def get_kpi_flags(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    org_id = _org_id(current_user)
    bench = _get_or_seed_benchmarks(db, org_id)
    flags = detect_bottlenecks(
        db,
        org_id,
        thresholds_raw=bench.thresholds if isinstance(bench.thresholds, dict) else None,
    )
    return KpiFlagsResponse(flags=flags, generated_at=utcnow())


@router.get("/public/{token}/entries/{entry_date}", response_model=KpiDailyEntryRead)
def get_public_kpi_entry(
    token: str,
    entry_date: str,
    db: Session = Depends(get_db),
):
    try:
        token_uuid = uuid.UUID(token)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid entry token") from exc
    bench = (
        db.query(OrgKpiBenchmark)
        .filter(OrgKpiBenchmark.entry_form_token == token_uuid)
        .first()
    )
    if not bench:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid entry token")
    day = _parse_date(entry_date)
    row = (
        db.query(OrgKpiDailyEntry)
        .filter(OrgKpiDailyEntry.org_id == bench.org_id, OrgKpiDailyEntry.entry_date == day)
        .first()
    )
    if row is None:
        row = OrgKpiDailyEntry(org_id=bench.org_id, entry_date=day)
        db.add(row)
        db.commit()
        db.refresh(row)
    return _with_auto_zero_defaults(
        KpiDailyEntryRead.from_orm_row(row),
        day,
        calendar_available=_has_calendar_source(db, bench.org_id),
        payments_available=_has_payment_source(db, bench.org_id),
    )


@router.put("/public/{token}/entries/{entry_date}", response_model=KpiDailyEntryRead)
def upsert_public_kpi_entry(
    token: str,
    entry_date: str,
    body: KpiDailyEntryUpdate,
    db: Session = Depends(get_db),
):
    try:
        token_uuid = uuid.UUID(token)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid entry token") from exc
    bench = (
        db.query(OrgKpiBenchmark)
        .filter(OrgKpiBenchmark.entry_form_token == token_uuid)
        .first()
    )
    if not bench:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid entry token")
    day = _parse_date(entry_date)
    # Public survey submissions add to the day's existing totals; grid edits stay absolute.
    return _upsert_kpi_entry_for_org(
        db=db,
        org_id=bench.org_id,
        entry_day=day,
        payload=body.model_dump(exclude_unset=True),
        additive=True,
    )
