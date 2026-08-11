"""
Batched funnel-lead digest emails for org admins.

Leads are enqueued at capture time into ``funnel_lead_notifications``.
The worker opens a fixed window from the oldest unsent row per org and
flushes one digest when the window elapses, using the same Brevo path as
org invites (``send_onboarding_email`` / global ``BREVO_API_KEY``).
"""
from __future__ import annotations

import html
import logging
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.client import Client
from app.models.funnel import Funnel
from app.models.funnel_lead_notification import FunnelLeadNotification
from app.models.organization import Organization
from app.services.onboarding_email import send_onboarding_email

LOG = logging.getLogger("app.funnel_lead_notifications")

MAX_ATTEMPTS = 5
DIGEST_ROW_CAP = 50
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_SCHEMA_READY = False

DEFAULT_FUNNEL_LEAD_SETTINGS: Dict[str, Any] = {
    "enabled": True,
    "window_minutes": 15,
    "recipient_mode": "admins",  # admins | custom
    "recipients": [],
    "include_returning_leads": True,
}


def ensure_funnel_lead_notifications_schema(db: Session) -> bool:
    """
    Ensure digest queue table + organizations.notification_settings exist.

    Safe to call repeatedly. Used as a prod safety net when alembic 066 has not
    been applied yet (missing table would abort Postgres transactions and 500
    the leads tab).
    """
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return True
    try:
        # Clear any aborted transaction before DDL
        try:
            db.rollback()
        except Exception:
            pass
        FunnelLeadNotification.__table__.create(db.bind, checkfirst=True)
        db.execute(
            text(
                "ALTER TABLE organizations "
                "ADD COLUMN IF NOT EXISTS notification_settings JSONB"
            )
        )
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_funnel_lead_notifications_unsent_org_created "
                "ON funnel_lead_notifications (org_id, created_at) "
                "WHERE sent_at IS NULL"
            )
        )
        db.commit()
        _SCHEMA_READY = True
        return True
    except Exception:
        LOG.exception("ensure_funnel_lead_notifications_schema failed")
        try:
            db.rollback()
        except Exception:
            pass
        return False


def table_exists(db: Session, table_name: str) -> bool:
    """Return True if a public table exists (never leaves the session aborted)."""
    try:
        row = db.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = :t LIMIT 1"
            ),
            {"t": table_name},
        ).first()
        return row is not None
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return False


def _default_window_minutes() -> int:
    raw = getattr(settings, "FUNNEL_LEAD_DIGEST_WINDOW_MINUTES", 15) or 15
    try:
        return max(1, min(int(raw), 1440))
    except (TypeError, ValueError):
        return 15


def get_funnel_lead_settings(org: Optional[Organization]) -> Dict[str, Any]:
    """Merge stored org.notification_settings.funnel_leads over defaults."""
    cfg = dict(DEFAULT_FUNNEL_LEAD_SETTINGS)
    cfg["window_minutes"] = _default_window_minutes()
    if org is None:
        return cfg
    bag = org.notification_settings if isinstance(org.notification_settings, dict) else {}
    stored = bag.get("funnel_leads") if isinstance(bag, dict) else None
    if not isinstance(stored, dict):
        return cfg

    if "enabled" in stored:
        cfg["enabled"] = bool(stored["enabled"])
    if "include_returning_leads" in stored:
        cfg["include_returning_leads"] = bool(stored["include_returning_leads"])
    if "recipient_mode" in stored:
        mode = str(stored["recipient_mode"] or "admins").strip().lower()
        cfg["recipient_mode"] = mode if mode in ("admins", "custom") else "admins"
    if "recipients" in stored and isinstance(stored["recipients"], list):
        cfg["recipients"] = [
            str(e).strip().lower()
            for e in stored["recipients"]
            if isinstance(e, str) and _EMAIL_RE.match(e.strip())
        ]
    if "window_minutes" in stored:
        try:
            cfg["window_minutes"] = max(1, min(int(stored["window_minutes"]), 1440))
        except (TypeError, ValueError):
            pass
    return cfg


def merge_funnel_lead_settings(
    org: Organization,
    patch: Dict[str, Any],
) -> Dict[str, Any]:
    """Apply a partial funnel_leads patch onto org.notification_settings and return merged settings."""
    current = get_funnel_lead_settings(org)
    if "enabled" in patch and patch["enabled"] is not None:
        current["enabled"] = bool(patch["enabled"])
    if "include_returning_leads" in patch and patch["include_returning_leads"] is not None:
        current["include_returning_leads"] = bool(patch["include_returning_leads"])
    if "recipient_mode" in patch and patch["recipient_mode"] is not None:
        mode = str(patch["recipient_mode"]).strip().lower()
        if mode not in ("admins", "custom"):
            raise ValueError("recipient_mode must be 'admins' or 'custom'")
        current["recipient_mode"] = mode
    if "window_minutes" in patch and patch["window_minutes"] is not None:
        try:
            minutes = int(patch["window_minutes"])
        except (TypeError, ValueError) as e:
            raise ValueError("window_minutes must be an integer") from e
        if minutes < 1 or minutes > 1440:
            raise ValueError("window_minutes must be between 1 and 1440")
        current["window_minutes"] = minutes
    if "recipients" in patch and patch["recipients"] is not None:
        if not isinstance(patch["recipients"], list):
            raise ValueError("recipients must be a list of emails")
        cleaned: List[str] = []
        for e in patch["recipients"]:
            if not isinstance(e, str):
                raise ValueError("each recipient must be an email string")
            addr = e.strip().lower()
            if not _EMAIL_RE.match(addr):
                raise ValueError(f"invalid recipient email: {e}")
            if addr not in cleaned:
                cleaned.append(addr)
        current["recipients"] = cleaned

    bag = dict(org.notification_settings) if isinstance(org.notification_settings, dict) else {}
    bag["funnel_leads"] = current
    org.notification_settings = bag
    org.updated_at = datetime.utcnow()
    return current


def _lead_display_name(client: Client, lead: Any) -> Optional[str]:
    first = getattr(lead, "first_name", None) or client.first_name
    last = getattr(lead, "last_name", None) or client.last_name
    name = getattr(lead, "name", None)
    parts = [p for p in (first, last) if p and str(p).strip()]
    if parts:
        return " ".join(str(p).strip() for p in parts)
    if name and str(name).strip():
        return str(name).strip()
    return None


def enqueue_funnel_lead_notification(
    db: Session,
    *,
    org_id: uuid.UUID,
    client: Client,
    funnel: Funnel,
    lead: Any,
    is_new_client: bool,
) -> Optional[uuid.UUID]:
    """Insert a notification queue row. Returns id or None if skipped."""
    if not table_exists(db, "funnel_lead_notifications"):
        if not ensure_funnel_lead_notifications_schema(db):
            LOG.warning("skip enqueue: funnel_lead_notifications schema unavailable")
            return None

    try:
        org = db.query(Organization).filter(Organization.id == org_id).first()
    except Exception:
        # notification_settings column may be missing — ensure and retry once
        db.rollback()
        ensure_funnel_lead_notifications_schema(db)
        org = db.query(Organization).filter(Organization.id == org_id).first()

    cfg = get_funnel_lead_settings(org)
    if not cfg.get("enabled", True):
        return None
    if not is_new_client and not cfg.get("include_returning_leads", True):
        return None

    email = (getattr(lead, "email", None) or client.email or "").strip() or None
    phone = (getattr(lead, "phone", None) or client.phone or "").strip() or None
    instagram = (getattr(lead, "instagram", None) or client.instagram or "").strip() or None
    source = (getattr(lead, "source", None) or "").strip() or None
    step = (getattr(lead, "funnel_step_reached", None) or "").strip() or None

    def _build_row() -> FunnelLeadNotification:
        return FunnelLeadNotification(
            id=uuid.uuid4(),
            org_id=org_id,
            client_id=client.id,
            funnel_id=funnel.id if funnel else None,
            funnel_name=(funnel.name if funnel else None) or None,
            lead_name=_lead_display_name(client, lead),
            lead_email=email,
            lead_phone=phone,
            lead_instagram=instagram,
            source=source,
            funnel_step_reached=step,
            is_new_client=bool(is_new_client),
        )

    row = _build_row()
    try:
        db.add(row)
        db.commit()
        return row.id
    except Exception:
        LOG.exception("enqueue_funnel_lead_notification commit failed")
        try:
            db.rollback()
        except Exception:
            pass
        # One retry after schema ensure (common on first deploy before alembic)
        if ensure_funnel_lead_notifications_schema(db):
            try:
                row = _build_row()
                db.add(row)
                db.commit()
                return row.id
            except Exception:
                LOG.exception("enqueue retry failed")
                try:
                    db.rollback()
                except Exception:
                    pass
        return None


def _admin_emails(db: Session, org_id: uuid.UUID) -> List[str]:
    rows = db.execute(
        text(
            """
            SELECT email FROM users
            WHERE org_id = :org_id
              AND (
                UPPER(role::text) IN ('OWNER', 'ADMIN')
                OR is_admin IS TRUE
              )
              AND email IS NOT NULL
              AND TRIM(email) <> ''
            """
        ),
        {"org_id": str(org_id)},
    ).fetchall()
    out: List[str] = []
    seen = set()
    for r in rows:
        addr = str(r[0] or "").strip().lower()
        if addr and addr not in seen and _EMAIL_RE.match(addr):
            seen.add(addr)
            out.append(addr)
    return out


def resolve_recipients(
    db: Session,
    org_id: uuid.UUID,
    cfg: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Resolve digest recipients from settings (custom list or org admins)."""
    if cfg is None:
        org = db.query(Organization).filter(Organization.id == org_id).first()
        cfg = get_funnel_lead_settings(org)

    mode = str(cfg.get("recipient_mode") or "admins").strip().lower()
    if mode == "custom":
        recipients = [
            str(e).strip().lower()
            for e in (cfg.get("recipients") or [])
            if isinstance(e, str) and _EMAIL_RE.match(e.strip())
        ]
        # Dedup preserving order
        seen = set()
        cleaned: List[str] = []
        for e in recipients:
            if e not in seen:
                seen.add(e)
                cleaned.append(e)
        if cleaned:
            return cleaned
        # Empty custom list falls back to admins
    return _admin_emails(db, org_id)


def render_digest(
    rows: Sequence[FunnelLeadNotification],
    org_name: str,
) -> Tuple[str, str]:
    """Build subject + HTML for a lead digest."""
    n = len(rows)
    if n == 1:
        subject = f"1 new lead from your funnels — {org_name}"
    else:
        subject = f"{n} new leads from your funnels — {org_name}"

    frontend = (getattr(settings, "FRONTEND_URL", None) or "http://localhost:3002").rstrip("/")
    pipeline_url = f"{frontend}/?tab=pipeline"

    shown = list(rows[:DIGEST_ROW_CAP])
    overflow = n - len(shown)

    def esc(v: Optional[str]) -> str:
        return html.escape(v or "—")

    body_rows = []
    for r in shown:
        kind = "New" if r.is_new_client else "Returning"
        body_rows.append(
            "<tr>"
            f"<td style='padding:8px;border-bottom:1px solid #e5e7eb;'>{esc(r.lead_name)}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e5e7eb;'>{esc(r.lead_email)}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e5e7eb;'>{esc(r.lead_phone)}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e5e7eb;'>{esc(r.lead_instagram)}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e5e7eb;'>{esc(r.funnel_name)}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e5e7eb;'>{esc(r.source)}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #e5e7eb;'>{kind}</td>"
            "</tr>"
        )

    footer = ""
    if overflow > 0:
        footer = (
            f"<p style='color:#6b7280;font-size:13px;'>"
            f"…and {overflow} more lead{'s' if overflow != 1 else ''} in this window."
            f"</p>"
        )

    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: #111827;">
      <p>You have <strong>{n}</strong> new lead{'s' if n != 1 else ''} for
         <strong>{html.escape(org_name)}</strong> from your funnel integrations.</p>
      <table style="border-collapse:collapse;width:100%;font-size:14px;margin:16px 0;">
        <thead>
          <tr style="background:#f3f4f6;text-align:left;">
            <th style="padding:8px;">Name</th>
            <th style="padding:8px;">Email</th>
            <th style="padding:8px;">Phone</th>
            <th style="padding:8px;">Instagram</th>
            <th style="padding:8px;">Funnel</th>
            <th style="padding:8px;">Source</th>
            <th style="padding:8px;">Type</th>
          </tr>
        </thead>
        <tbody>
          {''.join(body_rows)}
        </tbody>
      </table>
      {footer}
      <p><a href="{html.escape(pipeline_url)}">Open the pipeline board</a></p>
      <p style="color:#9ca3af;font-size:12px;">Sent by Sweep OS. Manage recipients in Settings → Notifications.</p>
    </div>
    """
    return subject, html_body


def _backoff_seconds(attempts: int) -> int:
    # Match automation_dispatcher: 30s, 2m, 8m, 30m, 2h capped
    base = 30
    return min(int(base * (4 ** max(0, attempts - 1))), 2 * 60 * 60)


def _due_org_ids(db: Session, *, max_orgs: int = 10) -> List[uuid.UUID]:
    """
    Orgs whose oldest ready unsent row is past their configured window.
    Skips rows with next_attempt_at in the future.
    """
    default_window = _default_window_minutes()
    sql = text(
        """
        WITH unsent AS (
            SELECT
                fln.org_id,
                MIN(fln.created_at) AS oldest_at
            FROM funnel_lead_notifications fln
            WHERE fln.sent_at IS NULL
              AND (fln.next_attempt_at IS NULL OR fln.next_attempt_at <= now())
            GROUP BY fln.org_id
        )
        SELECT u.org_id
        FROM unsent u
        JOIN organizations o ON o.id = u.org_id
        WHERE u.oldest_at <= now() - (
            COALESCE(
                NULLIF(
                    (o.notification_settings -> 'funnel_leads' ->> 'window_minutes')::int,
                    0
                ),
                :default_window
            ) * INTERVAL '1 minute'
        )
        ORDER BY u.oldest_at ASC
        LIMIT :max_orgs
        """
    )
    rows = db.execute(
        sql, {"default_window": default_window, "max_orgs": max_orgs}
    ).fetchall()
    out: List[uuid.UUID] = []
    for r in rows:
        try:
            out.append(uuid.UUID(str(r[0])))
        except (TypeError, ValueError):
            continue
    return out


def _claim_unsent_for_org(db: Session, org_id: uuid.UUID) -> List[FunnelLeadNotification]:
    """Atomically claim all currently sendable unsent rows for an org."""
    claim_sql = text(
        """
        WITH due AS (
            SELECT id FROM funnel_lead_notifications
            WHERE org_id = :org_id
              AND sent_at IS NULL
              AND (next_attempt_at IS NULL OR next_attempt_at <= now())
            ORDER BY created_at ASC
            FOR UPDATE SKIP LOCKED
        )
        UPDATE funnel_lead_notifications fln
        SET attempts = fln.attempts + 1,
            updated_at = now()
        FROM due
        WHERE fln.id = due.id
        RETURNING fln.id
        """
    )
    ids = [row[0] for row in db.execute(claim_sql, {"org_id": str(org_id)}).fetchall()]
    db.commit()
    if not ids:
        return []
    rows = (
        db.query(FunnelLeadNotification)
        .filter(FunnelLeadNotification.id.in_(ids))
        .order_by(FunnelLeadNotification.created_at.asc())
        .all()
    )
    return rows


def _mark_sent(
    db: Session,
    rows: Sequence[FunnelLeadNotification],
    *,
    error_text: Optional[str] = None,
) -> None:
    now = datetime.utcnow()
    for r in rows:
        r.sent_at = now
        r.error_text = (error_text[:2000] if error_text else None)
        r.updated_at = now
        r.next_attempt_at = None
    db.commit()


def _schedule_retry(
    db: Session,
    rows: Sequence[FunnelLeadNotification],
    error_text: str,
) -> None:
    now = datetime.utcnow()
    for r in rows:
        if r.attempts >= MAX_ATTEMPTS:
            r.sent_at = now
            r.error_text = f"gave up after {r.attempts} attempts: {error_text}"[:2000]
            r.next_attempt_at = None
        else:
            delay = _backoff_seconds(r.attempts)
            r.next_attempt_at = now + timedelta(seconds=delay)
            r.error_text = error_text[:2000]
        r.updated_at = now
    db.commit()


def _brevo_configured() -> bool:
    key = getattr(settings, "BREVO_API_KEY", None)
    return bool(key and str(key).strip())


def flush_due_funnel_lead_digests(db: Session, *, max_orgs: int = 10) -> int:
    """Flush due org digests. Returns number of orgs attempted."""
    # Alias used by worker
    return flush_due_digests(db, max_orgs=max_orgs)


def flush_due_digests(db: Session, *, max_orgs: int = 10) -> int:
    """Worker entry point: send digests for orgs whose window has elapsed."""
    if not table_exists(db, "funnel_lead_notifications"):
        if not ensure_funnel_lead_notifications_schema(db):
            return 0
    try:
        org_ids = _due_org_ids(db, max_orgs=max_orgs)
    except Exception:
        LOG.exception("failed to list due funnel-lead digest orgs")
        db.rollback()
        return 0

    attempted = 0
    for org_id in org_ids:
        try:
            _flush_one_org(db, org_id)
            attempted += 1
        except Exception:
            LOG.exception("funnel lead digest flush failed for org %s", org_id)
            try:
                db.rollback()
            except Exception:
                pass
    return attempted


def _flush_one_org(db: Session, org_id: uuid.UUID) -> None:
    org = db.query(Organization).filter(Organization.id == org_id).first()
    cfg = get_funnel_lead_settings(org)
    if not cfg.get("enabled", True):
        # Drain queue so disabled orgs don't pile up forever
        rows = _claim_unsent_for_org(db, org_id)
        if rows:
            _mark_sent(db, rows, error_text="notifications disabled; drained")
        return

    rows = _claim_unsent_for_org(db, org_id)
    if not rows:
        return

    try:
        if not cfg.get("include_returning_leads", True):
            # Filter claimed set; mark returning as drained without emailing
            keep: List[FunnelLeadNotification] = []
            drop: List[FunnelLeadNotification] = []
            for r in rows:
                if r.is_new_client:
                    keep.append(r)
                else:
                    drop.append(r)
            if drop:
                _mark_sent(db, drop, error_text="returning leads excluded by settings")
            rows = keep
            if not rows:
                return

        recipients = resolve_recipients(db, org_id, cfg)
        org_name = (org.name if org else None) or "your organization"
        subject, html_body = render_digest(rows, org_name)

        if not recipients:
            _mark_sent(db, rows, error_text="no recipients configured")
            return
        if not _brevo_configured():
            _mark_sent(db, rows, error_text="BREVO_API_KEY not configured")
            return

        ok_any = False
        errors: List[str] = []
        for addr in recipients:
            try:
                sent = send_onboarding_email(addr, subject, html_body)
                if sent:
                    ok_any = True
                else:
                    errors.append(f"{addr}: send returned False")
            except Exception as e:  # noqa: BLE001
                errors.append(f"{addr}: {e}")

        if ok_any:
            note = None
            if errors:
                note = "partial: " + "; ".join(errors)
            _mark_sent(db, rows, error_text=note)
        else:
            _schedule_retry(db, rows, "; ".join(errors) or "all sends failed")
    except Exception as e:  # noqa: BLE001 - never leave claimed rows without backoff
        LOG.exception("unexpected error flushing funnel lead digest for org %s", org_id)
        try:
            db.rollback()
            # Re-load claimed rows after rollback
            ids = [r.id for r in rows]
            rows = (
                db.query(FunnelLeadNotification)
                .filter(FunnelLeadNotification.id.in_(ids), FunnelLeadNotification.sent_at.is_(None))
                .all()
            )
            if rows:
                _schedule_retry(db, rows, f"unhandled: {e}")
        except Exception:
            LOG.exception("failed to schedule retry after flush error for org %s", org_id)


def send_test_digest(db: Session, org: Organization) -> Dict[str, Any]:
    """Send a sample digest to resolved recipients so admins can verify config."""
    cfg = get_funnel_lead_settings(org)
    recipients = resolve_recipients(db, org.id, cfg)
    if not recipients:
        return {"success": False, "message": "No recipients configured", "recipients": []}
    if not _brevo_configured():
        return {
            "success": False,
            "message": "BREVO_API_KEY is not configured on the server",
            "recipients": recipients,
        }

    now = datetime.utcnow()
    fake_rows = [
        FunnelLeadNotification(
            id=uuid.uuid4(),
            org_id=org.id,
            lead_name="Alex Example",
            lead_email="alex@example.com",
            lead_phone="+1 555 0100",
            lead_instagram="@alex",
            funnel_name="Sample Funnel",
            source="form",
            is_new_client=True,
            created_at=now,
            updated_at=now,
            attempts=0,
        ),
        FunnelLeadNotification(
            id=uuid.uuid4(),
            org_id=org.id,
            lead_name="Jordan Returning",
            lead_email="jordan@example.com",
            lead_phone="+1 555 0101",
            lead_instagram="@jordan",
            funnel_name="Sample Funnel",
            source="quiz",
            is_new_client=False,
            created_at=now,
            updated_at=now,
            attempts=0,
        ),
    ]
    subject, html_body = render_digest(fake_rows, org.name or "your organization")
    subject = f"[Test] {subject}"

    sent_to: List[str] = []
    failed: List[str] = []
    for addr in recipients:
        if send_onboarding_email(addr, subject, html_body):
            sent_to.append(addr)
        else:
            failed.append(addr)

    return {
        "success": len(sent_to) > 0,
        "message": (
            f"Test digest sent to {len(sent_to)} recipient(s)"
            if sent_to
            else "Failed to send test digest"
        ),
        "recipients": sent_to,
        "failed": failed,
    }
