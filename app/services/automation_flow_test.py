"""Sync flow-test sends + diagnostics for Automations.

Used by ``POST /automations/flows/{flow}/test`` so operators can prove Brevo +
rules work without waiting on the worker, delays, or live booking/payment events.
"""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.automation import (
    AutomationEmailJob,
    AutomationRule,
    FLOW_VALUES,
    JobState,
    NodeKind,
    ScheduleMode,
)
from app.models.client import Client
from app.services.automation_dispatcher import read_dispatcher_health
from app.services.automation_drafts import resolve_sender_for_org
from app.services.automation_engine import seed_default_rules
from app.services.brevo_client import (
    BrevoNotConnectedError,
    BrevoSendError,
    get_brevo_auth_headers,
    send_email,
)

LOG = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_MAX_TEST_SENDS = 5


def _normalize_email(raw: str) -> str:
    return (raw or "").strip().lower()


def diagnose_flow(db: Session, org_id: uuid.UUID, flow: str) -> Dict[str, Any]:
    """Cheap readiness snapshot for one flow (worker, Brevo, enabled steps, booking config)."""
    seed_default_rules(db, org_id)
    health = read_dispatcher_health(db)

    brevo_connected = False
    brevo_note: Optional[str] = None
    try:
        get_brevo_auth_headers(db, org_id)
        brevo_connected = True
    except BrevoNotConnectedError as e:
        brevo_note = str(e)
    except Exception as e:  # noqa: BLE001
        brevo_note = f"Brevo check failed: {e}"

    rules = (
        db.query(AutomationRule)
        .filter(AutomationRule.org_id == org_id, AutomationRule.flow == flow)
        .order_by(AutomationRule.step_index.asc(), AutomationRule.created_at.asc())
        .all()
    )
    enabled_actions = [
        r
        for r in rules
        if r.enabled
        and (getattr(r, "node_kind", None) or NodeKind.ACTION.value) != NodeKind.WAIT.value
    ]
    enabled_waits = [
        r
        for r in rules
        if r.enabled and (getattr(r, "node_kind", None) or "") == NodeKind.WAIT.value
    ]

    booking_ready = True
    booking_note: Optional[str] = None
    if flow == "post_booking":
        booking_ready = False
        booking_note = (
            "No booking trigger configured — open the Booking lands trigger and select "
            "sales event types (or match all sales). Non-sales bookings never fire."
        )
        for r in rules:
            cfg = r.trigger_config if isinstance(r.trigger_config, dict) else None
            if not cfg:
                continue
            if cfg.get("match_all_events") or (cfg.get("event_type_ids") or []):
                booking_ready = True
                booking_note = None
                break

    blockers: List[str] = []
    if not health.get("healthy"):
        blockers.append(
            "Worker heartbeat is stale or missing — jobs will not send until the Render "
            "Background Worker is running (`python -m app.worker`)."
        )
    if not brevo_connected:
        blockers.append(
            f"Brevo is not connected for this org ({brevo_note or 'reconnect in Integrations'})."
        )
    if not enabled_actions:
        blockers.append("No enabled email/action steps in this flow — turn a step On.")
    if flow == "post_booking" and not booking_ready:
        blockers.append(booking_note or "Booking trigger is not configured.")

    approval_gated = [r.playbook for r in enabled_actions if r.require_approval]

    return {
        "flow": flow,
        "dispatcher_healthy": bool(health.get("healthy")),
        "dispatcher": health,
        "brevo_connected": brevo_connected,
        "brevo_note": brevo_note,
        "step_count": len(rules),
        "enabled_action_count": len(enabled_actions),
        "enabled_wait_count": len(enabled_waits),
        "enabled_playbooks": [r.playbook for r in enabled_actions],
        "approval_gated_playbooks": approval_gated,
        "booking_trigger_ready": booking_ready,
        "booking_trigger_note": booking_note,
        "blockers": blockers,
        "ready_for_live_sends": len(blockers) == 0,
    }


def run_flow_test(
    db: Session,
    *,
    org_id: uuid.UUID,
    flow: str,
    email: str,
    client_id: Optional[uuid.UUID] = None,
    trigger_kind: Optional[str] = None,
    user_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """Send lightweight [TEST] emails for enabled action steps in a flow (sync, no worker)."""
    if flow not in FLOW_VALUES:
        raise ValueError(f"unknown flow '{flow}'")

    to_email = _normalize_email(email)
    if not _EMAIL_RE.match(to_email):
        raise ValueError("invalid test email")

    diagnostics = diagnose_flow(db, org_id, flow)

    client: Optional[Client] = None
    if client_id is not None:
        client = (
            db.query(Client)
            .filter(Client.id == client_id, Client.org_id == org_id)
            .first()
        )
        if not client:
            raise ValueError("client not found")
    else:
        client = (
            db.query(Client)
            .filter(Client.org_id == org_id, Client.email.isnot(None))
            .order_by(Client.updated_at.desc().nullslast())
            .first()
        )

    first_name = (client.first_name if client else None) or "there"
    client_label = None
    if client:
        client_label = (
            f"{(client.first_name or '')} {(client.last_name or '')}".strip() or client.email
        )

    try:
        headers = get_brevo_auth_headers(db, org_id, user_id=user_id)
        sender = resolve_sender_for_org(db, org_id)
    except BrevoNotConnectedError as e:
        return {
            **diagnostics,
            "ok": False,
            "to_email": to_email,
            "client_id": str(client.id) if client else None,
            "client_label": client_label,
            "sent_count": 0,
            "results": [],
            "error": f"Brevo not connected: {e}",
        }

    rules = (
        db.query(AutomationRule)
        .filter(AutomationRule.org_id == org_id, AutomationRule.flow == flow)
        .order_by(
            AutomationRule.trigger_kind.asc(),
            AutomationRule.step_index.asc(),
            AutomationRule.created_at.asc(),
        )
        .all()
    )
    if trigger_kind:
        rules = [r for r in rules if (r.trigger_kind or "") == trigger_kind]

    results: List[Dict[str, Any]] = []
    sent = 0
    now = datetime.utcnow()
    failed = False

    for rule in rules:
        kind = (getattr(rule, "node_kind", None) or NodeKind.ACTION.value).strip().lower()
        entry: Dict[str, Any] = {
            "playbook": rule.playbook,
            "node_kind": kind,
            "trigger_kind": rule.trigger_kind,
            "step_index": rule.step_index,
            "enabled": bool(rule.enabled),
            "status": "skipped",
            "detail": None,
            "brevo_message_id": None,
            "job_id": None,
        }

        if kind == NodeKind.WAIT.value:
            entry["detail"] = "Wait node — skipped in test send"
            results.append(entry)
            continue
        if not rule.enabled:
            entry["detail"] = "Step is Off"
            results.append(entry)
            continue
        if sent >= _MAX_TEST_SENDS:
            entry["detail"] = f"Cap of {_MAX_TEST_SENDS} test emails reached"
            results.append(entry)
            continue

        subject_base = (rule.subject_template or rule.playbook or "Automation step").strip()
        subject_base = subject_base.replace("{{first_name}}", first_name).replace(
            "{{ first_name }}", first_name
        )
        subject = f"[TEST] {subject_base}"[:200]
        schedule = rule.schedule_mode or ScheduleMode.AFTER_TRIGGER.value
        html = (
            "<div style='font-family:system-ui,sans-serif;line-height:1.5'>"
            "<p><strong>Sweep automation test</strong></p>"
            f"<p>Flow: <code>{flow}</code><br/>"
            f"Step: <code>{rule.playbook}</code> "
            f"(trigger={rule.trigger_kind or '—'}, schedule={schedule})</p>"
            "<p>This confirms Brevo delivery for your org. Live sends still need an enabled "
            "worker and a real trigger (booking / payment / win / offboarding).</p>"
            f"<p style='color:#666;font-size:12px'>Sent at {now.isoformat()}Z</p>"
            "</div>"
        )
        text = (
            f"Sweep automation test\nFlow: {flow}\nStep: {rule.playbook}\n"
            "Live sends still require the worker + a real trigger.\n"
        )

        try:
            resp = send_email(
                headers=headers,
                sender=sender,
                to=[{"email": to_email, "name": first_name}],
                subject=subject,
                html_content=html,
                text_content=text,
                tags=["sweep-automation-test", flow, (rule.playbook or "")[:40]],
            )
            message_id = None
            if isinstance(resp, dict):
                message_id = resp.get("messageId") or resp.get("message_id")

            # Only persist a Send-log row when we have a real client (FK).
            job_id = None
            if client is not None:
                job = AutomationEmailJob(
                    id=uuid.uuid4(),
                    org_id=org_id,
                    rule_id=rule.id,
                    client_id=client.id,
                    playbook=rule.playbook,
                    trigger_event="flow.test",
                    idempotency_key=(
                        f"test:{org_id}:{flow}:{rule.playbook}:{uuid.uuid4().hex[:12]}"
                    ),
                    scheduled_at=now,
                    state=JobState.SENT.value,
                    payload_json={
                        "test": True,
                        "flow": flow,
                        "to_email": to_email,
                        "client_id": str(client.id),
                    },
                    attempts=1,
                    last_attempt_at=now,
                    dispatched_at=now,
                    brevo_message_id=str(message_id) if message_id else None,
                )
                db.add(job)
                db.flush()
                job_id = str(job.id)

            entry["status"] = "sent"
            entry["detail"] = subject
            entry["brevo_message_id"] = str(message_id) if message_id else None
            entry["job_id"] = job_id
            sent += 1
        except BrevoSendError as e:
            failed = True
            entry["status"] = "failed"
            entry["detail"] = f"Brevo send error: {e}"
            LOG.warning("flow test send failed org=%s playbook=%s: %s", org_id, rule.playbook, e)
        except Exception as e:  # noqa: BLE001
            failed = True
            entry["status"] = "failed"
            entry["detail"] = f"Send failed: {e}"
            LOG.exception("flow test unexpected error org=%s playbook=%s", org_id, rule.playbook)

        results.append(entry)

    db.commit()

    ok = sent > 0 and not failed
    error = None
    if sent == 0:
        if diagnostics.get("enabled_action_count", 0) == 0:
            error = "No emails sent — enable at least one action step, or fix Brevo."
        else:
            error = "No emails sent — see step results."
    elif failed:
        error = "Some test sends failed — see step results."

    return {
        **diagnostics,
        "ok": ok,
        "to_email": to_email,
        "client_id": str(client.id) if client else None,
        "client_label": client_label,
        "sent_count": sent,
        "results": results,
        "error": error,
    }


def assert_trigger_wiring() -> Dict[str, List[str]]:
    """Static check that prod call sites still reference engine entrypoints."""
    import importlib
    import inspect

    expected = {
        "app.api.calendar_webhooks": ["on_booking_created_pre_sale"],
        "app.services.checkin_sync": ["on_booking_created_pre_sale"],
        "app.services.stripe_processor": ["on_payment_received"],
        "app.services.whop_sync": ["on_payment_received"],
        "app.services.call_insight_service": ["on_call_insight_processed"],
        "app.services.client_automation": ["on_lifecycle_entered_offboarding"],
    }
    missing: Dict[str, List[str]] = {}
    for mod_name, symbols in expected.items():
        mod = importlib.import_module(mod_name)
        src = inspect.getsource(mod)
        bad = [s for s in symbols if s not in src]
        if bad:
            missing[mod_name] = bad
    return missing
