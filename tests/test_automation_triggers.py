"""Tests for automation trigger entry points."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.models.automation import (
    AutomationEmailJob,
    AutomationRule,
    JobState,
    Playbook,
)
from app.models.client import Client
from app.models.client_call_insight import ClientCallInsight
from app.models.stripe_payment import StripePayment
from app.models.whop_payment import WhopPayment
from app.services import automation_engine


def _make_client(*, lifecycle="active", lifetime_cents=0):
    return SimpleNamespace(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        first_name="Sam",
        last_name="Lee",
        email="sam@example.com",
        lifecycle_state=SimpleNamespace(value=lifecycle),
        lifetime_revenue_cents=lifetime_cents,
        program_progress_percent=None,
        notes="",
    )


def _make_rule(playbook, *, enabled=True, audience_filter=None, require_approval=False, delay=0):
    return SimpleNamespace(
        id=uuid.uuid4(),
        playbook=playbook,
        enabled=enabled,
        audience_filter=audience_filter,
        require_approval=require_approval,
        approval_ttl_hours=None,
        delay_seconds=delay,
        opportunity_priority=None,
        combine_top_n=1,
        content_mode="ai_generated",
        subject_template=None,
        html_template_ref=None,
        ai_content_system_prompt=None,
    )


class _DbStub:
    """Minimal Session double — supports the paths these triggers exercise."""

    def __init__(
        self,
        *,
        client=None,
        rules=None,
        insight=None,
        stripe_count=0,
        whop_count=0,
        onboarding_job_exists=False,
    ):
        self.client = client
        self.rules = rules or {}
        self.insight = insight
        self.stripe_count = stripe_count
        self.whop_count = whop_count
        self.onboarding_job_exists = onboarding_job_exists
        self.flushed = False

    def query(self, model):
        if model is Client:
            return _Filter(self.client)
        if model is AutomationRule:
            # filter by org + playbook -> first
            return _RulesFilter(self.rules)
        # production uses ``query(AutomationEmailJob.id)`` — model is InstrumentedAttribute
        if model is AutomationEmailJob or getattr(model, "class_", None) is AutomationEmailJob:
            return _AutomationEmailJobExistingFilter(self.onboarding_job_exists)
        if model is ClientCallInsight:
            return _Filter(self.insight)
        # _is_first_succeeded_payment counts via func.count(StripePayment.id) etc.
        # The wrapper is a Query; we only need .filter().scalar() to work.
        if isinstance(model, type) and getattr(model, "__name__", "") == "function":
            return _ScalarStub(self.stripe_count)
        return _ScalarStub(0)

    def execute(self, _stmt):
        # Simulate a successful insert returning a UUID row.
        new_id = uuid.uuid4()
        return SimpleNamespace(fetchone=lambda: (new_id,))

    def flush(self):
        self.flushed = True

    def commit(self):
        return None

    def add(self, *_a, **_kw):
        return None


class _Filter:
    def __init__(self, value):
        self.value = value

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self.value


class _RulesFilter:
    def __init__(self, rules_by_playbook):
        self.rules_by_playbook = rules_by_playbook
        self.last_filter_args = None

    def filter(self, *args, **kwargs):
        # Try to spot the playbook string in the BinaryExpression args.
        for a in args:
            try:
                right = getattr(a, "right", None)
                if right is not None and getattr(right, "value", None) in self.rules_by_playbook:
                    self.last_filter_args = right.value
            except Exception:
                pass
        return self

    def first(self):
        return self.rules_by_playbook.get(self.last_filter_args)


class _AutomationEmailJobExistingFilter:
    """Supports onboarding dedupe query: query(Job.id).filter(...).limit(1).first()."""

    def __init__(self, has_row: bool):
        self.has_row = has_row

    def filter(self, *_a, **_kw):
        return self

    def limit(self, *_a, **_kw):
        return self

    def first(self):
        return (uuid.uuid4(),) if self.has_row else None


class _ScalarStub:
    def __init__(self, value):
        self.value = value

    def filter(self, *_a, **_kw):
        return self

    def scalar(self):
        return self.value


# ---------------------------------------------------------------------------
# Helpers: stub `_is_first_succeeded_payment` and `_enqueue_job` for these tests.
# ---------------------------------------------------------------------------

@pytest.fixture
def captured_enqueues(monkeypatch):
    captured = []

    def _fake_enqueue(db, **kwargs):
        captured.append(kwargs)
        return uuid.uuid4()

    monkeypatch.setattr(automation_engine, "_enqueue_job", _fake_enqueue)
    return captured


@pytest.fixture
def force_first_payment(monkeypatch):
    monkeypatch.setattr(automation_engine, "_is_first_succeeded_payment", lambda *a, **kw: True)


@pytest.fixture
def force_not_first_payment(monkeypatch):
    monkeypatch.setattr(automation_engine, "_is_first_succeeded_payment", lambda *a, **kw: False)


# ---------------------------------------------------------------------------
# on_payment_received
# ---------------------------------------------------------------------------

def test_on_payment_received_first_payment_enqueues_two_jobs(captured_enqueues, force_first_payment):
    client = _make_client(lifecycle="active")
    rules = {
        Playbook.FIRST_PAYMENT_ONBOARDING.value: _make_rule(Playbook.FIRST_PAYMENT_ONBOARDING.value),
        Playbook.FIRST_PAYMENT_REFERRAL.value: _make_rule(Playbook.FIRST_PAYMENT_REFERRAL.value, delay=3600),
    }
    db = _DbStub(client=client, rules=rules)

    ids = automation_engine.on_payment_received(
        db,
        org_id=client.org_id,
        client_id=client.id,
        payment_source="stripe",
        payment_external_id="ch_test_1",
        amount_cents=12_500,
    )
    assert len(ids) == 2
    assert len(captured_enqueues) == 2
    playbooks = {c["rule"].playbook for c in captured_enqueues}
    assert playbooks == {
        Playbook.FIRST_PAYMENT_ONBOARDING.value,
        Playbook.FIRST_PAYMENT_REFERRAL.value,
    }
    for c in captured_enqueues:
        assert c["org_id"] == client.org_id
        assert c["client_id"] == client.id
        assert c["payload"]["payment_external_id"] == "ch_test_1"
        assert c["payload"]["amount_cents"] == 12_500
        if c["rule"].playbook == Playbook.FIRST_PAYMENT_ONBOARDING.value:
            assert c["discriminator"] == f"lifetime_once:{client.id}"
        else:
            assert c["discriminator"] == "stripe:ch_test_1"


def test_on_payment_received_skips_duplicate_onboarding_when_job_exists(
    captured_enqueues,
    force_first_payment,
):
    """Exactly one onboarding automation per client: second payment path must not re-queue welcome."""
    client = _make_client(lifecycle="active")
    rules = {
        Playbook.FIRST_PAYMENT_ONBOARDING.value: _make_rule(Playbook.FIRST_PAYMENT_ONBOARDING.value),
        Playbook.FIRST_PAYMENT_REFERRAL.value: _make_rule(Playbook.FIRST_PAYMENT_REFERRAL.value, delay=3600),
    }
    db = _DbStub(client=client, rules=rules, onboarding_job_exists=True)
    ids = automation_engine.on_payment_received(
        db,
        org_id=client.org_id,
        client_id=client.id,
        payment_source="stripe",
        payment_external_id="ch_once",
        amount_cents=99_00,
    )
    assert Playbook.FIRST_PAYMENT_ONBOARDING.value not in {c["rule"].playbook for c in captured_enqueues}
    # Referral can still enqueue independently
    assert Playbook.FIRST_PAYMENT_REFERRAL.value in {c["rule"].playbook for c in captured_enqueues}
    assert len(ids) == 1


def test_on_payment_received_not_first_does_not_enqueue(captured_enqueues, force_not_first_payment):
    client = _make_client()
    rules = {
        Playbook.FIRST_PAYMENT_ONBOARDING.value: _make_rule(Playbook.FIRST_PAYMENT_ONBOARDING.value),
        Playbook.FIRST_PAYMENT_REFERRAL.value: _make_rule(Playbook.FIRST_PAYMENT_REFERRAL.value),
    }
    db = _DbStub(client=client, rules=rules)
    ids = automation_engine.on_payment_received(
        db,
        org_id=client.org_id,
        client_id=client.id,
        payment_source="whop",
        payment_external_id="pay_2",
        amount_cents=5_000,
    )
    assert ids == []
    assert captured_enqueues == []


def test_on_payment_received_audience_filter_blocks_onboarding(
    captured_enqueues, force_first_payment
):
    client = _make_client(lifecycle="cold_lead")
    rules = {
        Playbook.FIRST_PAYMENT_ONBOARDING.value: _make_rule(
            Playbook.FIRST_PAYMENT_ONBOARDING.value,
            audience_filter={"lifecycle_in": ["active"]},
        ),
        Playbook.FIRST_PAYMENT_REFERRAL.value: _make_rule(Playbook.FIRST_PAYMENT_REFERRAL.value),
    }
    db = _DbStub(client=client, rules=rules)
    ids = automation_engine.on_payment_received(
        db,
        org_id=client.org_id,
        client_id=client.id,
        payment_source="stripe",
        payment_external_id="ch_2",
        amount_cents=1_000,
    )
    # Onboarding skipped; referral has no filter so it still fires.
    playbooks = [c["rule"].playbook for c in captured_enqueues]
    assert Playbook.FIRST_PAYMENT_ONBOARDING.value not in playbooks
    assert Playbook.FIRST_PAYMENT_REFERRAL.value in playbooks


def test_on_payment_received_skips_when_client_already_has_lifetime_revenue(
    captured_enqueues, force_first_payment
):
    """Cross-source revenue gate: a client with prior revenue isn't actually new."""
    client = _make_client(lifetime_cents=20_000)  # $200 already on file
    rules = {
        Playbook.FIRST_PAYMENT_ONBOARDING.value: _make_rule(Playbook.FIRST_PAYMENT_ONBOARDING.value),
        Playbook.FIRST_PAYMENT_REFERRAL.value: _make_rule(Playbook.FIRST_PAYMENT_REFERRAL.value),
    }
    db = _DbStub(client=client, rules=rules)
    ids = automation_engine.on_payment_received(
        db,
        org_id=client.org_id,
        client_id=client.id,
        payment_source="stripe",
        payment_external_id="ch_lifetime_check",
        amount_cents=5_000,  # $50 -- way less than the $200 already on file
    )
    assert ids == []
    assert captured_enqueues == []


def test_on_payment_received_allows_when_lifetime_revenue_equals_this_payment(
    captured_enqueues, force_first_payment
):
    """Equal lifetime_revenue == this payment is the normal post-write state -- still fires."""
    client = _make_client(lifetime_cents=5_000)  # webhook handler bumps lifetime BEFORE engine fires
    rules = {
        Playbook.FIRST_PAYMENT_ONBOARDING.value: _make_rule(Playbook.FIRST_PAYMENT_ONBOARDING.value),
    }
    db = _DbStub(client=client, rules=rules)
    ids = automation_engine.on_payment_received(
        db,
        org_id=client.org_id,
        client_id=client.id,
        payment_source="stripe",
        payment_external_id="ch_equal",
        amount_cents=5_000,
    )
    assert len(ids) == 1


def test_on_payment_received_skips_when_payment_too_old_for_backfill(
    captured_enqueues, force_first_payment
):
    """Backfill recency gate: payments older than FIRST_PAYMENT_RECENCY_HOURS never fire."""
    client = _make_client()
    rules = {
        Playbook.FIRST_PAYMENT_ONBOARDING.value: _make_rule(Playbook.FIRST_PAYMENT_ONBOARDING.value),
        Playbook.FIRST_PAYMENT_REFERRAL.value: _make_rule(Playbook.FIRST_PAYMENT_REFERRAL.value),
    }
    db = _DbStub(client=client, rules=rules)
    ancient_paid_at = datetime.utcnow() - timedelta(
        hours=automation_engine.FIRST_PAYMENT_RECENCY_HOURS + 24
    )
    ids = automation_engine.on_payment_received(
        db,
        org_id=client.org_id,
        client_id=client.id,
        payment_source="whop",
        payment_external_id="pay_old",
        amount_cents=10_000,
        paid_at=ancient_paid_at,
    )
    assert ids == []
    assert captured_enqueues == []


def test_on_payment_received_allows_recent_payment(captured_enqueues, force_first_payment):
    """Payment made within the recency window passes the gate."""
    client = _make_client()
    rules = {
        Playbook.FIRST_PAYMENT_ONBOARDING.value: _make_rule(Playbook.FIRST_PAYMENT_ONBOARDING.value),
    }
    db = _DbStub(client=client, rules=rules)
    recent = datetime.utcnow() - timedelta(minutes=5)
    ids = automation_engine.on_payment_received(
        db,
        org_id=client.org_id,
        client_id=client.id,
        payment_source="stripe",
        payment_external_id="ch_fresh",
        amount_cents=10_000,
        paid_at=recent,
    )
    assert len(ids) == 1


def test_on_payment_received_when_client_missing(captured_enqueues):
    db = _DbStub(client=None, rules={})
    ids = automation_engine.on_payment_received(
        db,
        org_id=uuid.uuid4(),
        client_id=uuid.uuid4(),
        payment_source="stripe",
        payment_external_id="ch_x",
        amount_cents=10,
    )
    assert ids == []


# ---------------------------------------------------------------------------
# on_call_insight_processed
# ---------------------------------------------------------------------------

def test_on_call_insight_processed_enqueues_when_win_signal(captured_enqueues):
    client = _make_client()
    insight_id = uuid.uuid4()
    insight = SimpleNamespace(
        id=insight_id,
        status="complete",
        fathom_call_record_id=uuid.uuid4(),
        insight_json={
            "opportunity_tags": ["referral"],
            "wins": ["lost 12 lbs"],
        },
    )
    rules = {
        Playbook.WIN_COMBINED_ASK.value: _make_rule(
            Playbook.WIN_COMBINED_ASK.value, require_approval=True
        ),
    }
    db = _DbStub(client=client, rules=rules, insight=insight)

    ids = automation_engine.on_call_insight_processed(
        db, org_id=client.org_id, client_id=client.id, insight_id=insight_id
    )
    assert len(ids) == 1
    assert len(captured_enqueues) == 1
    payload = captured_enqueues[0]["payload"]
    assert payload["trigger"] == "win_detected"
    assert payload["insight_id"] == str(insight_id)
    assert "referral" in payload["opportunity_tags"]


def test_on_call_insight_processed_skips_without_signal(captured_enqueues):
    client = _make_client()
    insight = SimpleNamespace(
        id=uuid.uuid4(),
        status="complete",
        fathom_call_record_id=uuid.uuid4(),
        insight_json={"opportunity_tags": [], "wins": []},
    )
    rules = {Playbook.WIN_COMBINED_ASK.value: _make_rule(Playbook.WIN_COMBINED_ASK.value)}
    db = _DbStub(client=client, rules=rules, insight=insight)
    ids = automation_engine.on_call_insight_processed(
        db, org_id=client.org_id, client_id=client.id, insight_id=insight.id
    )
    assert ids == []
    assert captured_enqueues == []


def test_on_call_insight_processed_skips_when_rule_disabled(captured_enqueues):
    client = _make_client()
    insight = SimpleNamespace(
        id=uuid.uuid4(),
        status="complete",
        fathom_call_record_id=uuid.uuid4(),
        insight_json={"opportunity_tags": ["referral"], "wins": ["x"]},
    )
    rules = {
        Playbook.WIN_COMBINED_ASK.value: _make_rule(
            Playbook.WIN_COMBINED_ASK.value, enabled=False
        ),
    }
    db = _DbStub(client=client, rules=rules, insight=insight)
    ids = automation_engine.on_call_insight_processed(
        db, org_id=client.org_id, client_id=client.id, insight_id=insight.id
    )
    assert ids == []


def test_on_call_insight_processed_skips_when_insight_incomplete(captured_enqueues):
    client = _make_client()
    insight = SimpleNamespace(
        id=uuid.uuid4(),
        status="processing",
        fathom_call_record_id=uuid.uuid4(),
        insight_json={"opportunity_tags": ["referral"], "wins": ["x"]},
    )
    rules = {Playbook.WIN_COMBINED_ASK.value: _make_rule(Playbook.WIN_COMBINED_ASK.value)}
    db = _DbStub(client=client, rules=rules, insight=insight)
    ids = automation_engine.on_call_insight_processed(
        db, org_id=client.org_id, client_id=client.id, insight_id=insight.id
    )
    assert ids == []


# ---------------------------------------------------------------------------
# on_lifecycle_entered_offboarding
# ---------------------------------------------------------------------------

def test_on_lifecycle_offboarding_enqueues_recap(captured_enqueues):
    client = _make_client(lifecycle="offboarding")
    rules = {
        Playbook.OFFBOARDING_RECAP_ASK.value: _make_rule(
            Playbook.OFFBOARDING_RECAP_ASK.value,
            audience_filter={"lifecycle_in": ["offboarding"]},
            require_approval=True,
        )
    }
    db = _DbStub(client=client, rules=rules)
    ids = automation_engine.on_lifecycle_entered_offboarding(
        db, org_id=client.org_id, client_id=client.id
    )
    assert len(ids) == 1
    assert len(captured_enqueues) == 1
    payload = captured_enqueues[0]["payload"]
    assert payload["trigger"] == "lifecycle.offboarding"
    assert captured_enqueues[0]["discriminator"].startswith("offboarding:")


def test_on_lifecycle_offboarding_skips_when_rule_missing(captured_enqueues):
    client = _make_client(lifecycle="offboarding")
    db = _DbStub(client=client, rules={})
    ids = automation_engine.on_lifecycle_entered_offboarding(
        db, org_id=client.org_id, client_id=client.id
    )
    assert ids == []
    assert captured_enqueues == []


# ---------------------------------------------------------------------------
# Hooks must NOT call LLM or Brevo. (Critical for the async guarantee.)
# ---------------------------------------------------------------------------

def test_trigger_path_does_not_call_llm_or_brevo(captured_enqueues, force_first_payment):
    """The whole point of the worker model: the request path enqueues, never sends.
    If a future refactor accidentally re-introduces a synchronous LLM/Brevo call from
    the trigger path, this test will catch it.
    """
    client = _make_client()
    rules = {
        Playbook.FIRST_PAYMENT_ONBOARDING.value: _make_rule(Playbook.FIRST_PAYMENT_ONBOARDING.value),
    }
    db = _DbStub(client=client, rules=rules)

    with patch("app.services.brevo_client.send_email") as send_mail, patch(
        "app.services.llm_client.chat_json"
    ) as chat_json:
        automation_engine.on_payment_received(
            db,
            org_id=client.org_id,
            client_id=client.id,
            payment_source="stripe",
            payment_external_id="ch_async_check",
            amount_cents=10,
        )
        send_mail.assert_not_called()
        chat_json.assert_not_called()
