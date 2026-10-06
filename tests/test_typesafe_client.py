"""Jev (TypeSafe) shared client: requests, parsing, retries, fallback contract, redaction, usage."""
import json
import uuid

import httpx
import pytest

from app.core.config import settings
from app.services import typesafe_client as ts

ORG = uuid.uuid4()

OK_BODY = {
    "model": "jev-1.13.0",
    "answers": {
        "billing": {"type": "noul", "noul": 0.9},
        "tone": {"type": "choice", "choice": "frustrated", "probabilities": {"calm": 0.1, "frustrated": 0.8, "angry": 0.1}, "confidence": 0.7},
        "urgency": {"type": "score", "score": 1.6, "legend": {"0": "can wait", "1": "this week", "2": "today"}, "probabilities": {"0": 0.1, "1": 0.2, "2": 0.7}, "confidence": 0.55},
    },
    "usage": {"input_tokens": 296, "output_tokens": 20},
}

QUESTIONS = {
    "billing": ts.noul("Is this ticket about billing?"),
    "tone": ts.choice("What is the customer's tone?", {"calm": None, "frustrated": None, "angry": None}),
    "urgency": ts.score("How urgent is this ticket?", ["can wait", "this week", "today"]),
}


@pytest.fixture(autouse=True)
def jev_settings(monkeypatch):
    monkeypatch.setattr(settings, "JEV_API_KEY", "test-key", raising=False)
    monkeypatch.setattr(settings, "JEV_MAX_RETRIES", 2, raising=False)
    monkeypatch.setattr(ts.time, "sleep", lambda s: None)


@pytest.fixture
def usage_calls(monkeypatch):
    calls = []
    import app.services.llm_usage as llm_usage

    monkeypatch.setattr(llm_usage, "record_llm_usage", lambda **kw: calls.append(kw))
    return calls


def _transport(monkeypatch, responses):
    """Route httpx.Client through a MockTransport that plays `responses` in order."""
    seen = []
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    real_client = httpx.Client
    monkeypatch.setattr(ts.httpx, "Client", lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw))
    return seen


def _ask(**kw):
    return ts.ask("I was charged twice.", QUESTIONS, org_id=ORG, feature="test", question_set_version="jev-v1.0", **kw)


# ----------------------------------------------------------------------------- happy path


def test_parses_all_three_answer_types(monkeypatch, usage_calls):
    _transport(monkeypatch, [httpx.Response(200, json=OK_BODY, headers={"x-typesafe-request-id": "req_1"})])
    res = _ask()
    assert res.answers["billing"].value == 0.9
    assert res.answers["billing"].confidence == pytest.approx(0.8)  # |2p - 1|
    assert res.answers["tone"].value == "frustrated" and res.answers["tone"].confidence == 0.7
    assert res.answers["urgency"].value == 1.6 and res.answers["urgency"].probabilities["2"] == 0.7
    assert res.request_id == "req_1" and res.input_tokens == 296


def test_request_shape_and_auth(monkeypatch, usage_calls):
    seen = _transport(monkeypatch, [httpx.Response(200, json=OK_BODY)])
    _ask()
    req = seen[0]
    assert str(req.url).endswith("/v1/systemone")
    assert req.headers["authorization"] == "Bearer test-key"
    body = json.loads(req.content)
    assert body["model"] == settings.JEV_MODEL
    assert body["questions"]["tone"]["criteria"] == {"calm": None, "frustrated": None, "angry": None}
    assert body["questions"]["urgency"]["criteria"] == ["can wait", "this week", "today"]


def test_usage_logged_as_typesafe_input_only(monkeypatch, usage_calls):
    _transport(monkeypatch, [httpx.Response(200, json=OK_BODY)])
    _ask()
    (call,) = usage_calls
    assert call["provider"] == "typesafe"
    assert call["prompt_version"] == "jev-v1.0"
    assert call["prompt_tokens"] == 296 and call["completion_tokens"] == 0
    assert call["org_id"] == ORG


def test_to_json_never_contains_state(monkeypatch, usage_calls):
    _transport(monkeypatch, [httpx.Response(200, json=OK_BODY)])
    dumped = json.dumps(_ask().to_json())
    assert "charged twice" not in dumped


# ----------------------------------------------------------------------------- retries + fallback


def test_retries_429_then_succeeds(monkeypatch, usage_calls):
    seen = _transport(monkeypatch, [httpx.Response(429, headers={"retry-after": "1"}), httpx.Response(200, json=OK_BODY)])
    assert _ask().answers["billing"].value == 0.9
    assert len(seen) == 2


def test_overloaded_529_exhausts_retries(monkeypatch, usage_calls):
    seen = _transport(monkeypatch, [httpx.Response(529)] * 3)
    with pytest.raises(ts.JevUnavailableError):
        _ask()
    assert len(seen) == 3  # 1 try + JEV_MAX_RETRIES
    assert usage_calls == []


def test_timeout_raises_unavailable(monkeypatch, usage_calls):
    _transport(monkeypatch, [httpx.ReadTimeout("slow")] * 3)
    with pytest.raises(ts.JevUnavailableError):
        _ask()


def test_auth_error_is_not_retried(monkeypatch, usage_calls):
    seen = _transport(monkeypatch, [httpx.Response(401, json={"error": "bad key"})])
    with pytest.raises(ts.JevUnavailableError, match="401"):
        _ask()
    assert len(seen) == 1


def test_missing_answer_raises(monkeypatch, usage_calls):
    body = {**OK_BODY, "answers": {"billing": OK_BODY["answers"]["billing"]}}
    _transport(monkeypatch, [httpx.Response(200, json=body)])
    with pytest.raises(ts.JevUnavailableError, match="missing"):
        _ask()


def test_garbage_body_raises(monkeypatch, usage_calls):
    _transport(monkeypatch, [httpx.Response(200, content=b"not json")])
    with pytest.raises(ts.JevUnavailableError):
        _ask()


def test_no_key_raises_without_calling(monkeypatch, usage_calls):
    monkeypatch.setattr(settings, "JEV_API_KEY", None, raising=False)
    seen = _transport(monkeypatch, [])
    with pytest.raises(ts.JevUnavailableError):
        _ask()
    assert seen == []


# ----------------------------------------------------------------------------- redaction


def test_state_sent_is_redacted(monkeypatch, usage_calls):
    seen = _transport(monkeypatch, [httpx.Response(200, json=OK_BODY)])
    ts.ask(
        "Jane Doe (jane@example.com, +1 415-555-0134, @janefit) told Mark she lost 12 lbs on 2026-09-14.",
        QUESTIONS,
        org_id=ORG,
        feature="test",
        question_set_version="jev-v1.0",
        identity=ts.Identity(client_names=["Jane Doe"], coach_names=["Mark Ruiz"]),
    )
    sent = json.loads(seen[0].content)["state"]
    assert sent == "Client ([email], [phone], [handle]) told Coach she lost 12 lbs on 2026-09-14."


def test_redaction_keeps_numbers_dates_and_common_words():
    text = "Will said I will pay $4,997 on 10/05/2026, revenue 1500000, call 2026-10-05."
    out = ts.redact_identity(text, ts.Identity(client_names=["Will Smith"]))
    assert out == "Client said I will pay $4,997 on 10/05/2026, revenue 1500000, call 2026-10-05."


def test_redaction_longest_name_first_and_possessive():
    out = ts.redact_identity("Jane Doe's goal; Jane agreed.", ts.Identity(client_names=["Jane Doe"]))
    assert out == "Client's goal; Client agreed."


# ----------------------------------------------------------------------------- builders + modes


def test_question_builders_validate():
    with pytest.raises(ValueError):
        ts.choice("one option", {"only": None})
    with pytest.raises(ValueError):
        ts.score("one level", ["only"])
    assert ts.noul("q", true="yes case")["criteria"] == {"true": "yes case"}
    assert "criteria" not in ts.noul("q")


def test_feature_mode_normalizes(monkeypatch):
    monkeypatch.setattr(settings, "JEV_SENTIMENT_MODE", " Shadow ", raising=False)
    assert ts.feature_mode("JEV_SENTIMENT_MODE") == "shadow"
    monkeypatch.setattr(settings, "JEV_SENTIMENT_MODE", "maybe", raising=False)
    assert ts.feature_mode("JEV_SENTIMENT_MODE") == "off"


def test_jev_price_in_cost_table():
    from app.services.llm_usage import estimate_cost_usd

    assert estimate_cost_usd("jev-1.13.0", 1_000_000, 500) == pytest.approx(0.042)
