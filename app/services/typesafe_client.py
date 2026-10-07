"""
Jev (TypeSafe System One) client: typed Choice / Score / Noul questions against a text state.

Jev answers judgments our code consumes (tags, ratings, yes/no gates) with probabilities and
a calibrated confidence — it never generates text, so prose stays on llm_client. Raw httpx
like llm_client (the official SDK pins a newer pydantic than this app). Every call:

- strips identity (names, emails, phones, handles) — TypeSafe needs none of it to answer
- sanitizes and caps the state
- takes a Jev slot (separate from the LLM generation slots)
- logs usage to llm_usage_event with provider "typesafe" and the question-set version

Callers catch JevUnavailableError and fall back to their existing LLM path.
"""
from __future__ import annotations

import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional

import httpx

from app.core.config import settings
from app.core.prompt_security import sanitize_llm_text

logger = logging.getLogger(__name__)

PROVIDER = "typesafe"
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504, 529})


class JevUnavailableError(RuntimeError):
    """Jev could not answer (no key, slot timeout, network, rate limit, bad response)."""


# --------------------------------------------------------------------------- questions


def noul(instructions: str, *, true: Optional[str] = None, false: Optional[str] = None) -> Dict[str, Any]:
    """Yes/no question; answer is P(yes). `true`/`false` describe where the boundary sits."""
    q: Dict[str, Any] = {"type": "noul", "instructions": instructions}
    if true is not None or false is not None:
        q["criteria"] = {k: v for k, v in (("true", true), ("false", false)) if v is not None}
    return q


def choice(instructions: str, options: Mapping[str, Optional[str]]) -> Dict[str, Any]:
    """Pick one option. Order matters a little (first-option bias): prefer Nouls when it would."""
    if len(options) < 2:
        raise ValueError("choice needs at least two options")
    return {"type": "choice", "instructions": instructions, "criteria": dict(options)}


def score(instructions: str, levels: Iterable[str]) -> Dict[str, Any]:
    """Rate on 2-10 ordered, situational levels, lowest first."""
    lv = list(levels)
    if not 2 <= len(lv) <= 10:
        raise ValueError("score needs 2-10 levels")
    return {"type": "score", "instructions": instructions, "criteria": lv}


# --------------------------------------------------------------------------- answers


@dataclass
class JevAnswer:
    kind: str  # noul | choice | score
    value: Any  # P(yes) for noul, option name for choice, weighted level for score
    confidence: float
    probabilities: Dict[str, float] = field(default_factory=dict)


@dataclass
class JevResult:
    answers: Dict[str, JevAnswer]
    model: str
    input_tokens: int
    request_id: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        """Answers + probabilities only — safe to persist (never contains the state)."""
        return {
            "model": self.model,
            "answers": {
                k: {"kind": a.kind, "value": a.value, "confidence": a.confidence, "probabilities": a.probabilities}
                for k, a in self.answers.items()
            },
        }


def noul_confidence(p: float) -> float:
    """TypeSafe's recommended confidence for a Noul: 0 at p=0.5, 1 at p=0 or 1."""
    return round(abs(2.0 * float(p) - 1.0), 6)


def _parse_answer(raw: Mapping[str, Any]) -> JevAnswer:
    kind = raw.get("type")
    probs = {str(k): float(v) for k, v in (raw.get("probabilities") or {}).items()}
    if kind == "noul":
        p = float(raw["noul"])
        return JevAnswer("noul", p, noul_confidence(p), {"yes": p, "no": round(1.0 - p, 6)})
    if kind == "choice":
        return JevAnswer("choice", str(raw["choice"]), float(raw["confidence"]), probs)
    if kind == "score":
        return JevAnswer("score", float(raw["score"]), float(raw["confidence"]), probs)
    raise ValueError(f"unknown answer type {kind!r}")


# --------------------------------------------------------------------------- identity


@dataclass
class Identity:
    """People who may be named in the state. Names are replaced by role, never sent."""

    client_names: Iterable[str] = ()
    coach_names: Iterable[str] = ()
    other_terms: Iterable[str] = ()  # e.g. business names, Instagram handles from the CRM


_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE_CANDIDATE = re.compile(r"(?<![\w$])\+?\d[\d\s().-]{8,}\d(?!\w)")
_DATE_LIKE = re.compile(r"^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}$|^\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}$")
_HANDLE = re.compile(r"(?<![\w@])@[A-Za-z0-9_.]{2,30}")


def _phone_sub(m: re.Match) -> str:
    s = m.group(0)
    digits = sum(c.isdigit() for c in s)
    # Phone numbers carry 10-15 digits; dates, prices and years stay readable for the model.
    if 10 <= digits <= 15 and not _DATE_LIKE.match(s.strip()):
        return "[phone]"
    return s


def _name_variants(name: str) -> Iterable[str]:
    full = " ".join((name or "").split())
    if len(full) < 2:
        return []
    # Single parts under 3 letters ("Jo", "Al") collide with too many words to replace safely.
    parts = [p for p in full.split(" ") if len(p) >= 3]
    return sorted({full, *parts}, key=len, reverse=True)  # longest first: "Jane Doe" before "Jane"


def redact_identity(text: str, identity: Optional[Identity] = None) -> str:
    """Replace contact details with placeholders and known people with their role.

    Names match case-sensitively, so a client named "Will" doesn't turn "I will" into
    "I Client". A capitalized homonym ("Will you…") is still replaced: over-redacting
    a word costs less than sending a name.
    """
    out = _EMAIL.sub("[email]", text or "")
    out = _PHONE_CANDIDATE.sub(_phone_sub, out)
    out = _HANDLE.sub("[handle]", out)
    if identity is None:
        return out
    replacements = []
    for names, role in ((identity.client_names, "Client"), (identity.coach_names, "Coach")):
        for n in names or ():
            replacements.extend((v, role) for v in _name_variants(n))
    replacements.extend((t.strip(), "[redacted]") for t in identity.other_terms or () if t and len(t.strip()) >= 3)
    for term, role in sorted(replacements, key=lambda r: len(r[0]), reverse=True):
        out = re.sub(rf"(?<!\w){re.escape(term)}(?!\w)", role, out)
    return out


# --------------------------------------------------------------------------- concurrency

_sem: Optional[threading.BoundedSemaphore] = None
_sem_lock = threading.Lock()


def _semaphore() -> threading.BoundedSemaphore:
    global _sem
    with _sem_lock:
        if _sem is None:
            _sem = threading.BoundedSemaphore(max(1, int(settings.JEV_MAX_INFLIGHT or 8)))
        return _sem


# --------------------------------------------------------------------------- call


def jev_available() -> bool:
    return bool((settings.JEV_API_KEY or "").strip())


def feature_mode(feature_setting: str) -> str:
    """Normalized off | shadow | on for a JEV_*_MODE setting name; unknown values read as off."""
    mode = str(getattr(settings, feature_setting, "off") or "off").strip().lower()
    return mode if mode in ("off", "shadow", "on") else "off"


def _retry_delay(attempt: int, response: Optional[httpx.Response]) -> float:
    if response is not None:
        ra = response.headers.get("retry-after")
        try:
            if ra is not None:
                return min(float(ra), 10.0)
        except ValueError:
            pass
    return min(0.5 * (2 ** attempt), 8.0)


def _post(payload: Dict[str, Any]) -> httpx.Response:
    url = settings.JEV_BASE_URL.rstrip("/") + "/systemone"
    headers = {"Authorization": f"Bearer {settings.JEV_API_KEY}", "Content-Type": "application/json"}
    attempts = max(1, int(settings.JEV_MAX_RETRIES or 0) + 1)
    last: Optional[BaseException] = None
    for attempt in range(attempts):
        response: Optional[httpx.Response] = None
        try:
            with httpx.Client(timeout=float(settings.JEV_TIMEOUT_SEC)) as client:
                response = client.post(url, json=payload, headers=headers)
            if response.status_code not in _RETRYABLE_STATUS:
                return response
            last = httpx.HTTPStatusError(f"Jev HTTP {response.status_code}", request=response.request, response=response)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            last = e
        if attempt < attempts - 1:
            time.sleep(_retry_delay(attempt, response))
    raise JevUnavailableError(f"Jev request failed after {attempts} attempts: {type(last).__name__}") from last


def ask(
    state: str,
    questions: Mapping[str, Mapping[str, Any]],
    *,
    org_id: Optional[uuid.UUID],
    feature: str,
    question_set_version: str,
    identity: Identity,
) -> JevResult:
    """Evaluate `questions` against `state` in one request.

    `identity` is required so every caller decides who gets redacted; pass `Identity()`
    only when the state names no one. Raises JevUnavailableError on any failure; callers
    fall back to their LLM path.
    """
    if not isinstance(identity, Identity):
        raise TypeError("ask() needs an Identity; pass Identity() if the state names no one")
    if not jev_available():
        raise JevUnavailableError("JEV_API_KEY not configured")
    if not questions:
        raise ValueError("ask() needs at least one question")

    clean = sanitize_llm_text(redact_identity(state, identity), int(settings.JEV_MAX_STATE_CHARS))
    payload = {"model": settings.JEV_MODEL, "state": clean, "questions": {k: dict(v) for k, v in questions.items()}}

    sem = _semaphore()
    if not sem.acquire(timeout=float(settings.JEV_SLOT_WAIT_SEC)):
        raise JevUnavailableError("Jev slot wait timed out")
    try:
        response = _post(payload)
    finally:
        sem.release()

    if response.status_code != 200:
        # 401/403/422 are our bug or config, not transient. Never log the body: a validation
        # error can echo the state back. The request id is enough for TypeSafe to trace it.
        logger.warning(
            "Jev HTTP %s feature=%s request_id=%s",
            response.status_code,
            feature,
            response.headers.get("x-typesafe-request-id"),
        )
        raise JevUnavailableError(f"Jev HTTP {response.status_code}")

    try:
        body = response.json()
        answers = {k: _parse_answer(v) for k, v in (body.get("answers") or {}).items()}
    except (ValueError, KeyError, TypeError) as e:
        raise JevUnavailableError("Jev response could not be parsed") from e
    missing = set(questions) - set(answers)
    if missing:
        raise JevUnavailableError(f"Jev response missing answers: {sorted(missing)}")

    usage = body.get("usage") or {}
    input_tokens = int(usage.get("input_tokens") or 0)
    model = str(body.get("model") or settings.JEV_MODEL)
    _record_usage(org_id, model, feature, question_set_version, input_tokens)
    return JevResult(
        answers=answers,
        model=model,
        input_tokens=input_tokens,
        request_id=response.headers.get("x-typesafe-request-id"),
    )


def _record_usage(org_id: Optional[uuid.UUID], model: str, feature: str, version: str, input_tokens: int) -> None:
    try:
        from app.services.llm_usage import record_llm_usage

        record_llm_usage(
            org_id=org_id,
            provider=PROVIDER,
            model=model,
            feature=feature,
            prompt_version=version,
            prompt_tokens=input_tokens,
            completion_tokens=0,  # Jev bills input only
            total_tokens=input_tokens,
        )
    except Exception:
        logger.exception("jev usage hook failed")
