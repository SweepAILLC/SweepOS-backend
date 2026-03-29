"""
Minimal LLM HTTP client: Gemini (Google AI) or OpenAI chat completions.
Used for Fathom sentiment and AI health score. No extra deps beyond httpx.
"""
from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.core.config import settings
from app.core.llm_budget import consume_llm_budget
from app.core.prompt_security import sanitize_llm_user_payload


def _resolve_provider_and_key() -> Tuple[str, Optional[str]]:
    prov = (settings.LLM_PROVIDER or "auto").lower()
    model = (settings.HEALTH_SCORE_LLM_MODEL or "").lower()
    gk = settings.GOOGLE_API_KEY or settings.LLM_API_KEY
    ok = settings.OPENAI_API_KEY
    if prov == "gemini":
        return "gemini", gk
    if prov == "openai":
        return "openai", ok or settings.LLM_API_KEY
    # auto: model name hints (gpt → OpenAI; gemini → Google)
    if "gpt" in model and ok:
        return "openai", ok
    if "gemini" in model and gk:
        return "gemini", gk
    if settings.OPENAI_API_KEY:
        return "openai", settings.OPENAI_API_KEY
    if gk:
        return "gemini", gk
    return "none", None


def llm_available() -> bool:
    _, key = _resolve_provider_and_key()
    return bool(key)


def chat_json(
    system_prompt: str,
    user_prompt: str,
    *,
    temperature: float = 0.0,
    timeout: float = 60.0,
    org_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """
    Single-turn chat; request JSON object in response. Parses first JSON object from text.

    org_id: when set, enforces per-org LLM budget (drops call if over limit — callers should handle failure).
    """
    provider, api_key = _resolve_provider_and_key()
    if not api_key:
        raise RuntimeError("No LLM API key configured")

    if org_id is not None and not consume_llm_budget(org_id):
        raise RuntimeError("llm_budget_exceeded")

    max_total = getattr(settings, "LLM_MAX_INPUT_CHARS_TOTAL", 48000)
    system_prompt, user_prompt = sanitize_llm_user_payload(system_prompt, user_prompt, max_total)

    if provider == "gemini":
        return _gemini_generate_json(api_key, system_prompt, user_prompt, temperature, timeout)
    return _openai_chat_json(api_key, system_prompt, user_prompt, temperature, timeout)


def _gemini_generate_json(
    api_key: str,
    system: str,
    user: str,
    temperature: float,
    timeout: float,
) -> Dict[str, Any]:
    model = settings.HEALTH_SCORE_LLM_MODEL.strip()
    if not model.startswith("models/"):
        model_id = f"models/{model}"
    else:
        model_id = model
    url = f"https://generativelanguage.googleapis.com/v1beta/{model_id}:generateContent"
    body = {
        "systemInstruction": {"parts": [{"text": system}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": {
            "temperature": temperature,
            "responseMimeType": "application/json",
        },
    }
    data = _post_with_retries(
        lambda c: c.post(url, params={"key": api_key}, json=body),
        timeout=timeout,
    )
    text = ""
    try:
        parts = data["candidates"][0]["content"]["parts"]
        text = "".join(p.get("text", "") for p in parts)
    except (KeyError, IndexError, TypeError):
        text = json.dumps(data)
    return _parse_json_object(text)


def _openai_chat_json(
    api_key: str,
    system: str,
    user: str,
    temperature: float,
    timeout: float,
) -> Dict[str, Any]:
    model = settings.HEALTH_SCORE_LLM_MODEL.strip()
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body = {
        "model": model,
        "temperature": temperature,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    data = _post_with_retries(
        lambda c: c.post(url, headers=headers, json=body),
        timeout=timeout,
    )
    text = data["choices"][0]["message"]["content"] or "{}"
    return _parse_json_object(text)


def _parse_json_object(text: str) -> Dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    raise ValueError("LLM did not return valid JSON")


_RETRYABLE_STATUS = frozenset({429, 503, 502})


def _post_with_retries(post_fn, *, timeout: float, max_attempts: int = 3) -> Any:
    """POST with exponential backoff on transient errors (cost + provider stability)."""
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            with httpx.Client(timeout=timeout) as client:
                r = post_fn(client)
                if r.status_code in _RETRYABLE_STATUS and attempt < max_attempts - 1:
                    time.sleep(min(0.5 * (2 ** attempt), 8))
                    continue
                r.raise_for_status()
                return r.json()
        except httpx.TimeoutException as e:
            last_exc = e
            if attempt < max_attempts - 1:
                time.sleep(min(1.0 * (2 ** attempt), 10))
                continue
            raise RuntimeError(f"LLM request timed out after {timeout}s") from e
        except httpx.ConnectError as e:
            last_exc = e
            if attempt < max_attempts - 1:
                time.sleep(min(1.0 * (2 ** attempt), 10))
                continue
            raise RuntimeError("LLM provider connection failed") from e
        except httpx.HTTPStatusError as e:
            last_exc = e
            if e.response is not None and e.response.status_code in _RETRYABLE_STATUS and attempt < max_attempts - 1:
                time.sleep(min(0.5 * (2 ** attempt), 8))
                continue
            raise
    raise RuntimeError("LLM request exhausted retries") from last_exc


def truncate_for_tokens(text: Optional[str], max_chars: int = 12000) -> str:
    if not text:
        return ""
    t = text.strip()
    if len(t) <= max_chars:
        return t
    return t[: max_chars - 3] + "..."
