"""Deterministic theme bucketing for org-level sales/objection signals (v1: word-set hash)."""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, Optional, Tuple


def _word_core(text: str) -> str:
    s = (text or "").lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    words = sorted({w for w in s.split() if len(w) > 2})
    return " ".join(words[:32])


def theme_key_and_label_from_clip(clip: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """Return (theme_key, label) or None if no usable text."""
    quote = str(clip.get("quote") or "").strip()
    label = str(clip.get("label") or "").strip()
    rationale = str(clip.get("rationale") or "").strip()
    blob = quote or label or rationale
    if len(blob) < 8:
        return None
    core = _word_core(blob)
    if not core:
        h = hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]
        return h, (label or blob[:80]).strip()[:220]
    key = hashlib.sha256(core.encode("utf-8")).hexdigest()[:24]
    disp = label if label else quote[:80]
    return key, (disp or core[:80])[:220]


def theme_key_and_label_from_phrase(phrase: str) -> Optional[Tuple[str, str]]:
    p = str(phrase or "").strip()
    if len(p) < 4:
        return None
    core = _word_core(p)
    if not core:
        h = hashlib.sha256(p.encode("utf-8")).hexdigest()[:24]
        return h, p[:220]
    key = hashlib.sha256(core.encode("utf-8")).hexdigest()[:24]
    return key, p[:220]
