"""Sanitize free-text and structured content before sending to external LLMs (prompt injection + size limits)."""
from __future__ import annotations

import re
from typing import Any, Optional


# Remove null bytes and excessive control characters that can confuse parsers or hide payloads
_CTRL_EXCEPT_NEWLINE_TAB = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def sanitize_llm_text(text: Optional[str], max_length: int) -> str:
    if not text:
        return ""
    s = str(text)
    s = _CTRL_EXCEPT_NEWLINE_TAB.sub("", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    if len(s) > max_length:
        s = s[: max_length - 3] + "..."
    return s


def sanitize_llm_user_payload(system_prompt: str, user_prompt: str, max_total: int) -> tuple[str, str]:
    """Cap combined size; trim user portion first (system prompt is trusted/smaller)."""
    sys_s = sanitize_llm_text(system_prompt, max_total // 4)
    remaining = max(0, max_total - len(sys_s))
    usr_s = sanitize_llm_text(user_prompt, remaining)
    return sys_s, usr_s
