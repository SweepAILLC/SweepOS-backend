"""Resolve preview thumbnails for embedded SOP videos.

Loom does not expose a stable public thumbnail CDN pattern, so we ask Loom's
oEmbed endpoint for the real thumbnail and cache the answer in-process.
Only loom.com share/embed URLs are ever fetched (the id is validated and the
oEmbed URL is rebuilt from it), so callers cannot point this at arbitrary hosts.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

_log = logging.getLogger(__name__)

_LOOM_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_OEMBED_URL = "https://www.loom.com/v1/oembed"
_HIT_TTL_SEC = 24 * 3600
_MISS_TTL_SEC = 10 * 60
_MAX_URLS = 40

_cache: Dict[str, Tuple[float, Optional[str]]] = {}
_lock = threading.Lock()


def loom_video_id(raw: str) -> Optional[str]:
    try:
        url = urlparse(raw.strip())
    except Exception:
        return None
    if url.scheme not in ("http", "https"):
        return None
    host = (url.hostname or "").lower().removeprefix("www.")
    if host != "loom.com":
        return None
    parts = [p for p in url.path.split("/") if p]
    if len(parts) >= 2 and parts[0] in ("share", "embed"):
        vid = parts[1].lower()
        return vid if _LOOM_ID_RE.match(vid) else None
    return None


def _cached(video_id: str) -> Tuple[bool, Optional[str]]:
    with _lock:
        entry = _cache.get(video_id)
    if entry and entry[0] > time.time():
        return True, entry[1]
    return False, None


def _store(video_id: str, thumb: Optional[str]) -> None:
    ttl = _HIT_TTL_SEC if thumb else _MISS_TTL_SEC
    with _lock:
        _cache[video_id] = (time.time() + ttl, thumb)


def _fetch_loom_thumbnail(video_id: str) -> Optional[str]:
    hit, value = _cached(video_id)
    if hit:
        return value
    thumb: Optional[str] = None
    try:
        resp = httpx.get(
            _OEMBED_URL,
            params={"url": f"https://www.loom.com/share/{video_id}", "format": "json"},
            timeout=4.0,
        )
        if resp.status_code == 200:
            candidate = resp.json().get("thumbnail_url")
            if isinstance(candidate, str) and candidate.startswith("https://"):
                thumb = candidate
    except Exception as e:
        _log.info("loom oembed failed for %s: %s", video_id, e)
    _store(video_id, thumb)
    return thumb


def resolve_thumbnails(urls: List[str]) -> Dict[str, Optional[str]]:
    """Map each requested URL to a thumbnail URL (or None). Non-Loom URLs map to None."""
    unique = list(dict.fromkeys(u for u in urls if isinstance(u, str) and u.strip()))[:_MAX_URLS]
    by_url: Dict[str, Optional[str]] = {u: None for u in unique}
    ids = {u: loom_video_id(u) for u in unique}
    pending = sorted({vid for vid in ids.values() if vid})
    if pending:
        with ThreadPoolExecutor(max_workers=min(8, len(pending))) as pool:
            results = dict(zip(pending, pool.map(_fetch_loom_thumbnail, pending)))
        for u, vid in ids.items():
            if vid:
                by_url[u] = results.get(vid)
    return by_url
