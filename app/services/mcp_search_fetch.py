"""
`search` / `fetch` MCP tools in the shape ChatGPT expects for connectors and deep research.

search(query) -> {"results": [{"id", "title", "url"}]}
fetch(id)     -> {"id", "title", "text", "url", "metadata"}

Ids are typed so fetch can route them: client:<uuid>, funnel:<uuid>,
doc:<resource_id>, library:<uuid>. Everything is scoped to the token's org.
"""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.config import settings

_TEXT_CAP = 60_000


def _frontend(query: str) -> str:
    base = str(getattr(settings, "FRONTEND_URL", "") or "http://localhost:3003").rstrip("/")
    return f"{base}/?{query}"


# Only funnels have a per-record deep link in the app; clients/docs open their tab.
def _url(kind: str, key: str) -> str:
    if kind == "funnel":
        return _frontend(f"tab=funnels&funnelId={key}")
    if kind == "client":
        return _frontend("tab=clients")
    return _frontend("tab=resources")


def search_for_mcp(db: Session, org_id: uuid.UUID, query: str, limit: int = 20) -> Dict[str, Any]:
    from app.models.funnel import Funnel
    from app.services.client_profile_bundle import list_clients_for_mcp
    from app.services.resource_documents import ensure_resource_documents_table, search_resource_docs
    from app.services.resource_library import ensure_resource_library_table, list_library_items

    q = (query or "").strip()
    if not q:
        return {"results": []}
    results: List[Dict[str, str]] = []

    for c in list_clients_for_mcp(db, org_id, query=q, limit=10):
        name = " ".join(p for p in (c.get("first_name"), c.get("last_name")) if p) or c.get("email") or "Client"
        stage = c.get("lifecycle_state") or ""
        results.append(
            {
                "id": f"client:{c['client_id']}",
                "title": f"Client: {name}" + (f" ({stage})" if stage else ""),
                "url": _url("client", c["client_id"]),
            }
        )

    for fid, fname in (
        db.query(Funnel.id, Funnel.name)
        .filter(Funnel.org_id == org_id, Funnel.name.ilike(f"%{q}%"))
        .limit(10)
        .all()
    ):
        results.append({"id": f"funnel:{fid}", "title": f"Funnel: {fname}", "url": _url("funnel", str(fid))})

    ensure_resource_documents_table(db)
    for m in search_resource_docs(db, org_id, query=q, limit=8, include_content=False).get("matches", []):
        rid = str(m.get("resource_id") or "")
        if rid:
            results.append({"id": f"doc:{rid}", "title": f"SOP: {m.get('title') or rid}", "url": _url("doc", rid)})

    ensure_resource_library_table(db)
    ql = q.lower()
    for item in list_library_items(db, org_id):
        hay = f"{item.get('title') or ''} {item.get('description') or ''} {' '.join(item.get('tags') or [])}".lower()
        if ql in hay:
            iid = str(item.get("id"))
            results.append(
                {"id": f"library:{iid}", "title": f"Resource: {item.get('title') or iid}", "url": _url("library", iid)}
            )

    return {"results": results[: max(1, min(limit, 50))]}


def _as_text(payload: Any) -> str:
    text = payload if isinstance(payload, str) else json.dumps(payload, default=str, indent=1)
    return text if len(text) <= _TEXT_CAP else text[:_TEXT_CAP] + "\n…[truncated]"


def fetch_for_mcp(
    db: Session,
    org_id: uuid.UUID,
    doc_id: str,
    *,
    user_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    kind, _, key = (doc_id or "").partition(":")
    key = key.strip()
    if not key:
        return {"error": "id must look like client:<uuid>, funnel:<uuid>, doc:<resource_id>, or library:<uuid>"}

    if kind == "client":
        from app.services.client_profile_bundle import build_client_profile_bundle

        try:
            bundle = build_client_profile_bundle(db, org_id, uuid.UUID(key))
        except ValueError:
            bundle = None
        if not bundle:
            return {"error": "client not found"}
        contact = bundle.get("contact") or {}
        name = " ".join(p for p in (contact.get("first_name"), contact.get("last_name")) if p) or contact.get("email")
        return {
            "id": doc_id,
            "title": f"Client: {name or key}",
            "text": _as_text(bundle),
            "url": _url("client", key),
            "metadata": {"type": "client", "lifecycle_state": (bundle.get("pipeline") or {}).get("lifecycle_state")},
        }

    if kind == "funnel":
        from app.mcp.server import _run_funnel_team_tool

        try:
            uuid.UUID(key)
        except ValueError:
            return {"error": "invalid funnel id"}
        raw = _run_funnel_team_tool("get_funnel_analytics", {"funnel_id": key}, org_id, db, user_id=user_id)
        payload = json.loads(raw["content"][0]["text"])
        if "error" in payload:
            return payload
        name = (payload.get("funnel") or {}).get("name") or key
        return {
            "id": doc_id,
            "title": f"Funnel: {name}",
            "text": _as_text(payload),
            "url": _url("funnel", key),
            "metadata": {"type": "funnel", "window_days": 30},
        }

    if kind == "doc":
        from app.services.resource_documents import ensure_doc_content, ensure_resource_documents_table, get_doc

        ensure_resource_documents_table(db)
        doc = get_doc(db, org_id, key)
        if not doc:
            return {"error": "resource doc not found"}
        doc = ensure_doc_content(doc)
        # Video SOPs carry little markdown, so lead with the description and links.
        videos = doc.get("video_urls") or ([doc["video_url"]] if doc.get("video_url") else [])
        parts = [str(doc.get("description") or "").strip(), *(f"Video: {v}" for v in videos), str(doc.get("content") or "").strip()]
        return {
            "id": doc_id,
            "title": f"SOP: {doc.get('title') or key}",
            "text": _as_text("\n\n".join(p for p in parts if p)),
            "url": _url("doc", key),
            "metadata": {"type": "doc", "category": doc.get("category"), "sop_category": doc.get("sop_category")},
        }

    if kind == "library":
        from app.services.resource_library import ensure_resource_library_table, get_library_item

        ensure_resource_library_table(db)
        try:
            item = get_library_item(db, org_id, uuid.UUID(key))
        except ValueError:
            item = None
        if not item:
            return {"error": "resource library item not found"}
        body = item.get("content_text") or item.get("content_url") or item.get("description") or ""
        return {
            "id": doc_id,
            "title": f"Resource: {item.get('title') or key}",
            "text": _as_text(str(body)),
            "url": item.get("content_url") or _url("library", key),
            "metadata": {"type": "library", "kind": item.get("kind"), "tags": item.get("tags") or []},
        }

    return {"error": f"unknown id type '{kind}'"}
