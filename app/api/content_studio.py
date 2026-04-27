"""Content Studio: playbook, auto-drafted content bundle from sales signals, transcript analysis."""

from __future__ import annotations

import json
import logging
import threading
import uuid
from typing import Any, Dict

import httpx
from sqlalchemy.exc import SQLAlchemyError

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, check_tab_access, get_db
from app.long_jobs import schedule_background_work
from app.core.rate_limit import check_sliding_window
from app.models.content_studio_transcript_analysis import ContentStudioTranscriptAnalysis
from app.models.user import User
from app.schemas.content_studio import (
    BootstrapResponse,
    CompletePatchBody,
    CompletePatchResponse,
    ContentSectionOut,
    ContentStudioBundleOut,
    KnowledgeOut,
    KnowledgePutBody,
    ReanalyzeResponse,
    SalesPlaybookOut,
    SectionIdeaOut,
    TranscriptAnalyzeBody,
    TranscriptAnalyzeResponse,
    TranscriptListItem,
    TranscriptListResponse,
    VoiceMarketingOut,
)
from app.services import content_studio_service as css
from app.services.content_studio_bundle import (
    BUNDLE_VERSION,
    compute_signals_fingerprint,
    default_bundle_placeholder,
    draft_content_studio_bundle_llm,
)
from app.services.content_studio_fathom_context import (
    build_sales_playbook_for_studio,
    collect_fathom_sales_signals,
)
from app.services.llm_client import llm_available
from app.models.client import Client
from app.services.health_score_cache_service import invalidate_health_score_cache

logger = logging.getLogger(__name__)
router = APIRouter()

_bundle_regen_locks: Dict[str, threading.Lock] = {}
_bundle_regen_meta_lock = threading.Lock()


def _require_content_studio_tab(
    db: Session,
    current_user: User,
) -> None:
    if not check_tab_access("content_studio", current_user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Marketing Intel is not enabled for your organization.",
        )


def _org_id(user: User) -> uuid.UUID:
    raw = getattr(user, "selected_org_id", None) or user.org_id
    return raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))


def _user_orm(db: Session, user: User) -> User:
    uid = user.id if isinstance(user.id, uuid.UUID) else uuid.UUID(str(user.id))
    row = db.query(User).filter(User.id == uid).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return row


def _parse_bundle_dict(raw: Dict[str, Any]) -> ContentStudioBundleOut:
    sections_out: list[ContentSectionOut] = []
    for sec in raw.get("sections") or []:
        if not isinstance(sec, dict):
            continue
        ideas_out: list[SectionIdeaOut] = []
        for idea in sec.get("ideas") or []:
            if not isinstance(idea, dict):
                continue
            st = idea.get("stage")
            if st not in css.STAGE_SET:
                st = "TOF"
            iid = str(idea.get("id") or "").strip()
            if not iid:
                continue
            ideas_out.append(
                SectionIdeaOut(
                    id=iid,
                    stage=st,
                    hook=str(idea.get("hook") or ""),
                    concept=str(idea.get("concept") or ""),
                    why_it_works=str(idea.get("why_it_works") or ""),
                    format=str(idea.get("format") or "reel"),
                )
            )
        sid = str(sec.get("id") or "").strip()
        if not sid:
            continue
        sections_out.append(
            ContentSectionOut(
                id=sid,
                title=str(sec.get("title") or ""),
                body=str(sec.get("body") or ""),
                ideas=ideas_out,
            )
        )
    vm = raw.get("voice_marketing") if isinstance(raw.get("voice_marketing"), dict) else {}
    src = raw.get("source")
    if src not in ("llm", "default", "fathom"):
        src = "llm"
    return ContentStudioBundleOut(
        version=int(raw.get("version") or BUNDLE_VERSION),
        signals_fingerprint=str(raw.get("signals_fingerprint") or ""),
        batch_id=str(raw.get("batch_id") or ""),
        generated_at=raw.get("generated_at") if isinstance(raw.get("generated_at"), str) else None,
        source=src,
        sections=sections_out,
        voice_marketing=VoiceMarketingOut(
            title=str(vm.get("title") or ""),
            body=str(vm.get("body") or ""),
            bullets=[str(b) for b in (vm.get("bullets") or []) if str(b).strip()],
        ),
    )


def _needs_bundle_regeneration(gen_row: Any, fingerprint: str) -> bool:
    if not gen_row or not gen_row.ideas_json:
        return True
    raw = gen_row.ideas_json
    if isinstance(raw, list):
        return True
    if not isinstance(raw, dict):
        return True
    if int(raw.get("version") or 0) < BUNDLE_VERSION:
        return True
    stored = str(raw.get("signals_fingerprint") or "")
    return stored != fingerprint


def _get_org_regen_lock(org_id: uuid.UUID) -> threading.Lock:
    key = str(org_id)
    with _bundle_regen_meta_lock:
        if key not in _bundle_regen_locks:
            _bundle_regen_locks[key] = threading.Lock()
        return _bundle_regen_locks[key]


def _regenerate_bundle_outside_session(
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    fingerprint: str,
) -> None:
    """Run LLM bundle generation with its own short-lived DB session so we don't hold connections."""
    from app.db.session import SessionLocal
    from app.services.user_ai_profile_context import extract_ai_profile_for_llm

    db2 = SessionLocal()
    try:
        signals = collect_fathom_sales_signals(db2, org_id)
        urow = db2.query(User).filter(User.id == user_id).first()
        bundle: Dict[str, Any] | None = None
        if llm_available() and urow:
            try:
                bundle = draft_content_studio_bundle_llm(db2, org_id, urow, signals, fingerprint)
            except Exception:
                logger.exception("Content studio bundle LLM failed for org %s", org_id)
                bundle = None
        if not bundle:
            bundle = default_bundle_placeholder(fingerprint)
        bid = uuid.UUID(str(bundle["batch_id"]))
        css.upsert_generation(db2, org_id, urow.id if urow else user_id, bid, bundle)
        if urow:
            css.set_user_content_studio_batch_and_completions(db2, urow, str(bid), [])
    except Exception:
        logger.exception("Content studio bundle regen failed for org %s", org_id)
    finally:
        db2.close()


@router.get("/bootstrap", response_model=BootstrapResponse)
def get_bootstrap(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_content_studio_tab(db, current_user)
    org_id = _org_id(current_user)
    knowledge_dict = css.load_knowledge_grouped(db, org_id)
    knowledge = KnowledgeOut(**knowledge_dict)
    sp_source, sp_paragraphs = build_sales_playbook_for_studio(db, org_id, use_llm_synthesis=False)
    sales_playbook = SalesPlaybookOut(source=sp_source, paragraphs=sp_paragraphs)

    fingerprint = compute_signals_fingerprint(db, org_id)
    gen_row = css.get_latest_generation_row(db, org_id)
    urow = _user_orm(db, current_user)

    # If underlying signals changed, trigger bundle regeneration in the background.
    # Do not block the request; return the last known bundle (if any) so the UI can
    # keep rendering while a fresh draft is generated.
    if _needs_bundle_regeneration(gen_row, fingerprint):
        lock = _get_org_regen_lock(org_id)
        if lock.acquire(blocking=False):
            try:
                schedule_background_work(
                    _regenerate_bundle_outside_session,
                    None,
                    org_id,
                    urow.id,
                    fingerprint,
                )
            finally:
                lock.release()
        # Do not wait on the lock here; another request already kicked off regen.
        # We intentionally keep using gen_row from before so callers see the previous bundle.

    content_bundle: ContentStudioBundleOut | None = None
    if gen_row and isinstance(gen_row.ideas_json, dict) and int(gen_row.ideas_json.get("version") or 0) >= BUNDLE_VERSION:
        content_bundle = _parse_bundle_dict(gen_row.ideas_json)

    prof = urow.ai_profile if isinstance(urow.ai_profile, dict) else {}
    bid_prof, completed = css.content_studio_state_from_profile(prof)
    row_batch = str(gen_row.batch_id) if gen_row else None

    return BootstrapResponse(
        knowledge=knowledge,
        sales_playbook=sales_playbook,
        content_bundle=content_bundle,
        completed_idea_ids=completed,
        batch_id=bid_prof or row_batch,
    )


@router.post("/reanalyze", response_model=ReanalyzeResponse)
def post_content_studio_reanalyze(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Rate-limited: pull Fathom meetings, queue call-insight + call-library follow-ups,
    invalidate client health caches (Intelligence / board), and regenerate the Content Studio
    bundle (all sections + voice/marketing) from fresh signals.
    """
    _require_content_studio_tab(db, current_user)
    org_id = _org_id(current_user)
    check_sliding_window(
        f"cs_reanalyze_{org_id}_{current_user.id}",
        max_requests=3,
        window_seconds=3600,
        endpoint_name="content_studio_reanalyze",
    )
    urow = _user_orm(db, current_user)

    from app.services.fathom_ingest import queue_fathom_sync_followups, sync_recent_meetings_for_org

    try:
        result = sync_recent_meetings_for_org(db, org_id, user=current_user)
    except httpx.HTTPStatusError as e:
        if e.response is not None and e.response.status_code == 401:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Fathom rejected the API key (401). Regenerate it at Fathom → Settings → API Access, "
                    "then paste it under Integrations (organization key), save, and sync again. "
                    "Alternatively set FATHOM_API_KEY in the server environment."
                ),
            ) from e
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Fathom API error ({e.response.status_code if e.response else 'unknown'}). Try again later.",
        ) from e
    except (RuntimeError, ValueError) as e:
        msg = str(e)
        logger.warning("content_studio reanalyze fathom sync: %s", msg)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=msg if msg else "Fathom sync could not run (configuration or API response).",
        ) from e
    except json.JSONDecodeError as e:
        logger.exception("content_studio reanalyze: JSON decode")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Fathom returned invalid JSON. Try again in a moment.",
        ) from e
    except SQLAlchemyError as e:
        logger.exception("content_studio reanalyze: database error during Fathom sync")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                "Database error while syncing Fathom. Run database migrations "
                "(`alembic upgrade head`) and ensure columns exist on `fathom_call_records`."
            ),
        ) from e
    except Exception as e:
        logger.exception("content_studio reanalyze: Fathom sync failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Fathom sync failed: {e!s}",
        ) from e

    if not isinstance(result, dict):
        result = {}

    queue_fathom_sync_followups(background_tasks, org_id, result)

    cids = [row[0] for row in db.query(Client.id).filter(Client.org_id == org_id).all()]
    for cid in cids:
        try:
            invalidate_health_score_cache(db, cid, org_id, do_commit=False)
        except Exception:
            logger.exception("content_studio reanalyze: invalidate health cache for client %s", cid)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("content_studio reanalyze: commit after health invalidation")

    fingerprint = compute_signals_fingerprint(db, org_id)
    lock = _get_org_regen_lock(org_id)
    if lock.acquire(blocking=False):
        try:
            t = threading.Thread(
                target=_regenerate_bundle_outside_session,
                args=(org_id, urow.id, fingerprint),
                daemon=True,
            )
            t.start()
        finally:
            lock.release()

    return ReanalyzeResponse(
        fathom_sync=result,
        bundle_regenerating=True,
        health_clients_invalidated=len(cids),
    )


@router.put("/knowledge", response_model=KnowledgeOut)
def put_knowledge(
    body: KnowledgePutBody,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_content_studio_tab(db, current_user)
    org_id = _org_id(current_user)
    css.replace_knowledge(
        db,
        org_id,
        body.objections,
        body.closing,
        body.reframes,
    )
    return KnowledgeOut(**css.load_knowledge_grouped(db, org_id))


@router.patch("/ideas/complete", response_model=CompletePatchResponse)
def patch_ideas_complete(
    body: CompletePatchBody,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_content_studio_tab(db, current_user)
    org_id = _org_id(current_user)
    urow = _user_orm(db, current_user)
    gen_row = css.get_latest_generation_row(db, org_id)
    if not gen_row:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Content bundle not loaded yet.",
        )
    valid = css.valid_idea_ids_from_ideas_json(gen_row.ideas_json)
    filtered = [x for x in body.completed_idea_ids if x in valid]
    batch_id = str(gen_row.batch_id)
    updated_at = css.set_user_content_studio_batch_and_completions(db, urow, batch_id, filtered)

    return CompletePatchResponse(
        completed_idea_ids=filtered,
        batch_id=batch_id,
        updated_at=updated_at,
    )


@router.post("/transcripts/analyze", response_model=TranscriptAnalyzeResponse)
def post_transcript_analyze(
    body: TranscriptAnalyzeBody,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_content_studio_tab(db, current_user)
    check_sliding_window(
        f"cs_analyze_{current_user.id}_{_org_id(current_user)}",
        max_requests=8,
        window_seconds=300,
        endpoint_name="content_studio_transcript_analyze",
    )
    if not llm_available():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="AI analysis is not configured (missing LLM API key).",
        )
    org_id = _org_id(current_user)
    urow = _user_orm(db, current_user)
    try:
        analysis = css.analyze_transcript_llm(
            db,
            org_id,
            body.transcript,
            body.purpose,
            body.mixed_note,
            urow,
        )
    except RuntimeError as e:
        if "budget" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="AI usage limit reached. Try again later.",
            ) from e
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e)[:200]) from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Analysis failed: {str(e)[:200]}",
        ) from e

    if not isinstance(analysis, dict):
        analysis = {"summary": "Unable to parse structured analysis.", "raw": str(analysis)[:2000]}

    rid = css.persist_transcript_analysis(
        db,
        org_id,
        urow.id,
        body.transcript,
        body.purpose,
        body.mixed_note,
        analysis,
    )
    return TranscriptAnalyzeResponse(id=str(rid), purpose=body.purpose, analysis=analysis)


@router.get("/transcripts", response_model=TranscriptListResponse)
def list_transcripts(
    limit: int = 20,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_content_studio_tab(db, current_user)
    org_id = _org_id(current_user)
    lim = max(1, min(limit, 50))
    rows = (
        db.query(ContentStudioTranscriptAnalysis)
        .filter(ContentStudioTranscriptAnalysis.org_id == org_id)
        .order_by(ContentStudioTranscriptAnalysis.created_at.desc())
        .limit(lim)
        .all()
    )
    items: list[TranscriptListItem] = []
    for r in rows:
        aj = r.analysis_json if isinstance(r.analysis_json, dict) else {}
        summ = aj.get("summary") if isinstance(aj.get("summary"), str) else None
        items.append(
            TranscriptListItem(
                id=str(r.id),
                purpose=r.purpose,
                mixed_note=r.mixed_note,
                created_at=r.created_at.isoformat() if r.created_at else None,
                summary=(summ or "")[:400] or None,
            )
        )
    return TranscriptListResponse(items=items)
