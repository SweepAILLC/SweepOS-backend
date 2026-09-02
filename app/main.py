from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import auth, clients, events, oauth, integrations, stripe, whop, finances, webhooks, funnels, admin, users, organizations, encryption, email_ingestion, fathom_webhooks, content_studio, call_library, automations, outreach, calendar_webhooks, resources, auth_google, mcp_oauth, portal, portal_funnel_simulator, kpi, instagram, close_survey
from app.mcp import server as mcp_server
from app.core.config import settings as app_settings
from app.middleware.global_rate_limit import GlobalRateLimitMiddleware
import logging
import threading

import sentry_sdk

_sentry_dsn = (app_settings.SENTRY_DSN or "").strip()
if _sentry_dsn:
    sentry_sdk.init(
        dsn=_sentry_dsn,
        # Request headers / IP — see https://docs.sentry.io/platforms/python/data-management/data-collected/
        send_default_pii=app_settings.SENTRY_SEND_DEFAULT_PII,
        traces_sample_rate=app_settings.SENTRY_TRACES_SAMPLE_RATE,
        environment=app_settings.ENVIRONMENT,
    )

app = FastAPI(title="Sweep Coach OS API", version="1.0.0")

def _tune_api_threadpool() -> None:
    """Sync routes (LLM drafts, Stripe) run in anyio's threadpool. Default 40 is tight at 50 users."""
    n = int(getattr(app_settings, "API_THREADPOOL_SIZE", 64) or 64)
    try:
        import anyio.to_thread

        anyio.to_thread.current_default_thread_limiter().total_tokens = n
    except Exception:
        logging.getLogger("app").warning("could not raise API threadpool to %s", n, exc_info=True)

_tune_api_threadpool()

# CORS: default localhost + ALLOWED_ORIGINS_EXTRA for production (Render/Vercel)
_allowed_origins = app_settings.get_allowed_origins()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*", "Mcp-Session-Id", "MCP-Protocol-Version", "WWW-Authenticate"],
)
# Global throttle (after CORS registration so this runs first on each request — see Starlette order)
app.add_middleware(GlobalRateLimitMiddleware)

# Add LLM-specific exception handlers first (more specific)
from app.core.llm_exceptions import LLMException
from app.api.llm_error_handler import llm_exception_handler

app.add_exception_handler(LLMException, llm_exception_handler)

# Add exception handler to ensure CORS headers are included even on errors
from fastapi.responses import JSONResponse
from fastapi import Request

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Ensure CORS headers are included even on unhandled exceptions"""
    import logging
    logging.getLogger("app").exception("Unhandled exception on %s %s", request.method, request.url.path)
    if _sentry_dsn:
        sentry_sdk.capture_exception(exc)
        # Ensure verify hits (and short-lived workers) flush before response returns
        if request.url.path == "/sentry-debug":
            sentry_sdk.flush(timeout=5)

    response = JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )
    
    # Add CORS headers manually (same allowed origins as middleware)
    origin = request.headers.get("origin")
    if origin and origin in _allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "*"
    
    return response

# Include routers
# IMPORTANT: Router registration order matters! FastAPI matches routes in registration order.
# More specific prefixes (e.g., /integrations/stripe) must be registered AFTER
# less specific ones (e.g., /integrations) to avoid conflicts.
# See backend/app/api/ROUTING_GUIDELINES.md for details.
app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(auth_google.router, prefix="/auth", tags=["auth-google"])
app.include_router(mcp_oauth.router, tags=["mcp-oauth"])
app.include_router(mcp_server.router, tags=["mcp"])
app.include_router(clients.router, prefix="/clients", tags=["clients"])
app.include_router(automations.router, prefix="/automations", tags=["automations"])
app.include_router(outreach.router, prefix="/outreach", tags=["outreach"])
app.include_router(content_studio.router, prefix="/content-studio", tags=["content-studio"])
app.include_router(call_library.router, prefix="/call-library", tags=["call-library"])
app.include_router(events.router, prefix="/events", tags=["events"])
app.include_router(funnels.router, prefix="/funnels", tags=["funnels"])
app.include_router(users.router, prefix="/users", tags=["users"])
app.include_router(organizations.router, prefix="/organizations", tags=["organizations"])
app.include_router(oauth.router, prefix="/oauth", tags=["oauth"])
app.include_router(integrations.router, prefix="/integrations", tags=["integrations"])
app.include_router(finances.router, prefix="/integrations/finances", tags=["finances"])
app.include_router(whop.router, prefix="/integrations/whop", tags=["whop"])
app.include_router(stripe.router, prefix="/integrations/stripe", tags=["stripe"])
app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
app.include_router(fathom_webhooks.router, prefix="/webhooks", tags=["fathom"])
app.include_router(calendar_webhooks.router, prefix="/webhooks", tags=["calendar-webhooks"])
app.include_router(admin.router, prefix="/admin", tags=["admin"])
app.include_router(encryption.router, prefix="/admin", tags=["encryption"])
app.include_router(email_ingestion.router, prefix="/webhooks", tags=["brevo-webhooks"])
app.include_router(resources.router, prefix="/resources", tags=["resources"])
app.include_router(portal.router, prefix="/portal", tags=["portal"])
app.include_router(portal_funnel_simulator.router, prefix="/portal", tags=["portal"])
app.include_router(kpi.router, prefix="/kpi", tags=["kpi"])
app.include_router(close_survey.router, prefix="/close-survey", tags=["close-survey"])
app.include_router(instagram.router, prefix="/instagram", tags=["instagram"])

@app.on_event("startup")
def _ensure_schema_columns_on_startup() -> None:
    """
    Safety net for local/dev deployments without alembic migrations applied.

    We occasionally add columns to ORM models and rely on runtime `ALTER TABLE ... IF NOT EXISTS`
    to keep the app bootable. If these columns don't exist, *any* query touching the model
    (including login) can 500 due to UndefinedColumn.

    IMPORTANT: DDL uses ACCESS EXCLUSIVE. Always set a short lock_timeout and skip
    columns that already exist, otherwise a single idle-in-transaction reader can
    queue every organizations SELECT behind a blocked ALTER (prod outage pattern).
    """
    from sqlalchemy import text
    from app.db.session import SessionLocal

    log = logging.getLogger("app")
    db = SessionLocal()

    def _column_exists(table: str, column: str) -> bool:
        row = db.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :t AND column_name = :c"
            ),
            {"t": table, "c": column},
        ).first()
        return row is not None

    def _add_column_if_missing(table: str, column: str, ddl_type: str) -> None:
        if _column_exists(table, column):
            return
        db.execute(text("SET LOCAL lock_timeout = '3s'"))
        db.execute(
            text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {ddl_type}")
        )

    try:
        # Fail fast on lock contention rather than blocking the whole fleet.
        db.execute(text("SET LOCAL lock_timeout = '3s'"))

        # users: Google OAuth identity columns (non-unique index — multi-org rows share google_id)
        _add_column_if_missing("users", "google_id", "TEXT")
        _add_column_if_missing("users", "google_email", "TEXT")
        try:
            db.execute(text("SET LOCAL lock_timeout = '3s'"))
            db.execute(text("ALTER TABLE users ALTER COLUMN hashed_password DROP NOT NULL"))
        except Exception:
            db.rollback()
            db.execute(text("SET LOCAL lock_timeout = '3s'"))
        db.execute(text("DROP INDEX IF EXISTS ix_users_google_id"))
        db.execute(text("CREATE INDEX IF NOT EXISTS ix_users_google_id ON users (google_id)"))

        # MCP OAuth tables (Claude custom connector)
        from app.models.mcp_oauth import McpOAuthClient, McpOAuthGrant

        McpOAuthClient.__table__.create(db.bind, checkfirst=True)
        McpOAuthGrant.__table__.create(db.bind, checkfirst=True)

        # user_organizations: per-org Intelligence bank
        _add_column_if_missing("user_organizations", "ai_profile", "JSONB")

        # lifecyclestate: ensure lowercase labels exist (migration 047) for ORM + API writes
        for label in (
            "cold_lead",
            "nurturing",
            "qualified",
            "booked",
            "active",
            "offboarding",
            "dead",
        ):
            db.execute(
                text(
                    "DO $$ BEGIN "
                    f"ALTER TYPE lifecyclestate ADD VALUE IF NOT EXISTS '{label}'; "
                    "EXCEPTION WHEN duplicate_object THEN NULL; "
                    "END $$;"
                )
            )

        # organizations: fathom webhook fields + consulting portal
        _add_column_if_missing("organizations", "fathom_webhook_id", "TEXT")
        _add_column_if_missing("organizations", "fathom_webhook_secret", "TEXT")
        _add_column_if_missing("organizations", "fathom_webhook_url", "TEXT")
        _add_column_if_missing("organizations", "consulting_tier", "VARCHAR")
        _add_column_if_missing("organizations", "booking_url", "TEXT")
        _add_column_if_missing("organizations", "close_form_token", "UUID")
        try:
            db.execute(text("SET LOCAL lock_timeout = '3s'"))
            db.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_organizations_close_form_token "
                    "ON organizations (close_form_token)"
                )
            )
        except Exception:
            db.rollback()
            db.execute(text("SET LOCAL lock_timeout = '3s'"))

        # portal_todos: org portal to-do list
        from app.models.portal_todo import PortalTodo

        PortalTodo.__table__.create(db.bind, checkfirst=True)

        # portal_shared_pads: multi-tab live consulting notepad
        db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS portal_shared_pads (
                    id UUID PRIMARY KEY,
                    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                    title VARCHAR(120) NOT NULL DEFAULT 'Onboarding',
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    content TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL DEFAULT 1,
                    updated_by UUID REFERENCES users(id),
                    updated_by_name VARCHAR(255),
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
                )
                """
            )
        )
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_portal_shared_pads_org_id ON portal_shared_pads (org_id)"
            )
        )
        db.execute(
            text(
                "ALTER TABLE portal_shared_pads ADD COLUMN IF NOT EXISTS title VARCHAR(120) NOT NULL DEFAULT 'Onboarding'"
            )
        )
        db.execute(
            text(
                "ALTER TABLE portal_shared_pads ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0"
            )
        )
        db.execute(text("ALTER TABLE portal_shared_pads DROP CONSTRAINT IF EXISTS uq_portal_shared_pads_org_id"))

        # organizations: notification settings bag for funnel-lead digests etc.
        _add_column_if_missing("organizations", "notification_settings", "JSONB")

        # funnel_lead_notifications: batched lead digest queue
        from app.models.funnel_lead_notification import FunnelLeadNotification

        FunnelLeadNotification.__table__.create(db.bind, checkfirst=True)
        try:
            db.execute(text("SET LOCAL lock_timeout = '3s'"))
            db.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_funnel_lead_notifications_unsent_org_created "
                    "ON funnel_lead_notifications (org_id, created_at) "
                    "WHERE sent_at IS NULL"
                )
            )
        except Exception:
            db.rollback()
            db.execute(text("SET LOCAL lock_timeout = '3s'"))

        # fathom_call_records: ensure table exists (some DBs stamped past 031 without the table)
        from app.models.fathom_call_record import FathomCallRecord

        FathomCallRecord.__table__.create(db.bind, checkfirst=True)

        # fathom_call_records: media URLs from payload
        _add_column_if_missing("fathom_call_records", "share_url", "TEXT")
        _add_column_if_missing("fathom_call_records", "video_url", "TEXT")
        _add_column_if_missing("fathom_call_records", "meeting_title", "TEXT")
        _add_column_if_missing("fathom_call_records", "recording_url", "TEXT")
        _add_column_if_missing("fathom_call_records", "attendees_json", "JSONB")
        _add_column_if_missing("fathom_call_records", "related_client_ids", "JSONB")

        # call_library_reports: media URLs for embedding
        _add_column_if_missing("call_library_reports", "share_url", "TEXT")
        _add_column_if_missing("call_library_reports", "video_url", "TEXT")

        # Calendar tab: speed up GET /integrations/calendar/synced-bookings (org + provider + time range)
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_client_check_ins_org_provider_start "
                "ON client_check_ins (org_id, provider, start_time)"
            )
        )

        # resource_documents: owner-editable SOPs for the Resources / portal library
        # Isolated so earlier startup DDL failures cannot skip these column migrations.
        try:
            from app.services.resource_documents import ensure_resource_documents_table

            ensure_resource_documents_table(db)
        except Exception as e:
            log.warning("resource_documents schema ensure failed: %s", e)
            db.rollback()
            db.execute(text("SET LOCAL lock_timeout = '3s'"))

        # org_resource_library: org-specific uploads/links library
        try:
            from app.services.resource_library import ensure_resource_library_table

            ensure_resource_library_table(db)
        except Exception as e:
            log.warning("resource_library schema ensure failed: %s", e)
            db.rollback()
            db.execute(text("SET LOCAL lock_timeout = '3s'"))

        try:
            from app.services.funnel_simulator import ensure_funnel_simulator_scenarios_table

            ensure_funnel_simulator_scenarios_table(db)
        except Exception as e:
            log.warning("funnel_simulator_scenarios schema ensure failed: %s", e)
            db.rollback()
            db.execute(text("SET LOCAL lock_timeout = '3s'"))

        from app.models.org_app_session import OrgAppSession
        from app.models.owner_org_notice import OwnerOrgNotice, OwnerOrgNoticeRead
        from app.models.portal_shared_pad import PortalSharedPadDefault

        OrgAppSession.__table__.create(db.bind, checkfirst=True)
        OwnerOrgNotice.__table__.create(db.bind, checkfirst=True)
        OwnerOrgNoticeRead.__table__.create(db.bind, checkfirst=True)
        PortalSharedPadDefault.__table__.create(db.bind, checkfirst=True)
        try:
            db.execute(text("SET LOCAL lock_timeout = '3s'"))
            db.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_org_app_sessions_org_last_seen "
                    "ON org_app_sessions (org_id, last_seen_at)"
                )
            )
            db.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_owner_org_notices_org_created "
                    "ON owner_org_notices (org_id, created_at)"
                )
            )
        except Exception:
            db.rollback()
            db.execute(text("SET LOCAL lock_timeout = '3s'"))

        db.commit()

        if getattr(app_settings, "FATHOM_RECONCILE_WEBHOOKS_ON_STARTUP", True):
            from app.services.fathom_onboard import reconcile_fathom_webhooks_for_existing_orgs

            threading.Thread(
                target=reconcile_fathom_webhooks_for_existing_orgs,
                daemon=True,
                name="fathom-webhook-reconcile",
            ).start()

        if getattr(app_settings, "STRIPE_RECONCILE_WEBHOOKS_ON_STARTUP", True):
            from app.services.stripe_webhook_onboard import reconcile_stripe_webhooks_for_existing_orgs

            threading.Thread(
                target=reconcile_stripe_webhooks_for_existing_orgs,
                daemon=True,
                name="stripe-webhook-reconcile",
            ).start()
    except Exception as e:
        db.rollback()
        log.warning("startup schema ensure failed: %s", e)
    finally:
        db.close()


@app.get("/")
async def root():
    return {"message": "Sweep Coach OS API", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/sentry-debug")
async def trigger_error():
    division_by_zero = 1 / 0

