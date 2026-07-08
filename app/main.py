from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import auth, clients, events, oauth, integrations, stripe, whop, finances, webhooks, funnels, admin, users, organizations, encryption, email_ingestion, fathom_webhooks, performance, content_studio, call_library, automations, outreach, calendar_webhooks, resources
from app.core.config import settings as app_settings
from app.middleware.global_rate_limit import GlobalRateLimitMiddleware
import logging
import threading

app = FastAPI(title="Sweep Coach OS API", version="1.0.0")

# CORS: default localhost + ALLOWED_ORIGINS_EXTRA for production (Render/Vercel)
_allowed_origins = app_settings.get_allowed_origins()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
# Global throttle (after CORS registration so this runs first on each request — see Starlette order)
app.add_middleware(GlobalRateLimitMiddleware)

# Add exception handler to ensure CORS headers are included even on errors
from fastapi.responses import JSONResponse
from fastapi import Request

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Ensure CORS headers are included even on unhandled exceptions"""
    import logging
    logging.getLogger("app").exception("Unhandled exception on %s %s", request.method, request.url.path)

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
app.include_router(clients.router, prefix="/clients", tags=["clients"])
app.include_router(performance.router, prefix="/performance", tags=["performance"])
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

@app.on_event("startup")
def _ensure_schema_columns_on_startup() -> None:
    """
    Safety net for local/dev deployments without alembic migrations applied.

    We occasionally add columns to ORM models and rely on runtime `ALTER TABLE ... IF NOT EXISTS`
    to keep the app bootable. If these columns don't exist, *any* query touching the model
    (including login) can 500 due to UndefinedColumn.
    """
    from sqlalchemy import text
    from app.db.session import SessionLocal

    log = logging.getLogger("app")
    db = SessionLocal()
    try:
        # user_organizations: per-org Intelligence bank
        db.execute(text("ALTER TABLE user_organizations ADD COLUMN IF NOT EXISTS ai_profile JSONB"))

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

        # organizations: fathom webhook fields
        db.execute(text("ALTER TABLE organizations ADD COLUMN IF NOT EXISTS fathom_webhook_id TEXT"))
        db.execute(text("ALTER TABLE organizations ADD COLUMN IF NOT EXISTS fathom_webhook_secret TEXT"))
        db.execute(text("ALTER TABLE organizations ADD COLUMN IF NOT EXISTS fathom_webhook_url TEXT"))

        # fathom_call_records: ensure table exists (some DBs stamped past 031 without the table)
        from app.models.fathom_call_record import FathomCallRecord

        FathomCallRecord.__table__.create(db.bind, checkfirst=True)

        # fathom_call_records: media URLs from payload
        db.execute(text("ALTER TABLE fathom_call_records ADD COLUMN IF NOT EXISTS share_url TEXT"))
        db.execute(text("ALTER TABLE fathom_call_records ADD COLUMN IF NOT EXISTS video_url TEXT"))
        db.execute(text("ALTER TABLE fathom_call_records ADD COLUMN IF NOT EXISTS meeting_title TEXT"))
        db.execute(text("ALTER TABLE fathom_call_records ADD COLUMN IF NOT EXISTS recording_url TEXT"))
        db.execute(text("ALTER TABLE fathom_call_records ADD COLUMN IF NOT EXISTS attendees_json JSONB"))
        db.execute(text("ALTER TABLE fathom_call_records ADD COLUMN IF NOT EXISTS related_client_ids JSONB"))

        # call_library_reports: media URLs for embedding
        db.execute(text("ALTER TABLE call_library_reports ADD COLUMN IF NOT EXISTS share_url TEXT"))
        db.execute(text("ALTER TABLE call_library_reports ADD COLUMN IF NOT EXISTS video_url TEXT"))

        # Calendar tab: speed up GET /integrations/calendar/synced-bookings (org + provider + time range)
        db.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_client_check_ins_org_provider_start "
                "ON client_check_ins (org_id, provider, start_time)"
            )
        )

        # resource_documents: owner-editable SOPs for the Resources tab
        from app.services.resource_documents import ensure_resource_documents_table
        ensure_resource_documents_table(db)

        # org_resource_library: org-specific uploads/links library
        from app.services.resource_library import ensure_resource_library_table
        ensure_resource_library_table(db)

        db.commit()

        if getattr(app_settings, "FATHOM_RECONCILE_WEBHOOKS_ON_STARTUP", True):
            from app.services.fathom_onboard import reconcile_fathom_webhooks_for_existing_orgs

            threading.Thread(
                target=reconcile_fathom_webhooks_for_existing_orgs,
                daemon=True,
                name="fathom-webhook-reconcile",
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

