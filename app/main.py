from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import auth, clients, events, oauth, integrations, stripe, webhooks, funnels, admin, users, organizations, encryption, email_ingestion, fathom_webhooks, performance, content_studio, call_library
from app.core.config import settings as app_settings
from app.middleware.global_rate_limit import GlobalRateLimitMiddleware

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
app.include_router(content_studio.router, prefix="/content-studio", tags=["content-studio"])
app.include_router(call_library.router, prefix="/call-library", tags=["call-library"])
app.include_router(events.router, prefix="/events", tags=["events"])
app.include_router(funnels.router, prefix="/funnels", tags=["funnels"])
app.include_router(users.router, prefix="/users", tags=["users"])
app.include_router(organizations.router, prefix="/organizations", tags=["organizations"])
app.include_router(oauth.router, prefix="/oauth", tags=["oauth"])
app.include_router(integrations.router, prefix="/integrations", tags=["integrations"])
app.include_router(stripe.router, prefix="/integrations/stripe", tags=["stripe"])
app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
app.include_router(fathom_webhooks.router, prefix="/webhooks", tags=["fathom"])
app.include_router(admin.router, prefix="/admin", tags=["admin"])
app.include_router(encryption.router, prefix="/admin", tags=["encryption"])
app.include_router(email_ingestion.router, prefix="/webhooks", tags=["brevo-webhooks"])


@app.get("/")
async def root():
    return {"message": "Sweep Coach OS API", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}

