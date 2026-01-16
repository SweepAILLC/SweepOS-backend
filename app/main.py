from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import auth, clients, events, oauth, integrations, stripe, webhooks, funnels, admin, users

app = FastAPI(title="Sweep Coach OS API", version="1.0.0")

# CORS middleware - allow common frontend ports
# Note: CORS headers are added even on errors via exception handlers
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "http://localhost:3003",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
        "http://127.0.0.1:3002",
        "http://127.0.0.1:3003",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Add exception handler to ensure CORS headers are included even on errors
from fastapi.responses import JSONResponse
from fastapi import Request

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Ensure CORS headers are included even on unhandled exceptions"""
    import traceback
    print(f"Unhandled exception: {str(exc)}")
    print(traceback.format_exc())
    
    # Create response with CORS headers
    response = JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {str(exc)}"}
    )
    
    # Add CORS headers manually
    origin = request.headers.get("origin")
    if origin and origin in [
        "http://localhost:3000", "http://localhost:3001", "http://localhost:3002", "http://localhost:3003",
        "http://127.0.0.1:3000", "http://127.0.0.1:3001", "http://127.0.0.1:3002", "http://127.0.0.1:3003"
    ]:
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
app.include_router(events.router, prefix="/events", tags=["events"])
app.include_router(funnels.router, prefix="/funnels", tags=["funnels"])
app.include_router(users.router, prefix="/users", tags=["users"])
app.include_router(oauth.router, prefix="/oauth", tags=["oauth"])
app.include_router(integrations.router, prefix="/integrations", tags=["integrations"])
app.include_router(stripe.router, prefix="/integrations/stripe", tags=["stripe"])
app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
app.include_router(admin.router, prefix="/admin", tags=["admin"])


@app.get("/")
async def root():
    return {"message": "Sweep Coach OS API", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}

