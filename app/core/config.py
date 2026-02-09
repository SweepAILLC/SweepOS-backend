from pydantic_settings import BaseSettings
from typing import Optional, List


def _parse_allowed_origins(v: str) -> List[str]:
    """Parse comma-separated origins string; strip whitespace; keep non-empty."""
    if not v or not v.strip():
        return []
    return [o.strip() for o in v.split(",") if o.strip()]


_DEFAULT_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:3002",
    "http://localhost:3003",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
    "http://127.0.0.1:3002",
    "http://127.0.0.1:3003",
]


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql://postgres:postgres@db:5432/sweep"
    
    # CORS: comma-separated extra origins for production (e.g. https://app.sweepai.site)
    # Default localhost origins are always included. Set this for Render/Vercel beta.
    ALLOWED_ORIGINS_EXTRA: str = ""
    
    def get_allowed_origins(self) -> List[str]:
        """Return CORS allowed origins: default localhost + ALLOWED_ORIGINS_EXTRA."""
        return _DEFAULT_CORS_ORIGINS + _parse_allowed_origins(self.ALLOWED_ORIGINS_EXTRA)
    
    # Auth
    SECRET_KEY: str = "supersecret_jwt_key_change_in_production"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    
    # Stripe
    STRIPE_CLIENT_ID: Optional[str] = None
    STRIPE_SECRET_KEY: Optional[str] = None
    STRIPE_OAUTH_CLIENT_ID: Optional[str] = None
    STRIPE_REDIRECT_URI: str = "https://sweepai.site/api/oauth/stripe/callback"
    STRIPE_TEST_OAUTH_URL: Optional[str] = None  # For development: use test OAuth URL from External test tab
    STRIPE_WEBHOOK_SECRET: Optional[str] = None  # Webhook signing secret for signature verification
    
    # Brevo
    BREVO_CLIENT_ID: Optional[str] = None
    BREVO_CLIENT_SECRET: Optional[str] = None
    BREVO_REDIRECT_URI: str = "http://localhost:3002/oauth/brevo/callback"
    BREVO_LOGIN_URL: Optional[str] = None  # Optional custom login URL, defaults to standard Brevo auth URL
    BREVO_API_KEY: Optional[str] = None  # Global API key for onboarding emails (invitations); does not affect per-org Brevo OAuth/API
    
    # Frontend
    FRONTEND_URL: str = "http://localhost:3002"  # Frontend URL for OAuth redirects
    
    # Admin
    SUDO_ADMIN_EMAIL: str = "admin@sweepos.local"
    SUDO_ADMIN_PASSWORD: str = "changeme"
    
    # Encryption
    ENCRYPTION_KEY: Optional[str] = None
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        # Pydantic will read from environment variables first (set by Docker Compose),
        # then fall back to .env file if not found in environment


settings = Settings()

