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
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440  # 24 hours; reduce logout frequency
    
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

    # Backend public URL for webhook endpoints (required for per-org Stripe webhooks)
    # e.g. https://api.sweepai.site or http://localhost:8000 for local dev
    BACKEND_PUBLIC_URL: Optional[str] = None

    # Fathom (optional — omit for logic-only health score; sync/webhook no-op).
    # AI health score overlay is skipped until FATHOM_API_KEY is set; scoring stays logic-based.
    FATHOM_API_KEY: Optional[str] = None
    # Secret from Fathom webhook create response; if unset, signature verification is skipped
    FATHOM_WEBHOOK_SECRET: Optional[str] = None

    # LLM for Fathom sentiment + AI health score (optional — falls back to logic score)
    # Use Gemini: set GOOGLE_API_KEY (or LLM_API_KEY) and HEALTH_SCORE_LLM_MODEL e.g. gemini-2.0-flash
    # Or OpenAI: set OPENAI_API_KEY and model e.g. gpt-4o-mini
    LLM_API_KEY: Optional[str] = None
    GOOGLE_API_KEY: Optional[str] = None
    OPENAI_API_KEY: Optional[str] = None
    HEALTH_SCORE_LLM_MODEL: str = "gemini-2.0-flash"
    LLM_PROVIDER: str = "auto"  # auto | gemini | openai
    AI_HEALTH_SCORE_ENABLED: bool = True  # If False, never call LLM for health score (sentiment still uses LLM when Fathom ingests)

    # LLM cost & safety (per-org in-memory budget; tune for your traffic)
    LLM_BUDGET_ENABLED: bool = True
    LLM_MAX_CALLS_PER_MINUTE_PER_ORG: int = 45
    LLM_MAX_INPUT_CHARS_TOTAL: int = 48000  # Hard cap on system+user prompt size sent to providers

    # Fathom sentiment: skip LLM when combined input is too small (saves calls; default neutral locally)
    FATHOM_SENTIMENT_MIN_INPUT_CHARS: int = 80

    # GET /clients/{id}/health-score rate limits (sliding window, per user+org)
    HEALTH_SCORE_RATE_LIMIT_WINDOW_SEC: int = 300
    HEALTH_SCORE_RATE_LIMIT_MAX: int = 120  # logic-only path
    HEALTH_SCORE_AI_RATE_LIMIT_MAX: int = 25  # use_ai=true (more expensive)

    # Fathom sync: optional pause between meetings to respect provider rate limits (ms)
    FATHOM_SYNC_DELAY_MS: int = 0
    # Max pages per sync request (cost / time bound)
    FATHOM_SYNC_MAX_PAGES: int = 5

    # Call insights (LLM per matched Fathom recording; safeguards for cost)
    CALL_INSIGHT_MIN_INPUT_CHARS: int = 400
    CALL_INSIGHT_MIN_TRANSCRIPT_LINES: int = 3
    CALL_INSIGHT_CHECKIN_WINDOW_MINUTES: int = 105
    CALL_INSIGHT_COOLDOWN_HOURS: int = 24
    CALL_INSIGHT_ORG_MAX_PER_HOUR: int = 40
    CALL_INSIGHT_HEALTH_SCORE_DELTA_OVERRIDE: float = 15.0

    # Org sales content themes (objections / patterns must recur across clients before content use)
    ORG_SALES_THEME_MIN_DISTINCT_CLIENTS: int = 3
    ORG_SALES_THEME_MIN_OCCURRENCES: int = 3
    ORG_SALES_THEME_LOOKBACK_DAYS: int = 120
    ORG_SALES_THEME_MAX_CONTRIBUTING_CLIENTS: int = 500
    ORG_SALES_THEME_MAX_SAMPLE_QUOTES: int = 8
    
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

