from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql://postgres:postgres@db:5432/sweep"
    
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

