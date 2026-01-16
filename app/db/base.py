from app.db.session import Base
from app.models import user, client, event, oauth_token, campaign, recommendation

# Import all models so Alembic can detect them
__all__ = ["Base"]

