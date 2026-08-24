import os

from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

_role = os.environ.get("SWEEP_PROCESS_ROLE", "web")
if _role == "worker":
    _pool_size = int(getattr(settings, "WORKER_DATABASE_POOL_SIZE", 4) or 4)
    _max_overflow = int(getattr(settings, "WORKER_DATABASE_MAX_OVERFLOW", 6) or 6)
else:
    _pool_size = getattr(settings, "DATABASE_POOL_SIZE", 10)
    _max_overflow = getattr(settings, "DATABASE_MAX_OVERFLOW", 20)

engine = create_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_size=_pool_size,
    max_overflow=_max_overflow,
    pool_timeout=getattr(settings, "DATABASE_POOL_TIMEOUT", 30),
    pool_recycle=getattr(settings, "DATABASE_POOL_RECYCLE", 1800),
    pool_pre_ping=True,
)

@event.listens_for(engine, "connect")
def _set_statement_timeout(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("SET statement_timeout = '120s'")
    cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        # Always end the transaction before returning the connection to the pool.
        # Leaving SELECT transactions open causes AccessShareLocks that block
        # startup/runtime ALTER TABLE and cascade into auth/me timeouts.
        try:
            db.rollback()
        except Exception:
            pass
        db.close()
