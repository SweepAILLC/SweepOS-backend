from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

engine = create_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_size=getattr(settings, "DATABASE_POOL_SIZE", 10),
    max_overflow=getattr(settings, "DATABASE_MAX_OVERFLOW", 20),
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
        db.close()

