from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey, TypeDecorator
from sqlalchemy.dialects.postgresql import UUID, JSON
import uuid
from datetime import datetime
import enum
from app.db.session import Base


class UserRole(str, enum.Enum):
    """Stored in PostgreSQL as native enum `userrole`: OWNER, ADMIN, MEMBER (uppercase labels)."""

    OWNER = "OWNER"
    ADMIN = "ADMIN"
    MEMBER = "MEMBER"


def parse_user_role_from_db(raw: object) -> UserRole:
    """Map DB `userrole` (any legacy casing) to UserRole."""
    if raw is None:
        return UserRole.ADMIN
    s = str(raw).strip()
    up = s.upper()
    if up == "OWNER":
        return UserRole.OWNER
    if up == "ADMIN":
        return UserRole.ADMIN
    if up == "MEMBER":
        return UserRole.MEMBER
    # legacy lowercase `member` from older migrations
    if s.lower() == "member":
        return UserRole.MEMBER
    return UserRole.ADMIN


def userrole_bind_value(role: UserRole) -> str:
    """PostgreSQL `userrole` label for raw SQL CAST (legacy rows use lowercase `member`)."""
    if role == UserRole.MEMBER:
        return "member"
    return role.value


def role_to_api(role: UserRole) -> str:
    """API / frontend expect lowercase owner | admin | member."""
    return role.value.lower()


def parse_user_role_from_api(raw: str) -> UserRole:
    """Parse settings/UI role strings into UserRole."""
    if not raw:
        return UserRole.MEMBER
    key = str(raw).strip().lower()
    if key == "owner":
        return UserRole.OWNER
    if key == "admin":
        return UserRole.ADMIN
    if key == "member":
        return UserRole.MEMBER
    return UserRole.MEMBER


class PgUserRole(TypeDecorator):
    """
    Read/write PostgreSQL `userrole` tolerating legacy lowercase `member` labels.

    Avoid binding through PG_ENUM directly — production DBs may only accept lowercase
    `member` while SQLAlchemy's enum processor expects uppercase MEMBER.
    """

    impl = String(32)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, UserRole):
            return userrole_bind_value(value)
        return userrole_bind_value(parse_user_role_from_db(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return parse_user_role_from_db(value)


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True)
    email = Column(String, nullable=False, index=True)
    # Nullable for Google-only accounts (no password set)
    hashed_password = Column(String, nullable=True)
    role = Column(PgUserRole(), default=UserRole.ADMIN, nullable=False)
    is_admin = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    fathom_api_key = Column(String, nullable=True)
    ai_profile = Column(JSON, nullable=True)
    google_id = Column(String, nullable=True, index=True)
    google_email = Column(String, nullable=True)

    __table_args__ = (
        {"schema": None},
    )
