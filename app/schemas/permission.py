from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID
from datetime import datetime


class TabPermissionBase(BaseModel):
    tab_name: str
    enabled: bool


class OrganizationTabPermissionCreate(TabPermissionBase):
    pass


class OrganizationTabPermissionUpdate(BaseModel):
    enabled: Optional[bool] = None


class OrganizationTabPermission(TabPermissionBase):
    id: UUID
    org_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class UserTabPermissionCreate(TabPermissionBase):
    pass


class UserTabPermissionUpdate(BaseModel):
    enabled: Optional[bool] = None


class UserTabPermission(TabPermissionBase):
    id: UUID
    user_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TabAccessResponse(BaseModel):
    """Response for checking tab access"""
    tab_name: str
    has_access: bool
    reason: Optional[str] = None  # Why access is denied (if applicable)

