"""Schemas for org notification settings (funnel lead digests, etc.)."""
from typing import List, Literal, Optional
from pydantic import BaseModel, EmailStr, Field


class FunnelLeadNotificationSettings(BaseModel):
    enabled: bool = True
    window_minutes: int = Field(15, ge=1, le=1440)
    recipient_mode: Literal["admins", "custom"] = "admins"
    recipients: List[EmailStr] = []
    include_returning_leads: bool = True


class NotificationSettingsResponse(BaseModel):
    funnel_leads: FunnelLeadNotificationSettings


class FunnelLeadNotificationSettingsUpdate(BaseModel):
    enabled: Optional[bool] = None
    window_minutes: Optional[int] = Field(None, ge=1, le=1440)
    recipient_mode: Optional[Literal["admins", "custom"]] = None
    recipients: Optional[List[EmailStr]] = None
    include_returning_leads: Optional[bool] = None


class NotificationSettingsUpdate(BaseModel):
    funnel_leads: Optional[FunnelLeadNotificationSettingsUpdate] = None


class NotificationTestResponse(BaseModel):
    success: bool
    message: str
    recipients: List[str] = []
    failed: List[str] = []
