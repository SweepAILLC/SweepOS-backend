from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, Any
import uuid


class EventBase(BaseModel):
    type: str
    payload: Optional[Dict[str, Any]] = None


class EventCreate(EventBase):
    client_id: Optional[uuid.UUID] = None


class Event(EventBase):
    id: uuid.UUID
    client_id: Optional[uuid.UUID] = None
    occurred_at: datetime

    class Config:
        from_attributes = True

