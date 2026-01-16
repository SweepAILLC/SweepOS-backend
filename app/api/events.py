from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.models.event import Event
from app.schemas.event import Event as EventSchema, EventCreate
from app.api.deps import get_current_user
from app.models.user import User

router = APIRouter()


@router.post("", response_model=EventSchema, status_code=status.HTTP_201_CREATED)
def create_event(
    event_data: EventCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    event = Event(**event_data.model_dump())
    db.add(event)
    db.commit()
    db.refresh(event)
    return event

