from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class Payment(BaseModel):
    id: str
    amount: float
    currency: str
    status: str
    created_at: datetime


class Subscription(BaseModel):
    id: str
    customer_id: str
    status: str
    current_period_end: datetime
    amount: float


class StripeSummary(BaseModel):
    total_mrr: float
    last_30_days_revenue: float
    active_subscriptions: int
    payments: List[Payment]
    subscriptions: List[Subscription]


class BrevoStatus(BaseModel):
    connected: bool
    account_email: Optional[str] = None
    account_name: Optional[str] = None
    message: Optional[str] = None


class CalComStatus(BaseModel):
    connected: bool
    account_email: Optional[str] = None
    account_name: Optional[str] = None
    message: Optional[str] = None


class CalComBooking(BaseModel):
    id: int
    uid: Optional[str] = None  # Booking UID from Cal.com
    title: Optional[str] = None
    description: Optional[str] = None
    startTime: str  # ISO 8601 datetime string (mapped from 'start')
    endTime: str  # ISO 8601 datetime string (mapped from 'end')
    start: Optional[str] = None  # Original 'start' field from API
    end: Optional[str] = None  # Original 'end' field from API
    duration: Optional[int] = None  # Duration in minutes
    attendees: Optional[List[dict]] = None
    hosts: Optional[List[dict]] = None  # Original 'hosts' field from API
    user: Optional[dict] = None  # Mapped from first host
    eventTypeId: Optional[int] = None
    eventType: Optional[dict] = None
    status: Optional[str] = None
    location: Optional[str] = None
    cancellationReason: Optional[str] = None
    cancelledByEmail: Optional[str] = None
    reschedulingReason: Optional[str] = None
    rescheduledByEmail: Optional[str] = None
    rescheduledFromUid: Optional[str] = None
    rescheduledToUid: Optional[str] = None
    rescheduled: Optional[bool] = None
    absentHost: Optional[bool] = None
    paid: Optional[bool] = None
    payment: Optional[dict] = None
    meetingUrl: Optional[str] = None
    metadata: Optional[dict] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    rating: Optional[int] = None
    guests: Optional[List[str]] = None
    responses: Optional[dict] = None  # Form responses from booking questions
    bookingFields: Optional[List[dict]] = None  # Custom booking fields and their responses
    routingFormResponses: Optional[List[dict]] = None  # Routing form responses matched by booking UID
    
    class Config:
        extra = "allow"  # Allow extra fields from Cal.com API


class CalComEventType(BaseModel):
    id: int
    title: str
    slug: Optional[str] = None  # Make slug optional in case API doesn't return it
    description: Optional[str] = None
    length: Optional[int] = None  # Duration in minutes (mapped from 'lengthInMinutes') - make optional
    lengthInMinutes: Optional[int] = None  # Original field from API
    hidden: Optional[bool] = None
    position: Optional[int] = None
    eventName: Optional[str] = None
    timeZone: Optional[str] = None
    periodType: Optional[str] = None
    periodDays: Optional[int] = None
    periodStartDate: Optional[str] = None
    periodEndDate: Optional[str] = None
    periodCountCalendarDays: Optional[bool] = None
    requiresConfirmation: Optional[bool] = None
    bookingRequiresAuthentication: Optional[bool] = None
    recurringEvent: Optional[dict] = None
    recurrence: Optional[dict] = None  # Cal.com API uses 'recurrence'
    price: Optional[float] = None
    currency: Optional[str] = None
    metadata: Optional[dict] = None
    locations: Optional[List[dict]] = None
    bookingFields: Optional[List[dict]] = None
    disableGuests: Optional[bool] = None
    lockTimeZoneToggleOnBookingPage: Optional[bool] = None
    forwardParamsSuccessRedirect: Optional[bool] = None  # API returns bool, not dict
    successRedirectUrl: Optional[str] = None  # API returns string URL or None, not dict
    isInstantEvent: Optional[bool] = None
    scheduleId: Optional[int] = None
    ownerId: Optional[int] = None
    users: Optional[List[dict]] = None  # API returns list of user objects (dicts), not strings
    bookingUrl: Optional[str] = None
    lengthInMinutesOptions: Optional[List[int]] = None
    slotInterval: Optional[int] = None
    minimumBookingNotice: Optional[int] = None
    beforeEventBuffer: Optional[int] = None
    afterEventBuffer: Optional[int] = None
    seatsPerTimeSlot: Optional[dict] = None
    seatsShowAvailabilityCount: Optional[bool] = None
    bookingLimitsCount: Optional[dict] = None
    bookerActiveBookingsLimit: Optional[dict] = None
    onlyShowFirstAvailableSlot: Optional[bool] = None
    bookingLimitsDuration: Optional[dict] = None
    bookingWindow: Optional[dict] = None  # API returns dict like {'disabled': True}, not List[dict]
    bookerLayouts: Optional[dict] = None
    confirmationPolicy: Optional[dict] = None
    requiresBookerEmailVerification: Optional[bool] = None
    hideCalendarNotes: Optional[bool] = None
    color: Optional[dict] = None
    seats: Optional[dict] = None
    offsetStart: Optional[int] = None
    customName: Optional[str] = None
    destinationCalendar: Optional[dict] = None
    useDestinationCalendarEmail: Optional[bool] = None
    hideCalendarEventDetails: Optional[bool] = None
    hideOrganizerEmail: Optional[bool] = None
    calVideoSettings: Optional[dict] = None
    disableCancelling: Optional[dict] = None
    disableRescheduling: Optional[dict] = None
    interfaceLanguage: Optional[str] = None
    allowReschedulingPastBookings: Optional[bool] = None
    allowReschedulingCancelledBookings: Optional[bool] = None
    showOptimizedSlots: Optional[bool] = None
    
    class Config:
        extra = "allow"  # Allow extra fields from Cal.com API


class CalComBookingsResponse(BaseModel):
    bookings: List[CalComBooking]
    total: Optional[int] = None
    nextCursor: Optional[int] = None


class CalComEventTypesResponse(BaseModel):
    event_types: List[CalComEventType]


class CalendlyStatus(BaseModel):
    connected: bool
    account_email: Optional[str] = None
    account_name: Optional[str] = None
    message: Optional[str] = None


class CalendlyScheduledEvent(BaseModel):
    """Calendly scheduled event (booking)"""
    uri: str  # Unique identifier URI
    name: str  # Event name
    status: str  # active, canceled, etc.
    start_time: str  # ISO 8601 datetime
    end_time: str  # ISO 8601 datetime
    event_type: Optional[str] = None  # URI to event type
    location: Optional[dict] = None  # Location details
    invitees_counter: Optional[dict] = None  # { total: int, active: int, limit: int }
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    event_memberships: Optional[List[dict]] = None  # Host/organizer info
    event_guests: Optional[List[dict]] = None
    calendar_event: Optional[dict] = None
    tracking: Optional[dict] = None
    invitees: Optional[List[dict]] = None  # Detailed invitee information with form responses
    routingFormSubmissions: Optional[List[dict]] = None  # Routing form submissions matched by event URI or email
    
    class Config:
        extra = "allow"  # Allow extra fields from Calendly API


class CalendlyEventType(BaseModel):
    """Calendly event type"""
    uri: str  # Unique identifier URI
    name: str  # Event type name
    active: bool
    slug: str
    scheduling_url: Optional[str] = None
    duration: Optional[int] = None  # Duration in minutes
    kind: Optional[str] = None  # "solo", "group", "collective", etc.
    pooling_type: Optional[str] = None
    type: Optional[str] = None  # "StandardEventType", "AdhocEventType", etc.
    color: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    internal_note: Optional[str] = None
    description_plain: Optional[str] = None
    description_html: Optional[str] = None
    profile: Optional[dict] = None  # User profile info
    secret: Optional[bool] = None
    booking_questions: Optional[List[dict]] = None
    custom_questions: Optional[List[dict]] = None
    
    class Config:
        extra = "allow"  # Allow extra fields from Calendly API


class CalendlyScheduledEventsResponse(BaseModel):
    collection: List[CalendlyScheduledEvent]
    pagination: Optional[dict] = None  # { count: int, next_page: str, previous_page: str, next_page_token: str }


class CalendlyEventTypesResponse(BaseModel):
    collection: List[CalendlyEventType]
    pagination: Optional[dict] = None


class CalendarUpcomingAppointment(BaseModel):
    """Details of the most upcoming appointment"""
    id: Optional[str] = None  # Booking/event ID
    title: str  # Event name/title
    start_time: str  # ISO 8601 datetime
    end_time: Optional[str] = None  # ISO 8601 datetime
    link: Optional[str] = None  # Link to view/edit the appointment
    provider: str  # "calcom" or "calendly" or "manual"
    attendees: Optional[List[dict]] = None  # Attendee information
    location: Optional[str] = None  # Location details
    client_name: Optional[str] = None  # Client/contact name (for manual check-ins)


class CalendarNotificationsSummary(BaseModel):
    """Summary of upcoming calendar appointments with comparisons"""
    upcoming_count: int  # Number of upcoming appointments
    last_week_count: int  # Number of appointments in the last 7 days
    last_month_count: int  # Number of appointments in the last 30 days
    last_week_percentage_change: Optional[float] = None  # Percentage change vs last week
    last_month_percentage_change: Optional[float] = None  # Percentage change vs last month
    most_upcoming: Optional[CalendarUpcomingAppointment] = None  # The closest upcoming appointment
    upcoming_appointments: Optional[List[CalendarUpcomingAppointment]] = None  # Up to 3 upcoming appointments (including manual check-ins)
    provider: Optional[str] = None  # "calcom" or "calendly" or None if not connected
    connected: bool  # Whether a calendar provider is connected

