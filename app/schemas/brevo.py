from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime


class BrevoContact(BaseModel):
    id: Optional[int] = None
    email: str  # Using str instead of EmailStr for flexibility
    attributes: Optional[Dict[str, Any]] = None
    emailBlacklisted: Optional[bool] = None
    smsBlacklisted: Optional[bool] = None
    listIds: Optional[List[int]] = None
    unlinkListIds: Optional[List[int]] = None
    smtpBlacklistSender: Optional[List[str]] = None
    updateEnabled: Optional[bool] = None
    smtpBlacklistSenderDate: Optional[datetime] = None


class BrevoContactCreate(BaseModel):
    email: str  # Using str instead of EmailStr for flexibility
    attributes: Optional[Dict[str, Any]] = None
    listIds: Optional[List[int]] = None
    updateEnabled: Optional[bool] = True


class BrevoContactUpdate(BaseModel):
    attributes: Optional[Dict[str, Any]] = None
    listIds: Optional[List[int]] = None
    unlinkListIds: Optional[List[int]] = None


class BrevoContactResponse(BaseModel):
    id: int
    email: Optional[str] = None  # Email might not always be present (can be in attributes)
    attributes: Optional[Dict[str, Any]] = None
    emailBlacklisted: Optional[bool] = None
    smsBlacklisted: Optional[bool] = None
    listIds: Optional[List[int]] = None
    createdAt: Optional[str] = None
    modifiedAt: Optional[str] = None


class BrevoContactList(BaseModel):
    contacts: List[BrevoContactResponse]
    count: int
    offset: Optional[int] = None
    limit: Optional[int] = None


class BrevoList(BaseModel):
    id: Optional[int] = None
    name: str
    folderId: Optional[int] = None


class BrevoListCreate(BaseModel):
    name: str
    folderId: Optional[int] = None


class BrevoListResponse(BaseModel):
    id: int
    name: str
    folderId: Optional[int] = None
    uniqueSubscribers: Optional[int] = None
    doubleOptin: Optional[bool] = None
    createdAt: Optional[str] = None


class BrevoListList(BaseModel):
    lists: List[BrevoListResponse]
    count: int
    offset: Optional[int] = None
    limit: Optional[int] = None


class BrevoMoveContactsRequest(BaseModel):
    contactIds: List[int]
    sourceListId: int
    destinationListId: int


class BrevoAddContactsToListRequest(BaseModel):
    contactIds: List[int]
    listId: int


class BrevoRemoveContactsFromListRequest(BaseModel):
    contactIds: List[int]
    listId: int


class BrevoBulkDeleteContactsRequest(BaseModel):
    contactIds: List[int]


class BrevoCreateClientsFromContactsRequest(BaseModel):
    contactIds: List[int]


class BrevoListContactsRequest(BaseModel):
    listId: int
    limit: Optional[int] = 50
    offset: Optional[int] = 0


class BrevoEmailRecipient(BaseModel):
    email: str
    name: Optional[str] = None


class BrevoSendEmailRequest(BaseModel):
    # Recipients - can specify one of these
    contactIds: Optional[List[int]] = None  # Send to specific contact IDs
    listId: Optional[int] = None  # Send to entire list
    recipients: Optional[List[BrevoEmailRecipient]] = None  # Direct email addresses
    
    # Email content
    sender: Dict[str, str]  # {"email": "...", "name": "..."}
    subject: str
    htmlContent: Optional[str] = None
    textContent: Optional[str] = None
    templateId: Optional[int] = None  # Use template instead of content
    
    # Optional parameters
    params: Optional[Dict[str, Any]] = None  # Template parameters
    tags: Optional[List[str]] = None
    replyTo: Optional[Dict[str, str]] = None
    attachments: Optional[List[Dict[str, Any]]] = None  # [{"name": "...", "content": "base64..."}]


class BrevoSendEmailResponse(BaseModel):
    success: bool
    messageId: Optional[str] = None
    message: str
    recipientsCount: Optional[int] = None


# Analytics Schemas
class BrevoCampaignStatistics(BaseModel):
    """Statistics for a single email campaign"""
    campaignId: Optional[int] = None
    campaignName: Optional[str] = None
    sent: Optional[int] = 0
    delivered: Optional[int] = 0
    opened: Optional[int] = 0
    uniqueOpens: Optional[int] = 0
    clicked: Optional[int] = 0
    uniqueClicks: Optional[int] = 0
    bounced: Optional[int] = 0
    unsubscribed: Optional[int] = 0
    spamReports: Optional[int] = 0
    openRate: Optional[float] = 0.0
    clickRate: Optional[float] = 0.0
    bounceRate: Optional[float] = 0.0
    createdAt: Optional[str] = None


class BrevoTransactionalStatistics(BaseModel):
    """Transactional email statistics"""
    sent: Optional[int] = 0
    delivered: Optional[int] = 0
    opened: Optional[int] = 0
    uniqueOpens: Optional[int] = 0
    clicked: Optional[int] = 0
    uniqueClicks: Optional[int] = 0
    bounced: Optional[int] = 0
    spamReports: Optional[int] = 0
    openRate: Optional[float] = 0.0
    clickRate: Optional[float] = 0.0
    bounceRate: Optional[float] = 0.0
    period: Optional[str] = None  # e.g., "30days", "7days"


class BrevoAccountStatistics(BaseModel):
    """Overall account statistics"""
    totalContacts: Optional[int] = 0
    totalLists: Optional[int] = 0
    totalCampaigns: Optional[int] = 0
    totalSent: Optional[int] = 0
    totalDelivered: Optional[int] = 0
    totalOpened: Optional[int] = 0
    totalClicked: Optional[int] = 0
    totalBounced: Optional[int] = 0
    totalUnsubscribed: Optional[int] = 0
    overallOpenRate: Optional[float] = 0.0
    overallClickRate: Optional[float] = 0.0
    overallBounceRate: Optional[float] = 0.0


class BrevoAnalyticsResponse(BaseModel):
    """Complete analytics response"""
    account: Optional[BrevoAccountStatistics] = None
    transactional: Optional[BrevoTransactionalStatistics] = None
    campaigns: Optional[List[BrevoCampaignStatistics]] = []
    lastUpdated: Optional[str] = None
    period: Optional[str] = "30days"  # Default to 30 days

