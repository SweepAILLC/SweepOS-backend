"""Custom exceptions for LLM usage and rate limiting."""
from typing import Optional


class LLMException(Exception):
    """Base exception for LLM-related errors."""
    
    def __init__(self, message: str, user_message: Optional[str] = None):
        super().__init__(message)
        self.user_message = user_message or message


class LLMSlotUnavailableError(LLMException):
    """Raised when no LLM execution slot is available (too many concurrent requests)."""
    
    def __init__(self):
        super().__init__(
            "LLM slot unavailable",
            "The AI service is currently at capacity. Please try again in a moment."
        )


class LLMBudgetExceededError(LLMException):
    """Raised when organization has exceeded their LLM budget."""
    
    def __init__(self, org_id: Optional[str] = None):
        msg = f"LLM budget exceeded for org {org_id}" if org_id else "LLM budget exceeded"
        super().__init__(
            msg,
            "You've reached the usage limit for AI features. Please try again in a few minutes."
        )


class LLMTimeoutError(LLMException):
    """Raised when LLM request times out."""
    
    def __init__(self):
        super().__init__(
            "LLM request timed out",
            "The AI service took too long to respond. Please try again."
        )
