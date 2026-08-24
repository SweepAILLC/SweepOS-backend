"""FastAPI exception handler for LLM errors."""
from fastapi import Request, status
from fastapi.responses import JSONResponse

from app.core.llm_exceptions import (
    LLMBudgetExceededError,
    LLMException,
    LLMSlotUnavailableError,
    LLMTimeoutError,
)


async def llm_exception_handler(request: Request, exc: LLMException) -> JSONResponse:
    """Convert LLM exceptions to proper HTTP responses with user-friendly messages."""
    if isinstance(exc, LLMSlotUnavailableError):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": exc.user_message},
            headers={"Retry-After": "30"},
        )
    
    if isinstance(exc, LLMBudgetExceededError):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": exc.user_message},
            headers={"Retry-After": "60"},
        )
    
    if isinstance(exc, LLMTimeoutError):
        return JSONResponse(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            content={"detail": exc.user_message},
        )
    
    # Generic LLM error
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": exc.user_message or "AI service temporarily unavailable"},
    )
