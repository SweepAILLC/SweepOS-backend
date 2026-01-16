from pydantic import BaseModel


class OAuthStartResponse(BaseModel):
    redirect_url: str


class OAuthTokenResponse(BaseModel):
    success: bool
    message: str


class DirectApiKeyRequest(BaseModel):
    api_key: str

