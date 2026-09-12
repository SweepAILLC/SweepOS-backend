from pydantic import BaseModel


class OAuthStartResponse(BaseModel):
    redirect_url: str


class OAuthTokenResponse(BaseModel):
    success: bool
    message: str


class DirectApiKeyRequest(BaseModel):
    api_key: str


class DiscordChannelMappingRequest(BaseModel):
    channel_id: str
    channel_name: str | None = None

