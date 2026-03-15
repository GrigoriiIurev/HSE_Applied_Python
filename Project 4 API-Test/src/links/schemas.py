from datetime import datetime
from pydantic import BaseModel


class LinkCreate(BaseModel):
    original_url: str
    custom_alias: str | None = None
    expires_at: datetime | None = None


class LinkUpdate(BaseModel):
    original_url: str


class LinkResponse(BaseModel):
    original_url: str
    short_code: str
    custom_alias: str | None = None
    created_at: datetime
    expires_at: datetime | None = None


class LinkStats(BaseModel):
    original_url: str
    created_at: datetime
    click_count: int
    last_used_at: datetime | None = None