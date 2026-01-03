from pydantic import BaseModel, Field
from typing import Optional, Literal, List
from datetime import datetime
from decimal import Decimal


class ZipCodeEntry(BaseModel):
    zip_code: str
    country_code: str


class CollectionProgress(BaseModel):
    job_id: str = Field(default="historical_collection")
    zipcodes: List[ZipCodeEntry]
    total_items: int = Field(ge=0)
    completed_items: int = Field(ge=0, default=0)
    remaining_items: int = Field(ge=0)
    daily_calls_limit: int = Field(default=950)
    daily_calls_used: int = Field(ge=0, le=1000, default=0)
    last_run: Optional[str] = None
    status: Literal["in_progress", "completed", "paused"]
    started_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class CollectionQueueItem(BaseModel):
    item_id: str = Field(pattern=r"^\d{5}#[A-Z]{2}#\d{4}-\d{2}-\d{2}$")
    zip_code: str = Field(pattern=r"^\d{5}$")
    country_code: str = Field(pattern=r"^[A-Z]{2}$")
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    status: Literal["pending", "completed", "failed"] = "pending"
    retry_count: int = Field(ge=0, le=3, default=0)
    last_attempt: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None


class CollectionGeocodeCache(BaseModel):
    zip_code: str = Field(pattern=r"^\d{5}$")
    country_code: str = Field(pattern=r"^[A-Z]{2}$")
    latitude: Decimal
    longitude: Decimal
    name: Optional[str]
    country: Optional[str]
