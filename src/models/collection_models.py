from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime


class CollectionProgress(BaseModel):
    job_id: str = Field(default="historical_collection")
    total_items: int = Field(gt=0)
    completed_items: int = Field(ge=0, default=0)
    remaining_items: int = Field(ge=0)
    daily_calls_limit: int = Field(default=950)
    daily_calls_used: int = Field(ge=0, le=1000, default=0)
    last_run: Optional[datetime] = None
    status: Literal["in_progress", "completed", "paused"]
    started_at: datetime = Field(default_factory=datetime.now())


class CollectionQueueItem(BaseModel):
    item_id: str = Field(pattern=r"^\d{5}#\d{4}-\d{2}-\d{2}$")
    zipcode: str = Field(pattern=r"^\d{5}$")
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    status: Literal["pending", "completed", "failed"] = "pending"
    retry_count: int = Field(ge=0, le=3, default=0)
    last_attempt: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
