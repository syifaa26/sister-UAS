from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, Any, List

class EventPayload(BaseModel):
    topic: str
    event_id: str = Field(..., description="Unique string for dedup")
    timestamp: datetime
    source: str
    payload: Dict[str, Any]

class PublishRequest(BaseModel):
    events: List[EventPayload]

class EventResponse(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]
    processed_at: datetime
