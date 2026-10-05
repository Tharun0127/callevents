import uuid
from datetime import datetime
from typing import Any

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

from app.models import DeliveryStatus
from app.providers import EVENT_TYPES


class IngestResponse(BaseModel):
    event_id: uuid.UUID
    duplicate: bool


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    provider: str
    provider_event_id: str
    event_type: str
    call_id: str
    occurred_at: datetime | None
    received_at: datetime
    payload: dict[str, Any]


class EventPage(BaseModel):
    data: list[EventOut]
    next_cursor: str | None


class DeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    status: DeliveryStatus
    attempt_count: int
    next_retry_at: datetime | None
    last_response_code: int | None
    last_error: str | None
    delivered_at: datetime | None
    created_at: datetime


class DeliveryPage(BaseModel):
    data: list[DeliveryOut]
    next_cursor: str | None


def _validate_event_types(value: list[str]) -> list[str]:
    unknown = [t for t in value if t != "*" and t not in EVENT_TYPES]
    if unknown:
        raise ValueError(f"unknown event types {unknown}; allowed: {sorted(EVENT_TYPES)} or '*'")
    if not value:
        raise ValueError("subscribe to at least one event type")
    return sorted(set(value))


class EndpointCreate(BaseModel):
    url: AnyHttpUrl
    event_types: list[str] = Field(default_factory=lambda: ["*"])
    active: bool = True
    rate_per_second: float | None = Field(default=None, gt=0, le=10_000)
    burst: int | None = Field(default=None, gt=0, le=100_000)
    secret: str | None = Field(
        default=None, min_length=16, description="Omit to have one generated."
    )

    _check_types = field_validator("event_types")(_validate_event_types)


class EndpointUpdate(BaseModel):
    url: AnyHttpUrl | None = None
    event_types: list[str] | None = None
    active: bool | None = None
    rate_per_second: float | None = Field(default=None, gt=0, le=10_000)
    burst: int | None = Field(default=None, gt=0, le=100_000)

    @field_validator("event_types")
    @classmethod
    def _types(cls, value: list[str] | None) -> list[str] | None:
        return None if value is None else _validate_event_types(value)


class EndpointOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    url: str
    event_types: list[str]
    active: bool
    rate_per_second: float | None
    burst: int | None
    created_at: datetime
    updated_at: datetime


class EndpointCreated(EndpointOut):
    secret: str = Field(description="Shown once. Use it to verify X-Signature.")


class ReplayResponse(BaseModel):
    delivery_id: uuid.UUID
    status: DeliveryStatus
