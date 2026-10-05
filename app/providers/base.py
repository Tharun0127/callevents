from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# The internal vocabulary every provider is normalised into.
EVENT_TYPES = frozenset(
    {"call.initiated", "call.ringing", "call.answered", "call.completed", "call.failed"}
)


class NormalizedEvent(BaseModel):
    provider: str
    provider_event_id: str = Field(min_length=1, max_length=128)
    account_id: str = Field(min_length=1, max_length=128)
    event_type: str
    call_id: str = Field(min_length=1, max_length=128)
    occurred_at: datetime | None
    payload: dict[str, Any]


class SignatureError(Exception):
    pass


class Provider(ABC):
    name: str

    @abstractmethod
    def verify(self, headers: Mapping[str, str], body: bytes) -> None:
        """Raise SignatureError unless the body was signed with this provider's secret."""

    @abstractmethod
    def parse(self, body: bytes) -> NormalizedEvent:
        """Validate the provider shape (raises pydantic.ValidationError) and normalise it."""
