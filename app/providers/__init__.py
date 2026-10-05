from functools import lru_cache

from app.config import get_settings
from app.providers.acmetel import AcmeTel
from app.providers.base import EVENT_TYPES, NormalizedEvent, Provider, SignatureError
from app.providers.voxly import Voxly

__all__ = ["EVENT_TYPES", "NormalizedEvent", "Provider", "SignatureError", "get_provider"]


@lru_cache
def _registry() -> dict[str, Provider]:
    s = get_settings()
    return {
        "acmetel": AcmeTel(s.acmetel_signing_secret),
        "voxly": Voxly(s.voxly_signing_secret, s.voxly_signature_tolerance_seconds),
    }


def get_provider(name: str) -> Provider | None:
    return _registry().get(name)
