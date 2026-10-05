import hashlib
import hmac
import secrets

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError

# Argon2id with the library defaults (t=3, m=64MiB, p=4). Verification costs tens of
# milliseconds by design, so verified keys are cached (see app.auth) keyed by a SHA256 digest.
_hasher = PasswordHasher()

API_KEY_PREFIX = "ck_"


def generate_api_key() -> tuple[str, str]:
    """Return (key_id, full_key). key_id is stored in clear to locate the tenant row."""
    key_id = secrets.token_hex(4)
    return key_id, f"{API_KEY_PREFIX}{key_id}_{secrets.token_hex(16)}"


def parse_key_id(api_key: str) -> str | None:
    if not api_key.startswith(API_KEY_PREFIX):
        return None
    parts = api_key[len(API_KEY_PREFIX) :].split("_", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    return parts[0]


def hash_api_key(api_key: str) -> str:
    return _hasher.hash(api_key)


def verify_api_key(api_key: str, hashed: str) -> bool:
    try:
        return _hasher.verify(hashed, api_key)
    except (VerificationError, InvalidHashError):
        return False


def api_key_digest(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


def hmac_sha256_hex(secret: str, message: bytes) -> str:
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def signatures_match(expected: str, provided: str) -> bool:
    """Constant time comparison, so timing does not leak how many leading bytes matched."""
    return hmac.compare_digest(expected.encode(), provided.encode())


def generate_endpoint_secret() -> str:
    return f"whsec_{secrets.token_urlsafe(24)}"
