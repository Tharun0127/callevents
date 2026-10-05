import pytest
from pydantic import ValidationError

from app.config import Settings

REAL_SECRETS = {
    "acmetel_signing_secret": "prod-acme",
    "voxly_signing_secret": "prod-voxly",
}


def test_development_accepts_default_secrets() -> None:
    Settings(_env_file=None, app_env="development")


def test_production_rejects_default_provider_secrets() -> None:
    with pytest.raises(ValidationError, match="ACMETEL_SIGNING_SECRET"):
        Settings(_env_file=None, app_env="production", seed_demo_data=False)


def test_production_rejects_public_demo_key_when_seeding() -> None:
    with pytest.raises(ValidationError, match="DEMO_API_KEY"):
        Settings(_env_file=None, app_env="production", seed_demo_data=True, **REAL_SECRETS)


def test_production_with_real_secrets_and_no_seed_starts() -> None:
    s = Settings(_env_file=None, app_env="PRODUCTION", seed_demo_data=False, **REAL_SECRETS)
    assert s.app_env == "PRODUCTION"
