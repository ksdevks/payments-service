import pytest
from pydantic import ValidationError

from payments.config import Settings

REQUIRED = {
    "api_key": "x" * 32,
    "database_url": "postgresql+asyncpg://u:p@db/payments",
    "rabbitmq_url": "amqp://u:p@mq/",
}


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    # Тесты проверяют дефолты кода, а не окружение машины, на которой запущены.
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)


def make(**overrides: object) -> Settings:
    return Settings(_env_file=None, **{**REQUIRED, **overrides})  # type: ignore[arg-type]


def test_defaults_are_consistent() -> None:
    settings = make()
    assert settings.outbox_lease_seconds >= 2 * settings.relay_step_budget
    assert settings.max_redeliveries >= settings.max_delivery_attempts
    assert settings.api_docs_enabled is False
    assert settings.webhook_allowlist == frozenset()


def test_lease_must_cover_two_relay_steps() -> None:
    with pytest.raises(ValidationError, match="OUTBOX_LEASE_SECONDS"):
        make(outbox_lease_seconds=10, publish_timeout=3, db_command_timeout=5)


def test_broker_redelivery_limit_must_not_undercut_retry_budget() -> None:
    with pytest.raises(ValidationError, match="MAX_REDELIVERIES"):
        make(max_delivery_attempts=5, max_redeliveries=3)


def test_webhook_allowlist_parsing() -> None:
    assert make(webhook_allowed_hosts=" Webhook-Sink, ,hooks.local ").webhook_allowlist == {
        "webhook-sink",
        "hooks.local",
    }


def test_short_api_key_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make(api_key="short")
