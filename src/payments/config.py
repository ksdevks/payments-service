from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Конфигурация из переменных окружения. Секреты обязательны, без дефолтов."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: SecretStr = Field(min_length=16)
    database_url: str
    rabbitmq_url: str

    db_pool_size: int = Field(default=10, ge=1)
    # Предел времени одного запроса к БД (asyncpg command_timeout и таймаут
    # подключения). Relay использует его же как общий дедлайн операции с
    # хранилищем, включая ожидание соединения и коммит.
    db_command_timeout: float = Field(default=5.0, gt=0)
    log_level: str = "INFO"
    # Swagger и OpenAPI выключены по умолчанию: ТЗ требует ключ на всех эндпоинтах.
    api_docs_enabled: bool = False

    # Доставка сообщений consumer'у
    consumer_prefetch: int = Field(default=10, ge=1)
    max_delivery_attempts: int = Field(default=3, ge=1)
    retry_base_delay_ms: int = Field(default=1000, ge=1)
    # Брокерный предел возвратов в очередь (x-delivery-limit) - страховка
    # на случай, если до политики приложения сообщение не доходит.
    max_redeliveries: int = Field(default=10, ge=1)

    # Outbox relay
    outbox_batch_size: int = Field(default=20, ge=1)
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    outbox_lease_seconds: float = Field(default=60.0, gt=0)
    outbox_retry_base: float = Field(default=1.0, gt=0)
    outbox_retry_max: float = Field(default=60.0, gt=0)
    publish_timeout: float = Field(default=2.0, gt=0)

    # Внешние вызовы
    webhook_timeout: float = Field(default=10.0, gt=0)
    # Хосты, которым разрешено резолвиться во внутреннюю сеть, через запятую.
    webhook_allowed_hosts: str = ""
    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    @property
    def relay_step_budget(self) -> float:
        return self.publish_timeout + self.db_command_timeout

    @property
    def webhook_allowlist(self) -> frozenset[str]:
        hosts = (h.strip().lower() for h in self.webhook_allowed_hosts.split(","))
        return frozenset(h for h in hosts if h)

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        if self.outbox_lease_seconds < 2 * self.relay_step_budget:
            raise ValueError(
                "OUTBOX_LEASE_SECONDS must be at least 2 * (PUBLISH_TIMEOUT + DB_COMMAND_TIMEOUT)"
            )
        if self.max_redeliveries < self.max_delivery_attempts:
            # Иначе брокер отправит сообщение в DLQ раньше, чем исчерпается
            # бюджет приложения, и без диагностики.
            raise ValueError("MAX_REDELIVERIES must be >= MAX_DELIVERY_ATTEMPTS")
        return self
