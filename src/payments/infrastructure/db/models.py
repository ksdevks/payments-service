"""ORM-модели. Отдельно от домена: схема БД может меняться, не трогая сущности."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from payments.domain import ChargeOutcome, Currency, PaymentStatus

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

# UnitOfWork распознает нарушение именно этого ограничения как повтор ключа.
IDEMPOTENCY_KEY_CONSTRAINT = "uq_payments_idempotency_key"


def _in_list(column: str, values: list[str]) -> str:
    quoted = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({quoted})"


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class PaymentRecord(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name=IDEMPOTENCY_KEY_CONSTRAINT),
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint(_in_list("currency", [c.value for c in Currency]), name="currency_valid"),
        CheckConstraint(_in_list("status", [s.value for s in PaymentStatus]), name="status_valid"),
        CheckConstraint(
            "(status = 'pending') = (processed_at IS NULL)", name="processed_at_matches_status"
        ),
        CheckConstraint(
            "webhook_delivered_at IS NULL OR processed_at IS NOT NULL",
            name="webhook_after_processing",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(500))
    # Атрибут "metadata" зарезервирован в Declarative, колонка называется как в ТЗ.
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(String(16))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_fingerprint: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxRecord(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        # Relay выбирает только неопубликованные строки, индекс остается маленьким.
        Index(
            "ix_outbox_pending",
            "next_attempt_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    topic: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # next_attempt_at служит и расписанием повторов, и арендой строки relay'ем:
    # при захвате он сдвигается вперед, упавший relay не держит строку вечно.
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class EmulatedGatewayChargeRecord(Base):
    """Журнал эмулируемого провайдера. У реального шлюза он живет на его стороне."""

    __tablename__ = "emulated_gateway_charges"
    __table_args__ = (
        CheckConstraint(
            _in_list("outcome", [o.value for o in ChargeOutcome]), name="outcome_valid"
        ),
    )

    payment_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    outcome: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
