from payments.application.events import OutboxMessage
from payments.domain import Currency, Money, Payment, PaymentStatus
from payments.infrastructure.db.models import OutboxRecord, PaymentRecord


def payment_to_domain(record: PaymentRecord) -> Payment:
    return Payment(
        id=record.id,
        money=Money(record.amount, Currency(record.currency)),
        description=record.description,
        metadata=dict(record.metadata_),
        idempotency_key=record.idempotency_key,
        request_fingerprint=record.request_fingerprint,
        webhook_url=record.webhook_url,
        created_at=record.created_at,
        status=PaymentStatus(record.status),
        processed_at=record.processed_at,
        webhook_delivered_at=record.webhook_delivered_at,
    )


def payment_to_record(payment: Payment) -> PaymentRecord:
    return PaymentRecord(
        id=payment.id,
        amount=payment.money.amount,
        currency=payment.money.currency.value,
        description=payment.description,
        metadata_=dict(payment.metadata),
        status=payment.status.value,
        idempotency_key=payment.idempotency_key,
        request_fingerprint=payment.request_fingerprint,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
    )


def sync_payment_record(payment: Payment, record: PaymentRecord) -> None:
    """Переносит изменяемое состояние сущности в запись.

    Неизменяемые поля (сумма, ключ, url) не трогаем. Присвоение равного
    значения SQLAlchemy не считает изменением, лишних UPDATE не будет.
    """
    record.status = payment.status.value
    record.processed_at = payment.processed_at
    record.webhook_delivered_at = payment.webhook_delivered_at


def outbox_to_record(message: OutboxMessage) -> OutboxRecord:
    return OutboxRecord(
        id=message.id,
        topic=message.topic,
        payload=dict(message.payload),
        created_at=message.created_at,
        next_attempt_at=message.created_at,
        attempts=0,
    )
