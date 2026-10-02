import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments.application.events import OutboxMessage
from payments.domain import Payment
from payments.infrastructure.db.mappers import (
    outbox_to_record,
    payment_to_domain,
    payment_to_record,
    sync_payment_record,
)
from payments.infrastructure.db.models import PaymentRecord


class SqlAlchemyPaymentRepository:
    """Отдает доменные объекты и запоминает пары (сущность, запись).

    Перед commit UnitOfWork вызывает sync(), и изменения сущностей
    переносятся в записи. Источник правды - последний выданный объект:
    get_for_update перечитывает строку и заменяет ранее выданную сущность.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._tracked: dict[uuid.UUID, tuple[Payment, PaymentRecord]] = {}

    async def add(self, payment: Payment) -> None:
        record = payment_to_record(payment)
        self._session.add(record)
        self._tracked[payment.id] = (payment, record)

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        record = await self._session.get(PaymentRecord, payment_id)
        return self._track(record)

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        stmt = (
            select(PaymentRecord)
            .where(PaymentRecord.id == payment_id)
            .with_for_update()
            # Строку могли изменить между прошлым чтением и захватом блокировки.
            .execution_options(populate_existing=True)
        )
        return self._track(await self._session.scalar(stmt))

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = select(PaymentRecord).where(PaymentRecord.idempotency_key == key)
        return self._track(await self._session.scalar(stmt))

    def sync(self) -> None:
        for payment, record in self._tracked.values():
            sync_payment_record(payment, record)

    def _track(self, record: PaymentRecord | None) -> Payment | None:
        if record is None:
            return None
        payment = payment_to_domain(record)
        self._tracked[payment.id] = (payment, record)
        return payment


class SqlAlchemyOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, message: OutboxMessage) -> None:
        self._session.add(outbox_to_record(message))
