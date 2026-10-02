import uuid
from dataclasses import dataclass

from payments.application.commands import CreatePaymentCommand
from payments.application.errors import (
    DuplicateIdempotencyKeyError,
    IdempotencyKeyConflictError,
)
from payments.application.events import OutboxMessage, PaymentCreated
from payments.application.idempotency import request_fingerprint
from payments.application.ports import Clock, UnitOfWorkFactory
from payments.domain import Money, Payment


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool


class CreatePayment:
    def __init__(self, uow_factory: UnitOfWorkFactory, clock: Clock) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    async def execute(self, command: CreatePaymentCommand) -> CreatePaymentResult:
        fingerprint = request_fingerprint(command)

        # Быстрый путь для повтора. Корректность при гонке обеспечивает
        # не эта проверка, а уникальный индекс в БД (ниже).
        existing = await self._find_by_key(command.idempotency_key)
        if existing is not None:
            return self._replay(existing, fingerprint)

        now = self._clock.now()
        payment = Payment.create(
            money=Money(command.amount, command.currency),
            description=command.description,
            metadata=command.metadata,
            idempotency_key=command.idempotency_key,
            request_fingerprint=fingerprint,
            webhook_url=command.webhook_url,
            now=now,
        )
        event = PaymentCreated(event_id=uuid.uuid4(), payment_id=payment.id)

        try:
            async with self._uow_factory() as uow:
                await uow.payments.add(payment)
                await uow.outbox.add(OutboxMessage.for_payment_created(event, now))
                await uow.commit()
        except DuplicateIdempotencyKeyError:
            # Параллельный запрос с тем же ключом успел закоммитить первым.
            existing = await self._find_by_key(command.idempotency_key)
            if existing is None:
                raise
            return self._replay(existing, fingerprint)

        return CreatePaymentResult(payment=payment, created=True)

    async def _find_by_key(self, key: str) -> Payment | None:
        async with self._uow_factory() as uow:
            return await uow.payments.get_by_idempotency_key(key)

    @staticmethod
    def _replay(existing: Payment, fingerprint: str) -> CreatePaymentResult:
        if existing.request_fingerprint != fingerprint:
            raise IdempotencyKeyConflictError(
                "Idempotency-Key was already used with a different request"
            )
        return CreatePaymentResult(payment=existing, created=False)
