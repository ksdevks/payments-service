import logging
import uuid
from dataclasses import dataclass

from payments.application.errors import PaymentNotFoundError
from payments.application.events import PaymentCreated
from payments.application.notifications import PaymentNotification
from payments.application.ports import Clock, PaymentGateway, UnitOfWorkFactory, WebhookSender
from payments.domain import ChargeOutcome, Payment, PaymentStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProcessPaymentResult:
    status: PaymentStatus
    # Этот вызов зафиксировал результат списания. False, если результат уже был
    # сохранен ранее или параллельным обработчиком (шлюз при этом мог вызываться).
    outcome_applied: bool
    webhook_sent: bool


class ProcessPayment:
    """Обработка события payments.new. Идемпотентна по каждому шагу.

    Шаги фиксируются в БД по отдельности, поэтому повтор сообщения после
    сбоя продолжает с места падения: если упала только доставка webhook,
    шлюз повторно не вызывается.

    Шлюз вызывается вне транзакции, поэтому в двух случаях он может быть
    вызван повторно для того же платежа: параллельная обработка дубля
    сообщения и сбой коммита после ответа шлюза. Однократность списания
    в этих случаях обеспечивает контракт PaymentGateway: вызов идемпотентен
    по payment.id и возвращает ранее зафиксированный результат.
    """

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        gateway: PaymentGateway,
        webhooks: WebhookSender,
        clock: Clock,
    ) -> None:
        self._uow_factory = uow_factory
        self._gateway = gateway
        self._webhooks = webhooks
        self._clock = clock

    async def execute(self, event: PaymentCreated) -> ProcessPaymentResult:
        payment = await self._load(event.payment_id)

        outcome_applied = False
        if payment.is_pending:
            # Вызов шлюза вне транзакции: не держим блокировку на время сети.
            outcome = await self._gateway.charge(payment)
            payment, outcome_applied = await self._apply_outcome(payment.id, outcome)

        if not payment.needs_webhook:
            return ProcessPaymentResult(payment.status, outcome_applied, webhook_sent=False)

        await self._webhooks.send(payment.webhook_url, PaymentNotification.from_payment(payment))
        await self._mark_webhook_delivered(payment.id)
        return ProcessPaymentResult(payment.status, outcome_applied, webhook_sent=True)

    async def is_completed(self, event: PaymentCreated) -> bool:
        """Платеж обработан и клиент уведомлен. Только чтение."""
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(event.payment_id)
        return payment is not None and not payment.is_pending and not payment.needs_webhook

    async def _load(self, payment_id: uuid.UUID) -> Payment:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(f"payment {payment_id} not found")
        return payment

    async def _apply_outcome(
        self, payment_id: uuid.UUID, outcome: ChargeOutcome
    ) -> tuple[Payment, bool]:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get_for_update(payment_id)
            if payment is None:
                raise PaymentNotFoundError(f"payment {payment_id} not found")
            if not payment.is_pending:
                # Дубль сообщения обработан параллельно и закоммичен раньше.
                # Зафиксированный результат главный, наш отбрасываем.
                logger.warning(
                    "payment %s already processed concurrently, outcome %s discarded",
                    payment_id,
                    outcome,
                )
                return payment, False
            payment.apply_charge_outcome(outcome, self._clock.now())
            await uow.commit()
            return payment, True

    async def _mark_webhook_delivered(self, payment_id: uuid.UUID) -> None:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get_for_update(payment_id)
            if payment is None:
                raise PaymentNotFoundError(f"payment {payment_id} not found")
            payment.mark_webhook_delivered(self._clock.now())
            await uow.commit()
