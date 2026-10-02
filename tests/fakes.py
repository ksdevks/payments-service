"""In-memory реализации портов с семантикой транзакций.

Репозиторий отдает копии, изменения попадают в хранилище только на commit(),
выход без commit() - rollback. Иначе тесты use case'ов проходили бы даже
при забытом commit.
"""

from __future__ import annotations

import copy
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Self

from payments.application.errors import DuplicateIdempotencyKeyError
from payments.application.events import OutboxMessage
from payments.application.notifications import PaymentNotification
from payments.domain import ChargeOutcome, Payment


@dataclass
class InMemoryStore:
    payments: dict[uuid.UUID, Payment] = field(default_factory=dict)
    outbox: list[OutboxMessage] = field(default_factory=list)
    commits: int = 0
    # Сколько следующих commit() завершатся ошибкой: эмуляция сбоя БД.
    fail_commits: int = 0


class FakePaymentRepository:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store
        self.seen: dict[uuid.UUID, Payment] = {}
        self.added: list[Payment] = []

    async def add(self, payment: Payment) -> None:
        self.added.append(payment)

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return self._track(self._store.payments.get(payment_id))

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        return await self.get(payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        found = next(
            (p for p in self._store.payments.values() if p.idempotency_key == key), None
        )
        return self._track(found)

    def _track(self, payment: Payment | None) -> Payment | None:
        if payment is None:
            return None
        if payment.id not in self.seen:
            self.seen[payment.id] = copy.deepcopy(payment)
        return self.seen[payment.id]


class FakeOutboxRepository:
    def __init__(self) -> None:
        self.added: list[OutboxMessage] = []

    async def add(self, message: OutboxMessage) -> None:
        self.added.append(message)


class FakeUnitOfWork:
    def __init__(self, store: InMemoryStore) -> None:
        self._store = store
        self.payments = FakePaymentRepository(store)
        self.outbox = FakeOutboxRepository()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    async def commit(self) -> None:
        if self._store.fail_commits:
            self._store.fail_commits -= 1
            raise ConnectionError("commit failed")
        for new in self.payments.added:
            if any(
                p.idempotency_key == new.idempotency_key and p.id != new.id
                for p in self._store.payments.values()
            ):
                raise DuplicateIdempotencyKeyError(new.idempotency_key)
        for payment in [*self.payments.seen.values(), *self.payments.added]:
            self._store.payments[payment.id] = copy.deepcopy(payment)
        self._store.outbox.extend(self.outbox.added)
        self._store.commits += 1


class FixedClock:
    def __init__(self, start: datetime | None = None) -> None:
        self.current = start or datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class FakeGateway:
    """Идемпотентный по payment.id шлюз, как требует контракт PaymentGateway.

    calls - все обращения, effects - фактические списания: результат
    фиксируется при первом успешном обращении и возвращается на повторах.
    Ошибка из outcomes эмулирует технический сбой без списания.
    """

    def __init__(self, outcomes: Iterable[ChargeOutcome | Exception] = ()) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[uuid.UUID] = []
        self.effects: dict[uuid.UUID, ChargeOutcome] = {}

    async def charge(self, payment: Payment) -> ChargeOutcome:
        self.calls.append(payment.id)
        if payment.id in self.effects:
            return self.effects[payment.id]
        result = self._outcomes.pop(0) if self._outcomes else ChargeOutcome.SUCCEEDED
        if isinstance(result, Exception):
            raise result
        self.effects[payment.id] = result
        return result


class FakeWebhookSender:
    def __init__(self, failures: Iterable[Exception] = ()) -> None:
        self._failures = list(failures)
        self.attempts: list[tuple[str, PaymentNotification]] = []

    async def send(self, url: str, notification: PaymentNotification) -> None:
        self.attempts.append((url, notification))
        if self._failures:
            raise self._failures.pop(0)


class InMemoryChargeLedger:
    def __init__(self) -> None:
        self.records: dict[uuid.UUID, ChargeOutcome] = {}

    async def get(self, payment_id: uuid.UUID) -> ChargeOutcome | None:
        return self.records.get(payment_id)

    async def record(self, payment_id: uuid.UUID, outcome: ChargeOutcome) -> ChargeOutcome:
        return self.records.setdefault(payment_id, outcome)
