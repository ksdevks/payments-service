"""Эмулятор платежного шлюза и его журнал списаний."""

from __future__ import annotations

import asyncio
import random
import uuid
from collections.abc import Awaitable, Callable
from typing import Protocol

from payments.domain import ChargeOutcome, Payment


class ChargeLedger(Protocol):
    """Хранилище результатов на стороне эмулируемого провайдера."""

    async def get(self, payment_id: uuid.UUID) -> ChargeOutcome | None: ...

    async def record(self, payment_id: uuid.UUID, outcome: ChargeOutcome) -> ChargeOutcome:
        """Фиксирует результат, если его еще нет, и возвращает зафиксированный.

        При гонке побеждает первая запись: проигравший получает ее результат.
        """
        ...


class EmulatedGateway:
    """Эмуляция платежного шлюза: задержка 2-5 с, 90% успех, 10% отказ банка.

    Выполняет контракт PaymentGateway об идемпотентности так же, как реальный
    провайдер: результат списания фиксируется в журнале по payment.id при
    первом обращении, все повторы (параллельные, после рестарта, после смены
    настроек) возвращают зафиксированный результат без повторной обработки.

    Отказ - бизнес-результат (платеж станет failed, клиент получит webhook),
    а не технический сбой.
    """

    def __init__(
        self,
        ledger: ChargeLedger,
        *,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        success_rate: float = 0.9,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not 0 <= min_delay <= max_delay:
            raise ValueError("expected 0 <= min_delay <= max_delay")
        if not 0 <= success_rate <= 1:
            raise ValueError("success_rate must be within [0, 1]")
        self._ledger = ledger
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()
        self._sleep = sleep

    async def charge(self, payment: Payment) -> ChargeOutcome:
        known = await self._ledger.get(payment.id)
        if known is not None:
            return known
        await self._sleep(self._rng.uniform(self._min_delay, self._max_delay))
        outcome = (
            ChargeOutcome.SUCCEEDED
            if self._rng.random() < self._success_rate
            else ChargeOutcome.DECLINED
        )
        return await self._ledger.record(payment.id, outcome)
