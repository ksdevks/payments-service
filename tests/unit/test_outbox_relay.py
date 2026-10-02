import asyncio
import time
import uuid
from collections.abc import Sequence

import pytest

from payments.infrastructure.outbox import ClaimedMessage, OutboxRelay, RelaySettings


def message(attempts: int = 1) -> ClaimedMessage:
    return ClaimedMessage(
        id=uuid.uuid4(), topic="payments.new", payload={"k": "v"}, attempts=attempts
    )


class FakeStore:
    def __init__(self, batches: list[list[ClaimedMessage]]) -> None:
        self.batches = batches
        self.published: list[uuid.UUID] = []
        self.failed: list[tuple[uuid.UUID, float]] = []
        self.released: list[uuid.UUID] = []

    async def claim_batch(self, limit: int) -> Sequence[ClaimedMessage]:
        return self.batches.pop(0)[:limit] if self.batches else []

    async def mark_published(self, ids: Sequence[uuid.UUID]) -> None:
        self.published.extend(ids)

    async def mark_failed(self, message: ClaimedMessage, error: str, retry_in: float) -> None:
        self.failed.append((message.id, retry_in))

    async def release(self, messages: Sequence[ClaimedMessage]) -> None:
        self.released.extend(m.id for m in messages)


class FakePublisher:
    def __init__(self, failing: set[uuid.UUID] | None = None) -> None:
        self.failing = failing or set()
        self.sent: list[uuid.UUID] = []

    async def publish(self, message: ClaimedMessage) -> None:
        if message.id in self.failing:
            raise ConnectionError("broker unavailable")
        self.sent.append(message.id)


SETTINGS = RelaySettings(batch_size=10, poll_interval=0.01, retry_base=1.0, retry_max=8.0)


def test_backoff_is_exponential_and_capped() -> None:
    assert [SETTINGS.backoff(n) for n in range(1, 7)] == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]


def test_backoff_survives_huge_attempt_counts() -> None:
    # Без ограничения степени 2.0 ** 1024 дает OverflowError.
    assert SETTINGS.backoff(1025) == 8.0
    assert SETTINGS.backoff(10**9) == 8.0


async def test_publishes_batch_and_marks_published() -> None:
    batch = [message(), message()]
    store, publisher = FakeStore([batch]), FakePublisher()

    claimed = await OutboxRelay(store, publisher, SETTINGS).run_once()

    assert claimed == 2
    assert publisher.sent == [m.id for m in batch]
    assert store.published == [m.id for m in batch]
    assert store.failed == []


async def test_each_message_is_marked_right_after_publish() -> None:
    first, second = message(), message()
    store = FakeStore([[first, second]])

    class CheckingPublisher(FakePublisher):
        async def publish(self, message: ClaimedMessage) -> None:
            if message.id == second.id:
                # К моменту публикации второго первый уже отмечен.
                assert store.published == [first.id]
            await super().publish(message)

    await OutboxRelay(store, CheckingPublisher(), SETTINGS).run_once()
    assert store.published == [first.id, second.id]


async def test_batch_stops_before_lease_runs_out_and_releases_rest() -> None:
    """Сценарий из ревью: медленные шаги съедают аренду посреди пачки."""
    batch = [message() for _ in range(5)]
    store = FakeStore([batch])
    now = [0.0]

    class SlowPublisher(FakePublisher):
        async def publish(self, message: ClaimedMessage) -> None:
            now[0] += 3.0  # каждый шаг занимает 3 секунды
            await super().publish(message)

    settings = RelaySettings(
        batch_size=10, lease_seconds=10.0, publish_timeout=2.0, store_timeout=2.0
    )
    await OutboxRelay(store, SlowPublisher(), settings, clock=lambda: now[0]).run_once()

    # Шаги стартуют в 0 и 3 с; старт в 6 с уже позже 10 - 4, остаток возвращен.
    assert store.published == [m.id for m in batch[:2]]
    assert store.released == [m.id for m in batch[2:]]


def test_lease_must_cover_two_steps() -> None:
    with pytest.raises(ValueError):
        RelaySettings(lease_seconds=10.0, publish_timeout=3.0, store_timeout=3.0)


async def test_hanging_store_cannot_stretch_a_step() -> None:
    """Сценарий из ревью: медленное хранилище (ожидание соединения, коммит)."""

    class HangingStore(FakeStore):
        async def mark_published(self, ids: Sequence[uuid.UUID]) -> None:
            await asyncio.sleep(10)

    settings = RelaySettings(
        batch_size=10, lease_seconds=1.0, publish_timeout=0.05, store_timeout=0.05
    )
    relay = OutboxRelay(HangingStore([[message()]]), FakePublisher(), settings)

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await relay.run_once()
    assert time.monotonic() - started < 1


async def test_hanging_publish_is_bounded_and_rescheduled() -> None:
    class HangingPublisher(FakePublisher):
        async def publish(self, message: ClaimedMessage) -> None:
            await asyncio.sleep(10)

    stuck = message()
    store = FakeStore([[stuck]])
    settings = RelaySettings(
        batch_size=10, lease_seconds=1.0, publish_timeout=0.05, store_timeout=0.05
    )
    await asyncio.wait_for(
        OutboxRelay(store, HangingPublisher(), settings).run_once(), timeout=1
    )

    assert store.published == []
    assert [message_id for message_id, _ in store.failed] == [stuck.id]


async def test_failed_publish_is_rescheduled_without_blocking_others() -> None:
    ok, broken = message(), message(attempts=3)
    store = FakeStore([[broken, ok]])
    publisher = FakePublisher(failing={broken.id})

    await OutboxRelay(store, publisher, SETTINGS).run_once()

    assert store.published == [ok.id]
    assert store.failed == [(broken.id, 4.0)]


async def test_run_stops_on_event() -> None:
    store, publisher = FakeStore([[message()]]), FakePublisher()
    relay = OutboxRelay(store, publisher, SETTINGS)
    stop = asyncio.Event()

    task = asyncio.create_task(relay.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert len(store.published) == 1


async def test_run_survives_store_outage() -> None:
    class FlakyStore(FakeStore):
        calls = 0

        async def claim_batch(self, limit: int) -> Sequence[ClaimedMessage]:
            FlakyStore.calls += 1
            if FlakyStore.calls == 1:
                raise ConnectionError("db down")
            return await super().claim_batch(limit)

    store = FlakyStore([[message()]])
    stop = asyncio.Event()
    task = asyncio.create_task(OutboxRelay(store, FakePublisher(), SETTINGS).run(stop))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert len(store.published) == 1
