import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from payments.application.errors import WebhookRejectedError, WebhookTemporaryError
from payments.application.notifications import PaymentNotification
from payments.domain import Currency, PaymentStatus
from payments.infrastructure.webhooks import DestinationGuard, HttpWebhookSender

URL = "https://hook.test/cb"
NOTIFICATION = PaymentNotification(
    notification_id=uuid.uuid4(),
    payment_id=uuid.uuid4(),
    status=PaymentStatus.SUCCEEDED,
    amount=Decimal("10.00"),
    currency=Currency.USD,
    processed_at=datetime(2026, 1, 1, tzinfo=UTC),
)


def resolver(*addresses: str) -> Callable[[str, int], Awaitable[Iterable[str]]]:
    async def resolve(host: str, port: int) -> Iterable[str]:
        return addresses

    return resolve


PUBLIC = DestinationGuard(resolver=resolver("93.184.216.34"))


def sender(transport: httpx.MockTransport, deadline: float = 5.0) -> HttpWebhookSender:
    return HttpWebhookSender(
        httpx.AsyncClient(transport=transport), deadline=deadline, guard=PUBLIC
    )


async def test_success_sends_payload_and_idempotency_headers() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    await sender(httpx.MockTransport(handle)).send(URL, NOTIFICATION)

    request = seen[0]
    assert request.headers["Idempotency-Key"] == str(NOTIFICATION.notification_id)
    assert request.headers["X-Payment-Event"] == "payment.processed"
    assert b'"amount":"10.00"' in request.content.replace(b" ", b"")


@pytest.mark.parametrize("status", [500, 502, 503, 408, 425, 429])
async def test_retryable_statuses(status: int) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(status))
    with pytest.raises(WebhookTemporaryError):
        await sender(transport).send(URL, NOTIFICATION)


@pytest.mark.parametrize("status", [301, 400, 401, 404, 410, 422])
async def test_rejected_statuses(status: int) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(status))
    with pytest.raises(WebhookRejectedError):
        await sender(transport).send(URL, NOTIFICATION)


async def test_network_errors_are_temporary() -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(WebhookTemporaryError):
        await sender(httpx.MockTransport(fail)).send(URL, NOTIFICATION)


class ExplodingBody(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        raise AssertionError("response body must not be read")
        yield b""  # pragma: no cover


async def test_response_body_is_not_read() -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=ExplodingBody()))
    await sender(transport).send(URL, NOTIFICATION)


async def test_total_deadline_covers_slow_receiver() -> None:
    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1)
        return httpx.Response(200)

    with pytest.raises(WebhookTemporaryError, match="deadline"):
        await sender(httpx.MockTransport(slow), deadline=0.05).send(URL, NOTIFICATION)


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.5",
        "172.16.3.4",
        "192.168.1.1",
        "169.254.169.254",
        "::1",
        "::ffff:127.0.0.1",
        "fd00::1",
        "100.64.0.1",
        "0.0.0.0",
        "224.0.0.1",
        "ff02::1",
        "fec0::1",
        "2002:7f00:1::1",
    ],
)
async def test_guard_blocks_internal_addresses(address: str) -> None:
    guard = DestinationGuard(resolver=resolver(address))
    with pytest.raises(WebhookRejectedError, match="non-public"):
        await guard.check(URL)


async def test_guard_blocks_if_any_address_is_internal() -> None:
    guard = DestinationGuard(resolver=resolver("93.184.216.34", "10.0.0.1"))
    with pytest.raises(WebhookRejectedError):
        await guard.check(URL)


async def test_guard_allowlist_skips_resolution() -> None:
    async def must_not_resolve(host: str, port: int) -> Iterable[str]:
        raise AssertionError("allowlisted host must not be resolved")

    await DestinationGuard(["Hook.Test"], resolver=must_not_resolve).check(URL)


async def test_guard_dns_failure_is_temporary() -> None:
    async def broken(host: str, port: int) -> Iterable[str]:
        raise OSError("temporary failure in name resolution")

    with pytest.raises(WebhookTemporaryError):
        await DestinationGuard(resolver=broken).check(URL)


@pytest.mark.parametrize("url", ["ftp://hook.test/cb", "http:///nohost"])
async def test_guard_rejects_unsupported_urls(url: str) -> None:
    with pytest.raises(WebhookRejectedError):
        await PUBLIC.check(url)


async def test_internal_destination_is_never_contacted() -> None:
    def must_not_send(request: httpx.Request) -> httpx.Response:
        raise AssertionError("request to internal address must not be sent")

    blocked = HttpWebhookSender(
        httpx.AsyncClient(transport=httpx.MockTransport(must_not_send)),
        deadline=5,
        guard=DestinationGuard(resolver=resolver("10.1.2.3")),
    )
    with pytest.raises(WebhookRejectedError):
        await blocked.send(URL, NOTIFICATION)
