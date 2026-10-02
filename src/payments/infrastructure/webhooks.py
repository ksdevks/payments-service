"""Доставка webhook: защита от SSRF, общий дедлайн, тело ответа не читается."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Iterable

import httpx

from payments.application.errors import WebhookRejectedError, WebhookTemporaryError
from payments.application.notifications import EVENT_TYPE, PaymentNotification

# Коды, при которых повтор имеет смысл: получатель перегружен или временно недоступен.
RETRYABLE_STATUSES = frozenset({408, 425, 429})

Resolver = Callable[[str, int], Awaitable[Iterable[str]]]


async def resolve_host(host: str, port: int) -> list[str]:
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


def is_public_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Адрес допустим как получатель webhook.

    is_global недостаточно: для multicast и устаревшего site-local IPv6
    (fec0::/10) он возвращает True. IPv6-адреса со встроенным IPv4
    (IPv4-mapped, 6to4) проверяются по встроенному адресу.
    """
    if isinstance(address, ipaddress.IPv6Address):
        embedded = address.ipv4_mapped or address.sixtofour
        if embedded is not None:
            return is_public_address(embedded)
        if address.is_site_local:
            return False
    return address.is_global and not address.is_multicast


class DestinationGuard:
    """Не дает отправить webhook во внутреннюю сеть (SSRF).

    Хост резолвится, и все его адреса обязаны быть публичными: приватные,
    loopback, link-local, multicast и зарезервированные диапазоны запрещены,
    в том числе в IPv4-mapped IPv6. Хосты из allowlist пропускаются без
    проверки - так в e2e разрешается внутренний webhook-sink.

    Остаточный риск - DNS rebinding: httpx резолвит хост повторно при
    подключении, и злонамеренный DNS может между проверкой и подключением
    вернуть другой адрес. Полная защита - подключение строго к проверенному
    адресу или egress-прокси с той же политикой.
    """

    def __init__(
        self, allowed_hosts: Iterable[str] = (), resolver: Resolver = resolve_host
    ) -> None:
        self._allowed = frozenset(host.lower() for host in allowed_hosts)
        self._resolver = resolver

    async def check(self, url: str) -> None:
        try:
            parsed = httpx.URL(url)
        except httpx.InvalidURL as exc:
            raise WebhookRejectedError(f"invalid webhook url: {exc}") from exc
        if parsed.scheme not in {"http", "https"} or not parsed.host:
            raise WebhookRejectedError(f"unsupported webhook url: {url}")

        host = parsed.host.lower()
        if host in self._allowed:
            return
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        try:
            addresses = list(await self._resolver(host, port))
        except OSError as exc:
            # Сбой DNS может быть временным: даем политике повторить.
            raise WebhookTemporaryError(f"cannot resolve {host}: {exc}") from exc
        if not addresses:
            raise WebhookTemporaryError(f"no addresses for {host}")
        for raw in addresses:
            address = ipaddress.ip_address(raw.split("%", 1)[0])
            if not is_public_address(address):
                raise WebhookRejectedError(f"{host} resolves to non-public address {address}")


class HttpWebhookSender:
    """Доставка уведомлений по HTTP.

    Таймаут httpx ограничивает каждую фазу запроса отдельно, а чтение - паузу
    между порциями данных, а не весь ответ. Поэтому поверх него стоит общий
    дедлайн asyncio.timeout. Ответ открывается потоком, и тело не читается:
    решение принимается по статусу, а медленное или огромное тело не держит
    consumer и не расходует память. Редиректы не выполняются.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        deadline: float,
        guard: DestinationGuard | None = None,
    ) -> None:
        self._client = client
        self._deadline = deadline
        self._guard = guard or DestinationGuard()

    async def send(self, url: str, notification: PaymentNotification) -> None:
        notification_id = str(notification.notification_id)
        try:
            async with asyncio.timeout(self._deadline):
                await self._guard.check(url)
                async with self._client.stream(
                    "POST",
                    url,
                    json=notification.to_payload(),
                    headers={
                        "X-Payment-Event": EVENT_TYPE,
                        "X-Notification-Id": notification_id,
                        "Idempotency-Key": notification_id,
                    },
                ) as response:
                    status = response.status_code
        except TimeoutError as exc:
            raise WebhookTemporaryError(f"webhook deadline {self._deadline}s exceeded") from exc
        except (httpx.InvalidURL, httpx.UnsupportedProtocol) as exc:
            raise WebhookRejectedError(f"invalid webhook url: {exc}") from exc
        except httpx.TransportError as exc:
            raise WebhookTemporaryError(f"{type(exc).__name__}: {exc}") from exc

        if 200 <= status < 300:
            return
        if status >= 500 or status in RETRYABLE_STATUSES:
            raise WebhookTemporaryError(f"webhook responded {status}")
        raise WebhookRejectedError(f"webhook responded {status}")
