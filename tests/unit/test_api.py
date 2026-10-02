from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from payments.application.use_cases import CreatePayment, GetPayment
from payments.entrypoints.api import build_api
from payments.entrypoints.api.dependencies import ApiServices
from tests.fakes import FakeUnitOfWork, FixedClock, InMemoryStore

API_KEY = "test-api-key-0123456789"
AUTH = {"X-API-Key": API_KEY}
BODY: dict[str, Any] = {
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Order 42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://merchant.example/hooks/payments",
}


def make_app(*, ready: bool = True, docs: bool = False) -> FastAPI:
    store = InMemoryStore()

    def uow() -> FakeUnitOfWork:
        return FakeUnitOfWork(store)

    async def check_ready() -> None:
        if not ready:
            raise ConnectionError("db down")

    app = build_api(api_key=API_KEY, docs_enabled=docs)
    app.state.services = ApiServices(
        create_payment=CreatePayment(uow, FixedClock()),
        get_payment=GetPayment(uow),
        check_ready=check_ready,
    )
    return app


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=make_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create(client: httpx.AsyncClient, key: str = "key-1", **overrides: Any) -> httpx.Response:
    return await client.post(
        "/api/v1/payments",
        json={**BODY, **overrides},
        headers={**AUTH, "Idempotency-Key": key},
    )


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong-key-wrong-key"}])
async def test_requires_api_key(client: httpx.AsyncClient, headers: dict[str, str]) -> None:
    response = await client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000", headers=headers
    )
    assert response.status_code == 401
    assert response.json()["code"] == "unauthorized"
    assert response.headers["WWW-Authenticate"] == "ApiKey"


async def test_requires_idempotency_key(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY, headers=AUTH)
    assert response.status_code == 422


async def test_create_returns_accepted(client: httpx.AsyncClient) -> None:
    response = await create(client)

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert response.headers["Location"] == f"/api/v1/payments/{data['payment_id']}"
    assert "Idempotent-Replayed" not in response.headers


async def test_replay_returns_same_payment(client: httpx.AsyncClient) -> None:
    first = await create(client)
    second = await create(client, amount="1250.5")

    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]
    assert second.headers["Idempotent-Replayed"] == "true"


async def test_key_reuse_with_other_body_conflicts(client: httpx.AsyncClient) -> None:
    await create(client)
    response = await create(client, amount="1.00")

    assert response.status_code == 409
    assert response.json()["code"] == "idempotency_key_reused"


async def test_get_payment_details(client: httpx.AsyncClient) -> None:
    payment_id = (await create(client)).json()["payment_id"]

    response = await client.get(f"/api/v1/payments/{payment_id}", headers=AUTH)

    assert response.status_code == 200
    data = response.json()
    assert data["amount"] == "1250.50"
    assert data["currency"] == "RUB"
    assert data["metadata"] == {"order_id": 42}
    assert data["idempotency_key"] == "key-1"
    assert data["processed_at"] is None


async def test_unknown_payment_is_404(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000", headers=AUTH
    )
    assert response.status_code == 404
    assert response.json()["code"] == "payment_not_found"


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "0"},
        {"amount": "-5"},
        {"amount": "1.001"},
        {"currency": "GBP"},
        {"webhook_url": "not-a-url"},
        {"description": ""},
        {"unexpected": "field"},
    ],
)
async def test_validation(client: httpx.AsyncClient, overrides: dict[str, Any]) -> None:
    assert (await create(client, **overrides)).status_code == 422


async def test_health_requires_api_key() -> None:
    for ready, expected in ((True, 200), (False, 503)):
        transport = httpx.ASGITransport(app=make_app(ready=ready))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            assert (await client.get("/health/live")).status_code == 401
            assert (await client.get("/health/live", headers=AUTH)).status_code == 200
            assert (await client.get("/health/ready", headers=AUTH)).status_code == expected


async def test_docs_are_disabled_by_default(client: httpx.AsyncClient) -> None:
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert (await client.get(path)).status_code == 404


async def test_docs_describe_api_key_security_scheme() -> None:
    transport = httpx.ASGITransport(app=make_app(docs=True))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        schema = (await client.get("/openapi.json")).json()
    schemes = schema["components"]["securitySchemes"]
    assert schemes["APIKeyHeader"] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
        "description": "Статический API-ключ сервиса",
    }
    create = schema["paths"]["/api/v1/payments"]["post"]
    assert create["security"] == [{"APIKeyHeader": []}]
