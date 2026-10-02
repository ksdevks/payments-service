from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from payments.application.errors import IdempotencyKeyConflictError, PaymentNotFoundError
from payments.domain import DomainError


class ApiError(Exception):
    def __init__(self, status: int, code: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.code = code
        self.detail = detail


# Прикладные ошибки -> HTTP. Слой приложения про HTTP ничего не знает.
_ERROR_MAP: dict[type[Exception], tuple[int, str]] = {
    IdempotencyKeyConflictError: (409, "idempotency_key_reused"),
    PaymentNotFoundError: (404, "payment_not_found"),
    DomainError: (422, "invalid_payment"),
}


def _body(code: str, detail: str) -> dict[str, str]:
    return {"code": code, "detail": detail}


async def _handle_api_error(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ApiError)
    headers = {"WWW-Authenticate": "ApiKey"} if exc.status == 401 else None
    return JSONResponse(_body(exc.code, exc.detail), status_code=exc.status, headers=headers)


async def _handle_mapped(_: Request, exc: Exception) -> JSONResponse:
    for error_type, (status, code) in _ERROR_MAP.items():
        if isinstance(exc, error_type):
            return JSONResponse(_body(code, str(exc)), status_code=status)
    raise exc


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, _handle_api_error)
    for error_type in _ERROR_MAP:
        app.add_exception_handler(error_type, _handle_mapped)
