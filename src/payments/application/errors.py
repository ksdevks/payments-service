class ApplicationError(Exception):
    """Базовая ошибка прикладного слоя."""


class PaymentNotFoundError(ApplicationError):
    pass


class IdempotencyKeyConflictError(ApplicationError):
    """Ключ уже использован с другим телом запроса."""


class DuplicateIdempotencyKeyError(ApplicationError):
    """Сигнал от UnitOfWork: commit нарушил уникальность idempotency key.

    Инфраструктура переводит в эту ошибку нарушение конкретного ограничения,
    чтобы прикладной слой не знал про IntegrityError и имена констрейнтов.
    """


class RetryableError(ApplicationError):
    """Временный сбой внешней зависимости: операцию имеет смысл повторить."""


class PermanentError(ApplicationError):
    """Сбой, который не исправится повтором: сообщение уходит сразу в DLQ."""


class GatewayUnavailableError(RetryableError):
    pass


class WebhookTemporaryError(RetryableError):
    """Таймаут, сетевая ошибка, 5xx, 429."""


class WebhookRejectedError(PermanentError):
    """Получатель отклонил уведомление (4xx кроме 408/429)."""
