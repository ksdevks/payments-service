class DomainError(Exception):
    """Базовая ошибка доменного слоя: нарушение инварианта сущности."""


class InvalidMoneyError(DomainError):
    pass


class InvalidPaymentError(DomainError):
    pass


class InvalidPaymentStateError(DomainError):
    pass
