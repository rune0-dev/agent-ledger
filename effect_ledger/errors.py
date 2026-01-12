from __future__ import annotations


class EffectLedgerError(Exception):
    pass


class EffectLedgerValidationError(EffectLedgerError):
    """Raised when input validation fails."""

    def __init__(self, message: str, field: str | None = None) -> None:
        self.field = field
        super().__init__(message)


class EffectDeniedError(EffectLedgerError):
    def __init__(self, idem_key: str, reason: str | None = None) -> None:
        self.idem_key = idem_key
        self.reason = reason
        super().__init__(reason or f"Effect {idem_key} was denied")


class EffectTimeoutError(EffectLedgerError):
    def __init__(self, idem_key: str, timeout_ms: int) -> None:
        self.idem_key = idem_key
        self.timeout_ms = timeout_ms
        super().__init__(f"Effect {idem_key} did not complete within {timeout_ms}ms")


class EffectFailedError(EffectLedgerError):
    def __init__(
        self,
        idem_key: str,
        effect_error: dict[str, str | None],
    ) -> None:
        self.idem_key = idem_key
        self.effect_error = effect_error
        self.code = effect_error.get("code")
        super().__init__(effect_error.get("message", "Effect failed"))
