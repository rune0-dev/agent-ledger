from __future__ import annotations

from typing import Any


class EffectLedgerError(Exception):
    """Base exception for all effect ledger errors."""


class EffectStoreError(EffectLedgerError):
    """Raised when a store operation fails.

    Wraps backend-specific errors (database, network, etc.) so callers
    don't need to import driver-specific exception types.
    """

    def __init__(
        self,
        message: str,
        *,
        operation: str | None = None,
        idem_key: str | None = None,
        effect_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.operation = operation
        self.idem_key = idem_key
        self.effect_id = effect_id
        self.details = details or {}
        super().__init__(message)

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.operation:
            parts.append(f"operation={self.operation}")
        if self.idem_key:
            parts.append(f"idem_key={self.idem_key}")
        if self.effect_id:
            parts.append(f"effect_id={self.effect_id}")
        return " ".join(parts)


class EffectLedgerInvariantError(EffectLedgerError):
    """Raised when an internal invariant is violated.

    This indicates a bug in the library or data corruption,
    not a user error or transient failure.
    """


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
