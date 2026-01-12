from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from opentelemetry import trace

if TYPE_CHECKING:
    from effect_ledger.types import Effect, EffectStatus

_tracer = trace.get_tracer("effect_ledger")
_logger = logging.getLogger("effect_ledger")
_logger.addHandler(logging.NullHandler())

_workflow_id: ContextVar[str | None] = ContextVar("workflow_id", default=None)
_idem_key: ContextVar[str | None] = ContextVar("idem_key", default=None)
_effect_id: ContextVar[str | None] = ContextVar("effect_id", default=None)
_tool: ContextVar[str | None] = ContextVar("tool", default=None)


def get_tracer() -> trace.Tracer:
    return _tracer


def get_logger() -> logging.Logger:
    return _logger


def set_context(
    workflow_id: str | None = None,
    idem_key: str | None = None,
    effect_id: str | None = None,
    tool: str | None = None,
) -> None:
    if workflow_id is not None:
        _workflow_id.set(workflow_id)
    if idem_key is not None:
        _idem_key.set(idem_key)
    if effect_id is not None:
        _effect_id.set(effect_id)
    if tool is not None:
        _tool.set(tool)


def clear_context() -> None:
    _workflow_id.set(None)
    _idem_key.set(None)
    _effect_id.set(None)
    _tool.set(None)


def get_context() -> dict[str, Any]:
    ctx: dict[str, Any] = {}
    if (v := _workflow_id.get()) is not None:
        ctx["workflow_id"] = v
    if (v := _idem_key.get()) is not None:
        ctx["idem_key"] = v
    if (v := _effect_id.get()) is not None:
        ctx["effect_id"] = v
    if (v := _tool.get()) is not None:
        ctx["tool"] = v
    return ctx


def log_event(
    level: int,
    event: str,
    **extra: Any,
) -> None:
    ctx = get_context()
    ctx.update(extra)
    _logger.log(level, event, extra=ctx)


def log_effect_created(effect: Effect) -> None:
    log_event(
        logging.INFO,
        "effect.created",
        effect_id=effect.id,
        status=effect.status.value,
    )


def log_effect_replayed(effect: Effect) -> None:
    log_event(
        logging.DEBUG,
        "effect.replayed",
        effect_id=effect.id,
        status=effect.status.value,
        dedup_count=effect.dedup_count,
    )


def log_status_transition(
    effect_id: str,
    from_status: EffectStatus,
    to_status: EffectStatus,
    success: bool,
) -> None:
    log_event(
        logging.INFO if success else logging.WARNING,
        "effect.transition",
        effect_id=effect_id,
        from_status=from_status.value,
        to_status=to_status.value,
        success=success,
    )


def log_handler_error(effect_id: str, error: BaseException) -> None:
    log_event(
        logging.ERROR,
        "effect.handler_error",
        effect_id=effect_id,
        error_type=type(error).__name__,
        error_message=str(error),
    )


def log_wait_timeout(idem_key: str, timeout_ms: int) -> None:
    log_event(
        logging.WARNING,
        "effect.wait_timeout",
        idem_key=idem_key,
        timeout_ms=timeout_ms,
    )
