from __future__ import annotations

import asyncio
import random
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agent_ledger.errors import (
    EffectDeniedError,
    EffectFailedError,
    EffectLedgerInvariantError,
    EffectTimeoutError,
)
from agent_ledger.observability import (
    clear_context,
    get_tracer,
    log_effect_created,
    log_effect_replayed,
    log_handler_error,
    log_status_transition,
    log_wait_timeout,
    set_context,
)
from agent_ledger.types import (
    BeginResult,
    CommitOutcome,
    CommitSucceeded,
    ConcurrencyOptions,
    Effect,
    EffectStatus,
    LedgerDefaults,
    RunOptions,
    StaleOptions,
    ToolCall,
    UpsertEffectInput,
    is_terminal_status,
)
from agent_ledger.utils import (
    compute_idem_key,
    generate_id,
    resource_id_canonical,
    validate_args,
)

if TYPE_CHECKING:
    from agent_ledger.stores.base import EffectStore

TxT = TypeVar("TxT")

DEFAULT_CONCURRENCY = ConcurrencyOptions(
    wait_timeout_ms=30_000,
    initial_interval_ms=50,
    max_interval_ms=1_000,
    backoff_multiplier=1.5,
    jitter_factor=0.3,
)

DEFAULT_STALE = StaleOptions(after_ms=0)

MIN_STALE_THRESHOLD_MS = 1000


@dataclass(frozen=True, slots=True)
class MergedOptions:
    concurrency: ConcurrencyOptions
    stale: StaleOptions


def _coalesce(*values: Any) -> Any:
    """Return first non-None value, or None if all are None."""
    for v in values:
        if v is not None:
            return v
    return None


def _get_concurrency_field(
    per_call: RunOptions | None,
    instance_defaults: RunOptions | None,
    field: str,
    default: Any,
) -> Any:
    """Get a concurrency option field with proper None handling."""
    per_call_val = (
        getattr(per_call.concurrency, field, None)
        if per_call and per_call.concurrency
        else None
    )
    defaults_val = (
        getattr(instance_defaults.concurrency, field, None)
        if instance_defaults and instance_defaults.concurrency
        else None
    )
    return _coalesce(per_call_val, defaults_val, default)


def _merge_options(
    instance_defaults: RunOptions | None,
    per_call: RunOptions | None,
) -> MergedOptions:
    concurrency = ConcurrencyOptions(
        wait_timeout_ms=_get_concurrency_field(
            per_call,
            instance_defaults,
            "wait_timeout_ms",
            DEFAULT_CONCURRENCY.wait_timeout_ms,
        ),
        initial_interval_ms=_get_concurrency_field(
            per_call,
            instance_defaults,
            "initial_interval_ms",
            DEFAULT_CONCURRENCY.initial_interval_ms,
        ),
        max_interval_ms=_get_concurrency_field(
            per_call,
            instance_defaults,
            "max_interval_ms",
            DEFAULT_CONCURRENCY.max_interval_ms,
        ),
        backoff_multiplier=_get_concurrency_field(
            per_call,
            instance_defaults,
            "backoff_multiplier",
            DEFAULT_CONCURRENCY.backoff_multiplier,
        ),
        jitter_factor=_get_concurrency_field(
            per_call,
            instance_defaults,
            "jitter_factor",
            DEFAULT_CONCURRENCY.jitter_factor,
        ),
    )

    per_call_stale = per_call.stale.after_ms if per_call and per_call.stale else None
    defaults_stale = (
        instance_defaults.stale.after_ms
        if instance_defaults and instance_defaults.stale
        else None
    )
    stale = StaleOptions(
        after_ms=_coalesce(per_call_stale, defaults_stale, DEFAULT_STALE.after_ms),
    )

    return MergedOptions(concurrency=concurrency, stale=stale)


def _compute_next_interval(current_interval: int, opts: ConcurrencyOptions) -> int:
    base = min(current_interval * opts.backoff_multiplier, opts.max_interval_ms)
    jitter = base * opts.jitter_factor * (random.random() * 2 - 1)
    return max(1, int(base + jitter))


def _is_effect_stale(effect: Effect, stale_after_ms: int) -> bool:
    if stale_after_ms <= 0:
        return False
    effective_threshold = max(stale_after_ms, MIN_STALE_THRESHOLD_MS)
    age_ms = (datetime.now(tz=timezone.utc) - effect.updated_at).total_seconds() * 1000
    return age_ms > effective_threshold


@dataclass
class EffectLedgerOptions(Generic[TxT]):
    store: EffectStore[TxT]
    defaults: LedgerDefaults | None = None
    max_args_size_bytes: int | None = None


class EffectLedger(Generic[TxT]):
    def __init__(self, options: EffectLedgerOptions[TxT]) -> None:
        self._store = options.store
        self._defaults = options.defaults
        self._max_args_size_bytes = options.max_args_size_bytes

    async def begin(
        self,
        call: ToolCall,
        tx: TxT | None = None,
    ) -> BeginResult:
        tracer = get_tracer()

        args_canonical = validate_args(call.args, self._max_args_size_bytes)

        with tracer.start_as_current_span(
            "agent_ledger.begin",
            attributes={"tool": call.tool, "workflow_id": call.workflow_id},
        ) as span:
            idem_key = compute_idem_key(call)
            call_id = call.call_id or generate_id()
            resource_canonical = (
                resource_id_canonical(call.resource) if call.resource else call.tool
            )

            span.set_attribute("idem_key", idem_key)

            result = await self._store.upsert(
                UpsertEffectInput(
                    idem_key=idem_key,
                    workflow_id=call.workflow_id,
                    call_id=call_id,
                    tool=call.tool,
                    status=EffectStatus.PROCESSING,
                    args_canonical=args_canonical,
                    resource_id_canonical=resource_canonical,
                ),
                tx,
            )

            if not result.created:
                await self._store.increment_dedup_count(idem_key, tx)
                cached = is_terminal_status(result.effect.status)

                span.set_attribute("idempotency_status", "replayed")
                span.set_attribute("effect_id", result.effect.id)
                log_effect_replayed(result.effect)

                return BeginResult(
                    effect=result.effect,
                    cached=cached,
                    idempotency_status="replayed",
                    cached_result=result.effect.result,
                )

            span.set_attribute("idempotency_status", "fresh")
            span.set_attribute("effect_id", result.effect.id)
            log_effect_created(result.effect)

            return BeginResult(
                effect=result.effect,
                cached=False,
                idempotency_status="fresh",
            )

    async def commit(
        self,
        effect_id: str,
        outcome: CommitOutcome,
        tx: TxT | None = None,
    ) -> bool:
        tracer = get_tracer()
        to_status = (
            EffectStatus.SUCCEEDED
            if isinstance(outcome, CommitSucceeded)
            else EffectStatus.FAILED
        )

        with tracer.start_as_current_span(
            "agent_ledger.commit",
            attributes={"effect_id": effect_id, "to_status": to_status.value},
        ):
            if isinstance(outcome, CommitSucceeded):
                success = await self._store.transition(
                    effect_id,
                    EffectStatus.PROCESSING,
                    EffectStatus.SUCCEEDED,
                    result=outcome.result,
                    tx=tx,
                )
                log_status_transition(
                    effect_id, EffectStatus.PROCESSING, EffectStatus.SUCCEEDED, success
                )
                return success

            success = await self._store.transition(
                effect_id,
                EffectStatus.PROCESSING,
                EffectStatus.FAILED,
                error={"code": outcome.error.code, "message": outcome.error.message},
                tx=tx,
            )

            log_status_transition(
                effect_id, EffectStatus.PROCESSING, EffectStatus.FAILED, success
            )

            return success

    async def _wait_for_terminal(
        self,
        idem_key: str,
        opts: MergedOptions,
        tx: TxT | None = None,
    ) -> Effect:
        tracer = get_tracer()
        with tracer.start_as_current_span(
            "agent_ledger.wait_for_terminal",
            attributes={
                "idem_key": idem_key,
                "timeout_ms": opts.concurrency.wait_timeout_ms,
            },
        ) as span:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + (opts.concurrency.wait_timeout_ms / 1000)

            interval = opts.concurrency.initial_interval_ms
            poll_count = 0

            while True:
                poll_count += 1
                effect = await self._store.find_by_idem_key(idem_key, tx)

                if effect is None:
                    raise EffectLedgerInvariantError(
                        f"Effect {idem_key} disappeared while waiting"
                    )

                if effect.status in (EffectStatus.DENIED, EffectStatus.CANCELED):
                    raise EffectDeniedError(
                        effect.idem_key,
                        effect.error.message if effect.error else None,
                    )

                is_stale_processing = (
                    effect.status == EffectStatus.PROCESSING
                    and _is_effect_stale(effect, opts.stale.after_ms)
                )
                if (
                    is_terminal_status(effect.status)
                    or effect.status == EffectStatus.READY
                    or is_stale_processing
                ):
                    span.set_attribute("poll_count", poll_count)
                    span.set_attribute("resolved_status", effect.status.value)

                    return effect

                if loop.time() >= deadline:
                    span.set_attribute("poll_count", poll_count)
                    span.set_attribute("timed_out", True)
                    log_wait_timeout(idem_key, opts.concurrency.wait_timeout_ms)

                    raise EffectTimeoutError(idem_key, opts.concurrency.wait_timeout_ms)

                await asyncio.sleep(interval / 1000)
                interval = _compute_next_interval(interval, opts.concurrency)

    async def run(
        self,
        call: ToolCall,
        handler: Callable[[Effect], Coroutine[Any, Any, Any]],
        tx: TxT | None = None,
        run_options: RunOptions | None = None,
    ) -> Any:
        tracer = get_tracer()

        validate_args(call.args, self._max_args_size_bytes)
        idem_key = compute_idem_key(call)

        set_context(
            workflow_id=call.workflow_id,
            idem_key=idem_key,
            tool=call.tool,
        )

        try:
            with tracer.start_as_current_span(
                "agent_ledger.run",
                attributes={
                    "tool": call.tool,
                    "workflow_id": call.workflow_id,
                    "idem_key": idem_key,
                },
            ) as span:
                merged = _merge_options(
                    self._defaults.run if self._defaults else None,
                    run_options,
                )

                per_call_approval = (
                    run_options.requires_approval if run_options else None
                )
                defaults_approval = (
                    self._defaults.run.requires_approval
                    if self._defaults and self._defaults.run
                    else None
                )
                requires_approval = _coalesce(
                    per_call_approval, defaults_approval, False
                )

                begin_result = await self.begin(call, tx)
                effect = begin_result.effect

                span.set_attribute("effect_id", effect.id)
                set_context(effect_id=effect.id)

                if begin_result.idempotency_status == "fresh":
                    if requires_approval:
                        await self.request_approval(effect.idem_key, tx)

                        resolved = await self._wait_for_terminal(
                            effect.idem_key, merged, tx
                        )

                        return await self._handle_resolved_effect(
                            resolved, handler, merged, tx
                        )

                    return await self._execute_handler(effect, handler, tx)

                if begin_result.cached:
                    if effect.status == EffectStatus.FAILED and effect.error:
                        raise EffectFailedError(
                            effect.idem_key,
                            {
                                "code": effect.error.code,
                                "message": effect.error.message,
                            },
                        )

                    if effect.status == EffectStatus.SUCCEEDED:
                        return effect.result

                if effect.status == EffectStatus.PROCESSING:
                    if _is_effect_stale(effect, merged.stale.after_ms):
                        return await self._claim_and_execute(
                            effect,
                            EffectStatus.PROCESSING,
                            handler,
                            merged,
                            tx,
                            stale_threshold_ms=merged.stale.after_ms,
                        )

                    resolved = await self._wait_for_terminal(
                        effect.idem_key, merged, tx
                    )

                    return await self._handle_resolved_effect(
                        resolved, handler, merged, tx
                    )

                if effect.status == EffectStatus.REQUIRES_APPROVAL:
                    resolved = await self._wait_for_terminal(
                        effect.idem_key, merged, tx
                    )

                    return await self._handle_resolved_effect(
                        resolved, handler, merged, tx
                    )

                if effect.status == EffectStatus.READY:
                    return await self._claim_and_execute(
                        effect, EffectStatus.READY, handler, merged, tx
                    )

                if effect.status in (EffectStatus.DENIED, EffectStatus.CANCELED):
                    raise EffectDeniedError(
                        effect.idem_key,
                        effect.error.message if effect.error else None,
                    )

                if is_terminal_status(effect.status):
                    if effect.status == EffectStatus.FAILED and effect.error:
                        raise EffectFailedError(
                            effect.idem_key,
                            {
                                "code": effect.error.code,
                                "message": effect.error.message,
                            },
                        )

                    if effect.status == EffectStatus.SUCCEEDED:
                        return effect.result

                    raise EffectLedgerInvariantError(
                        f"Effect {effect.id} is terminal but not succeeded/failed"
                    )

                raise EffectLedgerInvariantError(
                    f"Effect {effect.idem_key} in unexpected state: {effect.status}"
                )
        finally:
            clear_context()

    async def _claim_and_execute(
        self,
        effect: Effect,
        from_status: EffectStatus,
        handler: Callable[[Effect], Coroutine[Any, Any, Any]],
        merged: MergedOptions,
        tx: TxT | None = None,
        stale_threshold_ms: int | None = None,
    ) -> Any:
        """Atomically claim an effect and execute the handler.

        If claim fails, waits for the winner to complete and returns their result.
        This ensures exactly-once execution semantics.
        """
        claimed = await self._store.claim_for_processing(
            effect.id,
            from_status,
            stale_threshold_ms=stale_threshold_ms,
            tx=tx,
        )

        if not claimed:
            # Someone else got it, wait for their result
            resolved = await self._wait_for_terminal(effect.idem_key, merged, tx)

            # If terminal, return result directly
            if is_terminal_status(resolved.status):
                return self._return_terminal_result(resolved)

            # Otherwise (READY or stale PROCESSING), handle recursively
            return await self._handle_resolved_effect(resolved, handler, merged, tx)

        return await self._execute_handler(effect, handler, tx)

    def _return_terminal_result(self, effect: Effect) -> Any:
        """Extract result from a terminal effect, raising appropriate errors."""
        if effect.status == EffectStatus.SUCCEEDED:
            return effect.result

        if effect.status == EffectStatus.FAILED and effect.error:
            raise EffectFailedError(
                effect.idem_key,
                {"code": effect.error.code, "message": effect.error.message},
            )

        if effect.status in (EffectStatus.DENIED, EffectStatus.CANCELED):
            raise EffectDeniedError(
                effect.idem_key,
                effect.error.message if effect.error else None,
            )

        raise EffectLedgerInvariantError(
            f"Effect {effect.idem_key} in unexpected terminal state: {effect.status}"
        )

    async def _handle_resolved_effect(
        self,
        effect: Effect,
        handler: Callable[[Effect], Coroutine[Any, Any, Any]],
        merged: MergedOptions,
        tx: TxT | None = None,
    ) -> Any:
        """Handle an effect that has resolved from waiting (terminal, READY, or stale)."""
        if effect.status == EffectStatus.FAILED and effect.error:
            raise EffectFailedError(
                effect.idem_key,
                {"code": effect.error.code, "message": effect.error.message},
            )

        if effect.status in (EffectStatus.DENIED, EffectStatus.CANCELED):
            raise EffectDeniedError(
                effect.idem_key,
                effect.error.message if effect.error else None,
            )

        if effect.status == EffectStatus.SUCCEEDED:
            return effect.result

        if effect.status == EffectStatus.READY:
            return await self._claim_and_execute(
                effect, EffectStatus.READY, handler, merged, tx
            )

        if effect.status == EffectStatus.REQUIRES_APPROVAL:
            resolved = await self._wait_for_terminal(effect.idem_key, merged, tx)
            return await self._handle_resolved_effect(resolved, handler, merged, tx)

        if effect.status == EffectStatus.PROCESSING and _is_effect_stale(
            effect, merged.stale.after_ms
        ):
            return await self._claim_and_execute(
                effect,
                EffectStatus.PROCESSING,
                handler,
                merged,
                tx,
                stale_threshold_ms=merged.stale.after_ms,
            )

        if effect.status == EffectStatus.PROCESSING:
            resolved = await self._wait_for_terminal(effect.idem_key, merged, tx)
            return await self._handle_resolved_effect(resolved, handler, merged, tx)

        raise EffectLedgerInvariantError(
            f"Effect {effect.idem_key} in unexpected state: {effect.status}"
        )

    async def _execute_handler(
        self,
        effect: Effect,
        handler: Callable[[Effect], Coroutine[Any, Any, Any]],
        tx: TxT | None = None,
    ) -> Any:
        try:
            result = await handler(effect)

            current = await self._store.find_by_idem_key(effect.idem_key, tx)
            from_status = current.status if current else effect.status

            committed = await self._store.transition(
                effect.id,
                from_status,
                EffectStatus.SUCCEEDED,
                result=result,
                tx=tx,
            )

            if not committed:
                # Lost the race - someone else modified the effect (stale takeover, deny, etc.)
                # Re-fetch and return the actual terminal result
                final = await self._store.find_by_idem_key(effect.idem_key, tx)
                if final is None:
                    raise EffectLedgerInvariantError(
                        f"Effect {effect.idem_key} disappeared during commit"
                    )

                if is_terminal_status(final.status):
                    return self._return_terminal_result(final)

                raise EffectLedgerInvariantError(
                    f"Effect {effect.idem_key} commit failed, status is {final.status}"
                )

            return result
        except Exception as err:
            log_handler_error(effect.id, err)

            # Don't wrap our own errors
            if isinstance(err, EffectFailedError | EffectDeniedError):
                raise

            error = {
                "code": getattr(err, "code", None),
                "message": str(err),
            }

            current = await self._store.find_by_idem_key(effect.idem_key, tx)
            from_status = current.status if current else effect.status

            committed = await self._store.transition(
                effect.id,
                from_status,
                EffectStatus.FAILED,
                error=error,
                tx=tx,
            )

            if not committed:
                # Lost the race - someone else modified the effect
                # Re-fetch to see actual state, but still raise the original error
                final = await self._store.find_by_idem_key(effect.idem_key, tx)
                if final and is_terminal_status(final.status):
                    # Another worker completed it - return their result instead of raising
                    return self._return_terminal_result(final)
                # Otherwise, just raise the original error

            raise

    async def get_effect(
        self,
        effect_id: str,
        tx: TxT | None = None,
    ) -> Effect | None:
        return await self._store.find_by_id(effect_id, tx)

    async def find_by_idem_key(
        self,
        idem_key: str,
        tx: TxT | None = None,
    ) -> Effect | None:
        return await self._store.find_by_idem_key(idem_key, tx)

    async def request_approval(
        self,
        idem_key: str,
        tx: TxT | None = None,
    ) -> bool:
        effect = await self._store.find_by_idem_key(idem_key, tx)
        if effect is None:
            return False

        return await self._store.transition(
            effect.id,
            EffectStatus.PROCESSING,
            EffectStatus.REQUIRES_APPROVAL,
            tx=tx,
        )

    async def approve(
        self,
        idem_key: str,
        tx: TxT | None = None,
    ) -> bool:
        effect = await self._store.find_by_idem_key(idem_key, tx)
        if effect is None:
            return False

        return await self._store.transition(
            effect.id,
            EffectStatus.REQUIRES_APPROVAL,
            EffectStatus.READY,
            tx=tx,
        )

    async def deny(
        self,
        idem_key: str,
        reason: str | None = None,
        tx: TxT | None = None,
    ) -> bool:
        effect = await self._store.find_by_idem_key(idem_key, tx)
        if effect is None:
            return False

        return await self._store.transition(
            effect.id,
            EffectStatus.REQUIRES_APPROVAL,
            EffectStatus.DENIED,
            error={"message": reason} if reason else None,
            tx=tx,
        )
