from __future__ import annotations

import asyncio
import random
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from effect_ledger.errors import (
    EffectDeniedError,
    EffectFailedError,
    EffectTimeoutError,
)
from effect_ledger.types import (
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
from effect_ledger.utils import (
    canonicalize,
    compute_idem_key,
    generate_id,
    resource_id_canonical,
)

if TYPE_CHECKING:
    from effect_ledger.stores.base import EffectStore

TxT = TypeVar("TxT")

DEFAULT_CONCURRENCY = ConcurrencyOptions(
    wait_timeout_ms=30_000,
    initial_interval_ms=50,
    max_interval_ms=1_000,
    backoff_multiplier=1.5,
    jitter_factor=0.3,
)

DEFAULT_STALE = StaleOptions(after_ms=0)


@dataclass(frozen=True, slots=True)
class MergedOptions:
    concurrency: ConcurrencyOptions
    stale: StaleOptions


def _merge_options(
    instance_defaults: RunOptions | None,
    per_call: RunOptions | None,
) -> MergedOptions:
    concurrency = ConcurrencyOptions(
        wait_timeout_ms=(
            per_call and per_call.concurrency and per_call.concurrency.wait_timeout_ms
        )
        or (
            instance_defaults
            and instance_defaults.concurrency
            and instance_defaults.concurrency.wait_timeout_ms
        )
        or DEFAULT_CONCURRENCY.wait_timeout_ms,
        initial_interval_ms=(
            per_call
            and per_call.concurrency
            and per_call.concurrency.initial_interval_ms
        )
        or (
            instance_defaults
            and instance_defaults.concurrency
            and instance_defaults.concurrency.initial_interval_ms
        )
        or DEFAULT_CONCURRENCY.initial_interval_ms,
        max_interval_ms=(
            per_call and per_call.concurrency and per_call.concurrency.max_interval_ms
        )
        or (
            instance_defaults
            and instance_defaults.concurrency
            and instance_defaults.concurrency.max_interval_ms
        )
        or DEFAULT_CONCURRENCY.max_interval_ms,
        backoff_multiplier=(
            per_call
            and per_call.concurrency
            and per_call.concurrency.backoff_multiplier
        )
        or (
            instance_defaults
            and instance_defaults.concurrency
            and instance_defaults.concurrency.backoff_multiplier
        )
        or DEFAULT_CONCURRENCY.backoff_multiplier,
        jitter_factor=(
            per_call and per_call.concurrency and per_call.concurrency.jitter_factor
        )
        or (
            instance_defaults
            and instance_defaults.concurrency
            and instance_defaults.concurrency.jitter_factor
        )
        or DEFAULT_CONCURRENCY.jitter_factor,
    )
    stale = StaleOptions(
        after_ms=(per_call and per_call.stale and per_call.stale.after_ms)
        or (
            instance_defaults
            and instance_defaults.stale
            and instance_defaults.stale.after_ms
        )
        or DEFAULT_STALE.after_ms,
    )
    return MergedOptions(concurrency=concurrency, stale=stale)


def _compute_next_interval(current_interval: int, opts: ConcurrencyOptions) -> int:
    base = min(current_interval * opts.backoff_multiplier, opts.max_interval_ms)
    jitter = base * opts.jitter_factor * (random.random() * 2 - 1)
    return max(1, int(base + jitter))


def _is_effect_stale(effect: Effect, stale_after_ms: int) -> bool:
    if stale_after_ms <= 0:
        return False
    age_ms = (datetime.now(tz=timezone.utc) - effect.updated_at).total_seconds() * 1000
    return age_ms > stale_after_ms


@dataclass
class EffectLedgerOptions(Generic[TxT]):
    store: EffectStore[TxT]
    defaults: LedgerDefaults | None = None


class EffectLedger(Generic[TxT]):
    def __init__(self, options: EffectLedgerOptions[TxT]) -> None:
        self._store = options.store
        self._defaults = options.defaults

    async def begin(
        self,
        call: ToolCall,
        tx: TxT | None = None,
    ) -> BeginResult:
        idem_key = compute_idem_key(call)
        call_id = call.call_id or generate_id()
        resource_canonical = (
            resource_id_canonical(call.resource) if call.resource else call.tool
        )

        result = await self._store.upsert(
            UpsertEffectInput(
                idem_key=idem_key,
                workflow_id=call.workflow_id,
                call_id=call_id,
                tool=call.tool,
                status=EffectStatus.PROCESSING,
                args_canonical=canonicalize(call.args),
                resource_id_canonical=resource_canonical,
            ),
            tx,
        )

        if not result.created:
            await self._store.increment_dedup_count(idem_key, tx)
            cached = is_terminal_status(result.effect.status)

            return BeginResult(
                effect=result.effect,
                cached=cached,
                idempotency_status="replayed",
                cached_result=result.effect.result,
            )

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
    ) -> None:
        if isinstance(outcome, CommitSucceeded):
            await self._store.transition(
                effect_id,
                EffectStatus.PROCESSING,
                EffectStatus.SUCCEEDED,
                result=outcome.result,
                tx=tx,
            )

            return

        await self._store.transition(
            effect_id,
            EffectStatus.PROCESSING,
            EffectStatus.FAILED,
            error={"code": outcome.error.code, "message": outcome.error.message},
            tx=tx,
        )

    async def _wait_for_terminal(
        self,
        idem_key: str,
        opts: MergedOptions,
        tx: TxT | None = None,
    ) -> Effect:
        deadline = asyncio.get_event_loop().time() + (
            opts.concurrency.wait_timeout_ms / 1000
        )

        interval = opts.concurrency.initial_interval_ms

        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(interval / 1000)

            effect = await self._store.find_by_idem_key(idem_key, tx)
            if effect is None:
                raise RuntimeError(f"Effect {idem_key} disappeared while waiting")

            if effect.status in (EffectStatus.DENIED, EffectStatus.CANCELED):
                raise EffectDeniedError(
                    effect.idem_key,
                    effect.error.message if effect.error else None,
                )

            if (
                is_terminal_status(effect.status)
                or effect.status == EffectStatus.READY
                or _is_effect_stale(effect, opts.stale.after_ms)
            ):
                return effect

            interval = _compute_next_interval(interval, opts.concurrency)

        raise EffectTimeoutError(idem_key, opts.concurrency.wait_timeout_ms)

    async def run(
        self,
        call: ToolCall,
        handler: Callable[[Effect], Coroutine[Any, Any, Any]],
        tx: TxT | None = None,
        run_options: RunOptions | None = None,
    ) -> Any:
        merged = _merge_options(
            self._defaults.run if self._defaults else None,
            run_options,
        )

        requires_approval = run_options.requires_approval if run_options else False

        begin_result = await self.begin(call, tx)
        effect = begin_result.effect

        if begin_result.idempotency_status == "fresh":
            if requires_approval:
                await self.request_approval(effect.idem_key, tx)

                resolved = await self._wait_for_terminal(effect.idem_key, merged, tx)
                return await self._execute_handler(resolved, handler, tx)

            return await self._execute_handler(effect, handler, tx)

        if begin_result.cached:
            if effect.status == EffectStatus.FAILED and effect.error:
                raise EffectFailedError(
                    effect.idem_key,
                    {"code": effect.error.code, "message": effect.error.message},
                )

            if begin_result.cached_result is not None:
                return begin_result.cached_result

            if effect.result is not None:
                return effect.result

        if effect.status == EffectStatus.PROCESSING:
            if _is_effect_stale(effect, merged.stale.after_ms):
                claimed = await self._store.claim_for_processing(
                    effect.id,
                    EffectStatus.PROCESSING,
                    stale_threshold_ms=merged.stale.after_ms,
                    tx=tx,
                )

                if not claimed:
                    # Someone else claimed it, wait for their result
                    resolved = await self._wait_for_terminal(
                        effect.idem_key, merged, tx
                    )

                    return await self._handle_resolved_effect(
                        resolved, handler, merged, tx
                    )

                return await self._execute_handler(effect, handler, tx)

            resolved = await self._wait_for_terminal(effect.idem_key, merged, tx)
            return await self._handle_resolved_effect(resolved, handler, merged, tx)

        if effect.status == EffectStatus.REQUIRES_APPROVAL:
            resolved = await self._wait_for_terminal(effect.idem_key, merged, tx)
            return await self._handle_resolved_effect(resolved, handler, merged, tx)

        if effect.status == EffectStatus.READY:
            claimed = await self._store.claim_for_processing(
                effect.id,
                EffectStatus.READY,
                tx=tx,
            )

            if not claimed:
                # Someone else claimed it, wait for their result
                resolved = await self._wait_for_terminal(effect.idem_key, merged, tx)
                return await self._handle_resolved_effect(resolved, handler, merged, tx)

            return await self._execute_handler(effect, handler, tx)

        if effect.status in (EffectStatus.DENIED, EffectStatus.CANCELED):
            raise EffectDeniedError(
                effect.idem_key,
                effect.error.message if effect.error else None,
            )

        if is_terminal_status(effect.status):
            if effect.status == EffectStatus.FAILED and effect.error:
                raise EffectFailedError(
                    effect.idem_key,
                    {"code": effect.error.code, "message": effect.error.message},
                )

            if effect.result is not None:
                return effect.result

            raise RuntimeError(f"Effect {effect.id} is terminal but has no result")

        raise RuntimeError(
            f"Effect {effect.idem_key} in unexpected state: {effect.status}"
        )

    async def _handle_resolved_effect(
        self,
        effect: Effect,
        handler: Callable[[Effect], Coroutine[Any, Any, Any]],
        merged: MergedOptions,
        tx: TxT | None = None,
    ) -> Any:
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

        if effect.status == EffectStatus.SUCCEEDED and effect.result is not None:
            return effect.result

        if effect.status == EffectStatus.READY:
            claimed = await self._store.claim_for_processing(
                effect.id,
                EffectStatus.READY,
                tx=tx,
            )
            if not claimed:
                # Someone else got it, re-fetch and handle
                updated = await self._store.find_by_idem_key(effect.idem_key, tx)
                if updated and is_terminal_status(updated.status):
                    if updated.status == EffectStatus.SUCCEEDED:
                        return updated.result
                    if updated.status == EffectStatus.FAILED and updated.error:
                        raise EffectFailedError(
                            updated.idem_key,
                            {
                                "code": updated.error.code,
                                "message": updated.error.message,
                            },
                        )
                raise RuntimeError(
                    f"Effect {effect.idem_key} claim failed, unexpected state"
                )
            return await self._execute_handler(effect, handler, tx)

        if _is_effect_stale(effect, merged.stale.after_ms):
            claimed = await self._store.claim_for_processing(
                effect.id,
                EffectStatus.PROCESSING,
                stale_threshold_ms=merged.stale.after_ms,
                tx=tx,
            )
            if not claimed:
                # Someone else got it, re-fetch and handle
                updated = await self._store.find_by_idem_key(effect.idem_key, tx)
                if updated and is_terminal_status(updated.status):
                    if updated.status == EffectStatus.SUCCEEDED:
                        return updated.result
                    if updated.status == EffectStatus.FAILED and updated.error:
                        raise EffectFailedError(
                            updated.idem_key,
                            {
                                "code": updated.error.code,
                                "message": updated.error.message,
                            },
                        )
                raise RuntimeError(
                    f"Effect {effect.idem_key} claim failed, unexpected state"
                )
            return await self._execute_handler(effect, handler, tx)

        raise RuntimeError(
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

            await self._store.transition(
                effect.id,
                from_status,
                EffectStatus.SUCCEEDED,
                result=result,
                tx=tx,
            )

            return result
        except Exception as err:
            error = {
                "code": getattr(err, "code", None),
                "message": str(err),
            }

            current = await self._store.find_by_idem_key(effect.idem_key, tx)
            from_status = current.status if current else effect.status

            await self._store.transition(
                effect.id,
                from_status,
                EffectStatus.FAILED,
                error=error,
                tx=tx,
            )

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
