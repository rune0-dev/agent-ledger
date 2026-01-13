from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

import pytest

from effect_ledger import (
    ConcurrencyOptions,
    EffectDeniedError,
    EffectFailedError,
    EffectLedger,
    EffectLedgerOptions,
    EffectLedgerValidationError,
    EffectStatus,
    EffectStoreError,
    EffectTimeoutError,
    MemoryStore,
    RunOptions,
    StaleOptions,
    ToolCall,
)
from effect_ledger.types import Effect, UpsertEffectInput

memory_only = pytest.mark.parametrize("store", ["memory"], indirect=True)


def make_call(**overrides: Any) -> ToolCall:
    return ToolCall(
        workflow_id=overrides.get("workflow_id", "test-workflow"),
        tool=overrides.get("tool", "test.tool"),
        args=overrides.get("args", {"key": "value"}),
        call_id=overrides.get("call_id"),
        resource=overrides.get("resource"),
        idempotency_keys=overrides.get("idempotency_keys"),
    )


# -----------------------------------------------------------------------------
# Core API: begin(), commit(), run()
# -----------------------------------------------------------------------------


class TestBegin:
    async def test_creates_fresh_effect_on_first_call(
        self, ledger: EffectLedger[None]
    ) -> None:
        result = await ledger.begin(make_call())

        assert result.idempotency_status == "fresh"
        assert result.cached is False
        assert result.effect.status.value == "processing"
        assert result.effect.tool == "test.tool"

    async def test_returns_replayed_status_on_duplicate_call(
        self, ledger: EffectLedger[None]
    ) -> None:
        call = make_call()

        await ledger.begin(call)
        result = await ledger.begin(call)

        assert result.idempotency_status == "replayed"
        assert result.cached is False

    async def test_returns_cached_result_for_terminal_effect(
        self, ledger: EffectLedger[None]
    ) -> None:
        call = make_call()
        begin_result = await ledger.begin(call)

        from effect_ledger import CommitSucceeded

        await ledger.commit(begin_result.effect.id, CommitSucceeded(result="done"))

        result = await ledger.begin(call)

        assert result.idempotency_status == "replayed"
        assert result.cached is True
        assert result.cached_result == "done"

    async def test_increments_dedup_count_on_replays(
        self, store: Any, ledger: EffectLedger[None]
    ) -> None:
        call = make_call()

        await ledger.begin(call)
        await ledger.begin(call)
        await ledger.begin(call)

        result = await ledger.begin(call)
        effect = await store.find_by_idem_key(result.effect.idem_key)
        assert effect is not None
        assert effect.dedup_count == 3


class TestCommit:
    async def test_transitions_to_succeeded_with_result(
        self, ledger: EffectLedger[None]
    ) -> None:
        begin_result = await ledger.begin(make_call())

        from effect_ledger import CommitSucceeded

        await ledger.commit(
            begin_result.effect.id,
            CommitSucceeded(result={"data": 123}),
        )

        updated = await ledger.get_effect(begin_result.effect.id)
        assert updated is not None
        assert updated.status.value == "succeeded"
        assert updated.result == {"data": 123}

    async def test_transitions_to_failed_with_error(
        self, ledger: EffectLedger[None]
    ) -> None:
        begin_result = await ledger.begin(make_call())

        from effect_ledger import CommitFailed, EffectError

        await ledger.commit(
            begin_result.effect.id,
            CommitFailed(
                error=EffectError(code="ERR_TEST", message="Something went wrong")
            ),
        )

        updated = await ledger.get_effect(begin_result.effect.id)
        assert updated is not None
        assert updated.status.value == "failed"
        assert updated.error is not None
        assert updated.error.code == "ERR_TEST"


class TestRun:
    @memory_only
    async def test_executes_handler_and_commits_success(
        self, store: MemoryStore, ledger: EffectLedger[None]
    ) -> None:
        async def handler(effect):
            return {"executed": True}

        result = await ledger.run(make_call(), handler)

        assert result == {"executed": True}
        assert store.size == 1

        effects = store.list_effects()
        assert effects[0].status.value == "succeeded"

    async def test_returns_cached_result_on_replay(
        self, ledger: EffectLedger[None]
    ) -> None:
        call = make_call()
        call_count = 0

        async def handler(effect):
            nonlocal call_count
            call_count += 1
            return {"count": call_count}

        first = await ledger.run(call, handler)
        second = await ledger.run(call, handler)
        third = await ledger.run(call, handler)

        assert first == {"count": 1}
        assert second == {"count": 1}
        assert third == {"count": 1}
        assert call_count == 1

    @memory_only
    async def test_commits_failure_and_rethrows_on_error(
        self, store: MemoryStore, ledger: EffectLedger[None]
    ) -> None:
        call = make_call()

        async def handler(effect):
            raise ValueError("Handler failed")

        with pytest.raises(ValueError, match="Handler failed"):
            await ledger.run(call, handler)

        effects = store.list_effects()
        assert effects[0].status.value == "failed"
        assert effects[0].error is not None
        assert effects[0].error.message == "Handler failed"

    async def test_throws_effect_failed_error_on_replayed_failure(
        self, ledger: EffectLedger[None]
    ) -> None:
        call = make_call()

        async def failing_handler(effect):
            raise ValueError("Original error")

        async def success_handler(effect):
            return "should not run"

        with pytest.raises(ValueError, match="Original error"):
            await ledger.run(call, failing_handler)

        with pytest.raises(EffectFailedError):
            await ledger.run(call, success_handler)

    @memory_only
    async def test_commit_race_returns_winner_result_on_success(
        self, store: MemoryStore
    ) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"commit_race": "success"})

        handler_started = asyncio.Event()
        handler_continue = asyncio.Event()

        async def slow_handler(eff):
            handler_started.set()
            await handler_continue.wait()
            return {"from": "slow_handler"}

        task = asyncio.create_task(ledger.run(call, slow_handler))
        await handler_started.wait()

        effect = store.list_effects()[0]
        await store.transition(
            effect.id,
            EffectStatus.PROCESSING,
            EffectStatus.SUCCEEDED,
            result={"from": "fast_worker"},
        )

        handler_continue.set()
        result = await task

        assert result == {"from": "fast_worker"}

    @memory_only
    async def test_commit_race_returns_winner_result_on_failure(
        self, store: MemoryStore
    ) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"commit_race": "failure"})

        handler_started = asyncio.Event()
        handler_continue = asyncio.Event()

        async def failing_handler(eff):
            handler_started.set()
            await handler_continue.wait()
            raise ValueError("Handler failed")

        task = asyncio.create_task(ledger.run(call, failing_handler))
        await handler_started.wait()

        effect = store.list_effects()[0]
        await store.transition(
            effect.id,
            EffectStatus.PROCESSING,
            EffectStatus.SUCCEEDED,
            result={"from": "successful_worker"},
        )

        handler_continue.set()
        result = await task

        assert result == {"from": "successful_worker"}


# -----------------------------------------------------------------------------
# Idempotency Key Computation
# -----------------------------------------------------------------------------


class TestIdempotencyKey:
    async def test_same_key_for_same_tool_call(
        self, ledger: EffectLedger[None]
    ) -> None:
        call1 = make_call(args={"a": 1, "b": 2})
        call2 = make_call(args={"b": 2, "a": 1})

        e1 = await ledger.begin(call1)
        e2 = await ledger.begin(call2)

        assert e1.effect.idem_key == e2.effect.idem_key

    async def test_different_keys_for_different_args(
        self, ledger: EffectLedger[None]
    ) -> None:
        e1 = await ledger.begin(make_call(args={"x": 1}))
        e2 = await ledger.begin(make_call(args={"x": 2}))

        assert e1.effect.idem_key != e2.effect.idem_key

    async def test_uses_resource_descriptor_when_provided(
        self, ledger: EffectLedger[None]
    ) -> None:
        from effect_ledger import ResourceDescriptor

        call1 = make_call(
            resource=ResourceDescriptor(
                namespace="slack",
                type="channel",
                id={"name": "#general"},
            ),
            args={"text": "hello"},
        )
        call2 = make_call(
            resource=ResourceDescriptor(
                namespace="slack",
                type="channel",
                id={"name": "#general"},
            ),
            args={"text": "different"},
        )

        e1 = await ledger.begin(call1)
        e2 = await ledger.begin(call2)

        assert e1.effect.idem_key == e2.effect.idem_key

    async def test_uses_idempotency_keys_subset(
        self, ledger: EffectLedger[None]
    ) -> None:
        call1 = make_call(
            args={"user_id": "u1", "timestamp": 1000, "data": "a"},
            idempotency_keys=["user_id"],
        )
        call2 = make_call(
            args={"user_id": "u1", "timestamp": 2000, "data": "b"},
            idempotency_keys=["user_id"],
        )

        e1 = await ledger.begin(call1)
        e2 = await ledger.begin(call2)

        assert e1.effect.idem_key == e2.effect.idem_key


# -----------------------------------------------------------------------------
# Lookup
# -----------------------------------------------------------------------------


class TestLookup:
    async def test_find_by_idem_key(self, ledger: EffectLedger[None]) -> None:
        begin_result = await ledger.begin(make_call())

        found = await ledger.find_by_idem_key(begin_result.effect.idem_key)

        assert found is not None
        assert found.id == begin_result.effect.id

    async def test_find_by_idem_key_unknown_returns_none(
        self, ledger: EffectLedger[None]
    ) -> None:
        found = await ledger.find_by_idem_key("unknown-key")
        assert found is None

    async def test_get_effect_unknown_returns_none(
        self, ledger: EffectLedger[None]
    ) -> None:
        result = await ledger.get_effect("00000000-0000-0000-0000-000000000000")
        assert result is None


# -----------------------------------------------------------------------------
# Approval Flow
# -----------------------------------------------------------------------------


class TestApprovalFlow:
    async def test_request_approval(
        self, store: Any, ledger: EffectLedger[None]
    ) -> None:
        call = make_call(args={"approval": "test1"})

        begin_result = await ledger.begin(call)
        await ledger.request_approval(begin_result.effect.idem_key)

        updated = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert updated is not None
        assert updated.status.value == "requires_approval"

    async def test_approve_transitions_to_ready(
        self, store: Any, ledger: EffectLedger[None]
    ) -> None:
        call = make_call(args={"approval": "test2"})

        begin_result = await ledger.begin(call)
        await ledger.request_approval(begin_result.effect.idem_key)
        await ledger.approve(begin_result.effect.idem_key)

        updated = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert updated is not None
        assert updated.status.value == "ready"

    async def test_deny_with_reason(
        self, store: Any, ledger: EffectLedger[None]
    ) -> None:
        call = make_call(args={"approval": "test3"})

        begin_result = await ledger.begin(call)
        await ledger.request_approval(begin_result.effect.idem_key)
        await ledger.deny(begin_result.effect.idem_key, "Not authorized")

        updated = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert updated is not None
        assert updated.status.value == "denied"
        assert updated.error is not None
        assert updated.error.message == "Not authorized"

    async def test_approve_nonexistent_returns_false(
        self, ledger: EffectLedger[None]
    ) -> None:
        result = await ledger.approve("nonexistent-key")
        assert result is False

    async def test_deny_nonexistent_returns_false(
        self, ledger: EffectLedger[None]
    ) -> None:
        result = await ledger.deny("nonexistent-key", "reason")
        assert result is False

    async def test_request_approval_nonexistent_returns_false(
        self, ledger: EffectLedger[None]
    ) -> None:
        result = await ledger.request_approval("nonexistent-key")
        assert result is False

    async def test_approve_wrong_status_returns_false(
        self, store: Any, ledger: EffectLedger[None]
    ) -> None:
        call = make_call(args={"approve": "wrong_status"})
        begin_result = await ledger.begin(call)

        result = await ledger.approve(begin_result.effect.idem_key)
        assert result is False

        effect = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert effect is not None
        assert effect.status == EffectStatus.PROCESSING

    async def test_deny_wrong_status_returns_false(
        self, store: Any, ledger: EffectLedger[None]
    ) -> None:
        call = make_call(args={"deny": "wrong_status"})
        begin_result = await ledger.begin(call)

        result = await ledger.deny(begin_result.effect.idem_key, "reason")
        assert result is False

        effect = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert effect is not None
        assert effect.status == EffectStatus.PROCESSING

    @memory_only
    async def test_denied_during_wait_raises_error(self, store: MemoryStore) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"denied": "during_wait"})

        waiter_started = asyncio.Event()

        async def handler(eff):
            return {"should": "not run"}

        async def waiter():
            waiter_started.set()
            return await ledger.run(
                call,
                handler,
                run_options=RunOptions(requires_approval=True),
            )

        task = asyncio.create_task(waiter())
        await waiter_started.wait()
        await asyncio.sleep(0.02)

        effect = store.list_effects()[0]
        await ledger.deny(effect.idem_key, "Access denied")

        with pytest.raises(EffectDeniedError) as exc_info:
            await task

        assert exc_info.value.reason == "Access denied"

    @memory_only
    async def test_canceled_during_wait_raises_error(self, store: MemoryStore) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"canceled": "during_wait"})

        waiter_started = asyncio.Event()

        async def handler(eff):
            return {"should": "not run"}

        async def waiter():
            waiter_started.set()
            return await ledger.run(
                call,
                handler,
                run_options=RunOptions(requires_approval=True),
            )

        task = asyncio.create_task(waiter())
        await waiter_started.wait()
        await asyncio.sleep(0.02)

        effect = store.list_effects()[0]
        await store.transition(
            effect.id,
            EffectStatus.REQUIRES_APPROVAL,
            EffectStatus.CANCELED,
            error={"message": "System shutdown"},
        )

        with pytest.raises(EffectDeniedError):
            await task

    async def test_approve_deny_race_only_one_succeeds(self, store: Any) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"approve_deny": "race"})

        begin_result = await ledger.begin(call)
        await ledger.request_approval(begin_result.effect.idem_key)

        approve_result, deny_result = await asyncio.gather(
            ledger.approve(begin_result.effect.idem_key),
            ledger.deny(begin_result.effect.idem_key, "denied"),
        )

        assert (approve_result, deny_result) in [(True, False), (False, True)]

        effect = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert effect is not None
        assert effect.status in (EffectStatus.READY, EffectStatus.DENIED)


# -----------------------------------------------------------------------------
# Concurrency
# -----------------------------------------------------------------------------


class TestConcurrency:
    async def test_concurrent_ready_claim_only_one_executes(self, store: Any) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"concurrent": "ready_claim"})

        begin_result = await ledger.begin(call)
        await ledger.request_approval(begin_result.effect.idem_key)
        await ledger.approve(begin_result.effect.idem_key)

        effect = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert effect is not None
        assert effect.status == EffectStatus.READY

        execution_count = 0
        execution_lock = asyncio.Lock()

        async def counting_handler(eff):
            nonlocal execution_count
            async with execution_lock:
                execution_count += 1
                current = execution_count
            await asyncio.sleep(0.01)
            return {"executed_by": current}

        tasks = [ledger.run(call, counting_handler) for _ in range(5)]
        results = await asyncio.gather(*tasks)

        assert all(r == {"executed_by": 1} for r in results)
        assert execution_count == 1

    async def test_concurrent_processing_only_one_executes(self, store: Any) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"concurrent": "processing"})

        execution_count = 0
        execution_lock = asyncio.Lock()
        started = asyncio.Event()

        async def slow_handler(eff):
            nonlocal execution_count
            async with execution_lock:
                execution_count += 1
            started.set()
            await asyncio.sleep(0.05)
            return {"result": "done"}

        async def worker():
            return await ledger.run(call, slow_handler)

        task1 = asyncio.create_task(worker())
        await started.wait()

        tasks = [asyncio.create_task(worker()) for _ in range(4)]
        tasks.insert(0, task1)

        results = await asyncio.gather(*tasks)

        assert all(r == {"result": "done"} for r in results)
        assert execution_count == 1

    @memory_only
    async def test_stale_takeover_returns_winner_result(
        self, store: MemoryStore
    ) -> None:
        stale_options = RunOptions(stale=StaleOptions(after_ms=50))
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"stale": "takeover"})

        worker_a_started = asyncio.Event()
        worker_a_continue = asyncio.Event()

        async def slow_handler_a(eff):
            worker_a_started.set()
            await worker_a_continue.wait()
            return {"from": "worker_a"}

        async def fast_handler_b(eff):
            return {"from": "worker_b"}

        task_a = asyncio.create_task(
            ledger.run(call, slow_handler_a, run_options=stale_options)
        )
        await worker_a_started.wait()

        effects = store.list_effects()
        assert len(effects) == 1
        effect = effects[0]

        old_time = datetime.now(tz=timezone.utc) - timedelta(milliseconds=100)
        aged_effect = Effect(
            id=effect.id,
            idem_key=effect.idem_key,
            workflow_id=effect.workflow_id,
            call_id=effect.call_id,
            tool=effect.tool,
            status=effect.status,
            args_canonical=effect.args_canonical,
            resource_id_canonical=effect.resource_id_canonical,
            result=effect.result,
            error=effect.error,
            dedup_count=effect.dedup_count,
            created_at=effect.created_at,
            updated_at=old_time,
            completed_at=effect.completed_at,
        )
        store._cache[effect.id] = aged_effect

        result_b = await ledger.run(call, fast_handler_b, run_options=stale_options)
        assert result_b == {"from": "worker_b"}

        worker_a_continue.set()
        result_a = await task_a
        assert result_a == {"from": "worker_b"}

    async def test_transition_failure_returns_committed_result(
        self, store: Any
    ) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"transition": "race"})

        begin_result = await ledger.begin(call)
        effect_id = begin_result.effect.id

        await store.transition(
            effect_id,
            EffectStatus.PROCESSING,
            EffectStatus.SUCCEEDED,
            result={"from": "other_worker"},
        )

        async def our_handler(eff):
            return {"from": "our_handler"}

        result = await ledger.run(call, our_handler)
        assert result == {"from": "other_worker"}

    @memory_only
    async def test_concurrent_approval_only_one_executes(
        self, store: MemoryStore
    ) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"approval": "concurrent"})

        execution_count = 0
        waiters_ready = asyncio.Event()
        waiter_count = 0
        waiter_lock = asyncio.Lock()

        async def handler(eff):
            nonlocal execution_count
            execution_count += 1
            await asyncio.sleep(0.01)
            return {"executed": True}

        async def waiter():
            nonlocal waiter_count
            async with waiter_lock:
                waiter_count += 1
                if waiter_count >= 3:
                    waiters_ready.set()
            return await ledger.run(
                call,
                handler,
                run_options=RunOptions(requires_approval=True),
            )

        tasks = [asyncio.create_task(waiter()) for _ in range(3)]

        await waiters_ready.wait()
        await asyncio.sleep(0.02)

        effect = store.list_effects()[0]
        await ledger.approve(effect.idem_key)

        results = await asyncio.gather(*tasks)

        assert all(r == {"executed": True} for r in results)
        assert execution_count == 1

    @memory_only
    async def test_multiple_stale_takeover_only_one_executes(
        self, store: MemoryStore
    ) -> None:
        stale_options = RunOptions(stale=StaleOptions(after_ms=50))
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"multi_stale": "race"})

        begin_result = await ledger.begin(call)
        effect = begin_result.effect

        old_time = datetime.now(tz=timezone.utc) - timedelta(milliseconds=100)
        aged_effect = Effect(
            id=effect.id,
            idem_key=effect.idem_key,
            workflow_id=effect.workflow_id,
            call_id=effect.call_id,
            tool=effect.tool,
            status=effect.status,
            args_canonical=effect.args_canonical,
            resource_id_canonical=effect.resource_id_canonical,
            result=effect.result,
            error=effect.error,
            dedup_count=effect.dedup_count,
            created_at=effect.created_at,
            updated_at=old_time,
            completed_at=effect.completed_at,
        )
        store._cache[effect.id] = aged_effect

        execution_count = 0
        execution_lock = asyncio.Lock()

        async def handler(eff):
            nonlocal execution_count
            async with execution_lock:
                execution_count += 1
                current = execution_count
            await asyncio.sleep(0.01)
            return {"executed_by": current}

        tasks = [ledger.run(call, handler, run_options=stale_options) for _ in range(5)]
        results = await asyncio.gather(*tasks)

        assert all(r == {"executed_by": 1} for r in results)
        assert execution_count == 1

    async def test_wait_timeout_raises_error(self, store: Any) -> None:
        short_timeout = RunOptions(
            concurrency=ConcurrencyOptions(
                wait_timeout_ms=50,
                initial_interval_ms=10,
                max_interval_ms=20,
            )
        )
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"timeout": "test"})

        worker_started = asyncio.Event()

        async def slow_handler(eff):
            worker_started.set()
            await asyncio.sleep(10)
            return {"done": True}

        task_a = asyncio.create_task(ledger.run(call, slow_handler))
        await worker_started.wait()

        async def fast_handler(eff):
            return {"should": "not run"}

        with pytest.raises(EffectTimeoutError) as exc_info:
            await ledger.run(call, fast_handler, run_options=short_timeout)

        assert "50ms" in str(exc_info.value)

        task_a.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task_a


# -----------------------------------------------------------------------------
# Store Behavior
# -----------------------------------------------------------------------------


class TestStore:
    async def test_upsert_does_not_overwrite_terminal_status(self, store: Any) -> None:
        input1 = UpsertEffectInput(
            idem_key="test-terminal-protection",
            workflow_id="wf1",
            call_id="c1",
            tool="test.tool",
            status=EffectStatus.PROCESSING,
            args_canonical='{"key": "value"}',
            resource_id_canonical="test.tool",
        )
        result1 = await store.upsert(input1)
        assert result1.created is True
        effect_id = result1.effect.id

        await store.transition(
            effect_id,
            EffectStatus.PROCESSING,
            EffectStatus.SUCCEEDED,
            result={"original": "result"},
        )

        input2 = UpsertEffectInput(
            idem_key="test-terminal-protection",
            workflow_id="wf1",
            call_id="c1",
            tool="test.tool",
            status=EffectStatus.PROCESSING,
            args_canonical='{"key": "value"}',
            resource_id_canonical="test.tool",
            result={"new": "result"},
        )
        result2 = await store.upsert(input2)

        assert result2.created is False
        assert result2.effect.status == EffectStatus.SUCCEEDED
        assert result2.effect.result == {"original": "result"}

    async def test_transition_fails_on_status_mismatch(self, store: Any) -> None:
        input_data = UpsertEffectInput(
            idem_key="test-cas",
            workflow_id="wf1",
            call_id="c1",
            tool="test.tool",
            status=EffectStatus.PROCESSING,
            args_canonical='{"key": "value"}',
            resource_id_canonical="test.tool",
        )
        result = await store.upsert(input_data)
        effect_id = result.effect.id

        success = await store.transition(
            effect_id,
            EffectStatus.READY,
            EffectStatus.SUCCEEDED,
            result={"data": 123},
        )

        assert success is False

        effect = await store.find_by_id(effect_id)
        assert effect is not None
        assert effect.status == EffectStatus.PROCESSING
        assert effect.result is None

    async def test_transition_fails_from_terminal_status(self, store: Any) -> None:
        input_data = UpsertEffectInput(
            idem_key="test-terminal-transition",
            workflow_id="wf1",
            call_id="c1",
            tool="test.tool",
            status=EffectStatus.PROCESSING,
            args_canonical='{"key": "value"}',
            resource_id_canonical="test.tool",
        )
        result = await store.upsert(input_data)
        effect_id = result.effect.id

        await store.transition(
            effect_id,
            EffectStatus.PROCESSING,
            EffectStatus.SUCCEEDED,
            result={"done": True},
        )

        success = await store.transition(
            effect_id,
            EffectStatus.SUCCEEDED,
            EffectStatus.FAILED,
            error={"message": "oops"},
        )

        assert success is False

        effect = await store.find_by_id(effect_id)
        assert effect is not None
        assert effect.status == EffectStatus.SUCCEEDED


# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------


class TestValidation:
    async def test_rejects_non_json_serializable_args(self, store: Any) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))

        with pytest.raises(EffectLedgerValidationError) as exc_info:
            await ledger.begin(make_call(args={"func": lambda x: x}))

        assert exc_info.value.field == "args"
        assert "JSON-serializable" in str(exc_info.value)

    async def test_rejects_args_exceeding_size_limit(self, store: Any) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store, max_args_size_bytes=100))

        with pytest.raises(EffectLedgerValidationError) as exc_info:
            await ledger.begin(make_call(args={"data": "x" * 200}))

        assert exc_info.value.field == "args"
        assert "exceed maximum size" in str(exc_info.value)

    async def test_rejects_non_dict_args(self, store: Any) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = ToolCall(
            workflow_id="test",
            tool="test.tool",
            args=["not", "a", "dict"],  # type: ignore
        )

        with pytest.raises(EffectLedgerValidationError) as exc_info:
            await ledger.begin(call)

        assert exc_info.value.field == "args"
        assert "must be a dict" in str(exc_info.value)


# -----------------------------------------------------------------------------
# Error Handling
# -----------------------------------------------------------------------------


class TestErrorHandling:
    async def test_store_error_wraps_backend_exceptions(self) -> None:
        store = MemoryStore()

        with (
            patch.object(
                store._cache, "get", side_effect=RuntimeError("Cache corrupted")
            ),
            pytest.raises(EffectStoreError) as exc_info,
        ):
            await store.find_by_id("some-id")

        assert exc_info.value.operation == "find_by_id"
        assert exc_info.value.effect_id == "some-id"
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, RuntimeError)
        assert "Cache corrupted" in str(exc_info.value.__cause__)

    async def test_store_error_str_includes_context(self) -> None:
        err = EffectStoreError(
            "Test error",
            operation="upsert",
            idem_key="test-key",
            effect_id="test-id",
        )

        err_str = str(err)
        assert "Test error" in err_str
        assert "operation=upsert" in err_str
        assert "idem_key=test-key" in err_str
        assert "effect_id=test-id" in err_str

    async def test_store_error_not_double_wrapped(self) -> None:
        store = MemoryStore()

        original_store_error = EffectStoreError(
            "Already wrapped",
            operation="inner_op",
        )

        with (
            patch.object(store._cache, "get", side_effect=original_store_error),
            pytest.raises(EffectStoreError) as exc_info,
        ):
            await store.find_by_id("some-id")

        assert exc_info.value is original_store_error
        assert exc_info.value.operation == "inner_op"

    async def test_store_error_from_transition(self) -> None:
        store = MemoryStore()
        ledger = EffectLedger(EffectLedgerOptions(store=store))

        begin_result = await ledger.begin(make_call())
        effect_id = begin_result.effect.id

        with (
            patch.object(store._cache, "get", side_effect=OSError("Disk full")),
            pytest.raises(EffectStoreError) as exc_info,
        ):
            await store.transition(
                effect_id,
                EffectStatus.PROCESSING,
                EffectStatus.SUCCEEDED,
                result={"ok": True},
            )

        assert exc_info.value.operation == "transition"
        assert exc_info.value.effect_id == effect_id
        assert "Disk full" in str(exc_info.value.__cause__)
