import pytest

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from effect_ledger import (
    EffectFailedError,
    EffectLedger,
    EffectLedgerOptions,
    EffectLedgerValidationError,
    EffectStatus,
    MemoryStore,
    RunOptions,
    StaleOptions,
    ToolCall,
)


def make_call(**overrides) -> ToolCall:
    return ToolCall(
        workflow_id=overrides.get("workflow_id", "test-workflow"),
        tool=overrides.get("tool", "test.tool"),
        args=overrides.get("args", {"key": "value"}),
        call_id=overrides.get("call_id"),
        resource=overrides.get("resource"),
        idempotency_keys=overrides.get("idempotency_keys"),
    )


@pytest.fixture
def store() -> MemoryStore:
    return MemoryStore()


@pytest.fixture
def ledger(store: MemoryStore) -> EffectLedger[None]:
    return EffectLedger(EffectLedgerOptions(store=store))


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
        self, store: MemoryStore, ledger: EffectLedger[None]
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


class TestIdempotencyKeyComputation:
    async def test_generates_same_key_for_same_tool_call(
        self, ledger: EffectLedger[None]
    ) -> None:
        call1 = make_call(args={"a": 1, "b": 2})
        call2 = make_call(args={"b": 2, "a": 1})

        e1 = await ledger.begin(call1)
        e2 = await ledger.begin(call2)

        assert e1.effect.idem_key == e2.effect.idem_key

    async def test_generates_different_keys_for_different_args(
        self, ledger: EffectLedger[None]
    ) -> None:
        e1 = await ledger.begin(make_call(args={"x": 1}))
        e2 = await ledger.begin(make_call(args={"x": 2}))

        assert e1.effect.idem_key != e2.effect.idem_key

    async def test_uses_resource_descriptor_for_key_when_provided(
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

    async def test_uses_idempotency_keys_subset_when_provided(
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


class TestFindByIdemKey:
    async def test_finds_effect_by_idempotency_key(
        self, ledger: EffectLedger[None]
    ) -> None:
        begin_result = await ledger.begin(make_call())

        found = await ledger.find_by_idem_key(begin_result.effect.idem_key)

        assert found is not None
        assert found.id == begin_result.effect.id

    async def test_returns_none_for_unknown_key(
        self, ledger: EffectLedger[None]
    ) -> None:
        found = await ledger.find_by_idem_key("unknown-key")
        assert found is None


class TestApprovalFlow:
    async def test_requests_approval_and_wait_for_it(
        self, store: MemoryStore, ledger: EffectLedger[None]
    ) -> None:
        call = make_call(args={"approval": "test1"})

        begin_result = await ledger.begin(call)
        await ledger.request_approval(begin_result.effect.idem_key)

        updated = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert updated is not None
        assert updated.status.value == "requires_approval"

    async def test_approves_effect_and_transitions_to_ready(
        self, store: MemoryStore, ledger: EffectLedger[None]
    ) -> None:
        call = make_call(args={"approval": "test2"})

        begin_result = await ledger.begin(call)
        await ledger.request_approval(begin_result.effect.idem_key)
        await ledger.approve(begin_result.effect.idem_key)

        updated = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert updated is not None
        assert updated.status.value == "ready"

    async def test_denies_effect_with_reason(
        self, store: MemoryStore, ledger: EffectLedger[None]
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


class TestConcurrency:
    """Tests for concurrent execution scenarios and race condition handling."""

    async def test_concurrent_ready_claim_only_one_handler_executes(
        self, store: MemoryStore
    ) -> None:
        """When multiple workers race to claim a READY effect, only one executes."""
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"concurrent": "ready_claim"})

        # Set up: create effect and move to READY state
        begin_result = await ledger.begin(call)
        await ledger.request_approval(begin_result.effect.idem_key)
        await ledger.approve(begin_result.effect.idem_key)

        # Verify it's in READY state
        effect = await store.find_by_idem_key(begin_result.effect.idem_key)
        assert effect is not None
        assert effect.status == EffectStatus.READY

        # Track handler executions
        execution_count = 0
        execution_lock = asyncio.Lock()

        async def counting_handler(eff):
            nonlocal execution_count
            async with execution_lock:
                execution_count += 1
                current = execution_count
            await asyncio.sleep(0.01)  # Simulate work
            return {"executed_by": current}

        # Race: 5 concurrent workers try to run the same effect
        tasks = [ledger.run(call, counting_handler) for _ in range(5)]
        results = await asyncio.gather(*tasks)

        # All should get the same result
        assert all(r == {"executed_by": 1} for r in results)
        # Handler should only execute once
        assert execution_count == 1

    async def test_concurrent_processing_only_one_handler_executes(
        self, store: MemoryStore
    ) -> None:
        """When multiple workers call run() simultaneously, only one executes."""
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
            await asyncio.sleep(0.05)  # Simulate work
            return {"result": "done"}

        async def worker():
            return await ledger.run(call, slow_handler)

        # Start first worker
        task1 = asyncio.create_task(worker())
        await started.wait()  # Wait for it to start

        # Start 4 more workers while first is running
        tasks = [asyncio.create_task(worker()) for _ in range(4)]
        tasks.insert(0, task1)

        results = await asyncio.gather(*tasks)

        # All get same result
        assert all(r == {"result": "done"} for r in results)
        # Only one execution
        assert execution_count == 1

    async def test_stale_takeover_returns_winner_result(
        self, store: MemoryStore
    ) -> None:
        """When worker B takes over stale effect from A, B's result is returned."""
        # Very short stale threshold for testing
        stale_options = RunOptions(stale=StaleOptions(after_ms=50))
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"stale": "takeover"})

        worker_a_started = asyncio.Event()
        worker_a_continue = asyncio.Event()

        async def slow_handler_a(eff):
            worker_a_started.set()
            await worker_a_continue.wait()  # Wait until told to continue
            return {"from": "worker_a"}

        async def fast_handler_b(eff):
            return {"from": "worker_b"}

        # Worker A starts but hangs
        task_a = asyncio.create_task(
            ledger.run(call, slow_handler_a, run_options=stale_options)
        )
        await worker_a_started.wait()

        # Get effect directly from store
        effects = store.list_effects()
        assert len(effects) == 1
        effect = effects[0]

        # Artificially age the effect by modifying updated_at
        old_time = datetime.now(tz=timezone.utc) - timedelta(milliseconds=100)
        aged_effect = effect.__class__(
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
            updated_at=old_time,  # Make it look stale
            completed_at=effect.completed_at,
        )
        store._cache[effect.id] = aged_effect

        # Worker B takes over
        result_b = await ledger.run(call, fast_handler_b, run_options=stale_options)

        # Worker B should win
        assert result_b == {"from": "worker_b"}

        # Let worker A continue - it should return B's result (not its own)
        worker_a_continue.set()
        result_a = await task_a

        # Worker A should also return B's result (the committed one)
        assert result_a == {"from": "worker_b"}

    async def test_transition_failure_returns_actual_committed_result(
        self, store: MemoryStore
    ) -> None:
        """When transition fails (someone else committed), return their result."""
        ledger = EffectLedger(EffectLedgerOptions(store=store))
        call = make_call(args={"transition": "race"})

        # Begin effect
        begin_result = await ledger.begin(call)
        effect_id = begin_result.effect.id
        idem_key = begin_result.effect.idem_key

        # Simulate: another worker already committed a different result
        await store.transition(
            effect_id,
            EffectStatus.PROCESSING,
            EffectStatus.SUCCEEDED,
            result={"from": "other_worker"},
        )

        # Now our handler runs but transition will fail
        async def our_handler(eff):
            return {"from": "our_handler"}

        # Calling run again should return the cached result
        result = await ledger.run(call, our_handler)

        # Should get the already-committed result
        assert result == {"from": "other_worker"}

    async def test_concurrent_approval_only_one_executes_after_approve(
        self, store: MemoryStore
    ) -> None:
        """Multiple waiters on approval, only one executes after approved."""
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

        # Start 3 waiters
        tasks = [asyncio.create_task(waiter()) for _ in range(3)]

        # Wait for all to be waiting
        await waiters_ready.wait()
        await asyncio.sleep(0.02)  # Let them enter wait state

        # Approve
        effect = store.list_effects()[0]
        await ledger.approve(effect.idem_key)

        # All waiters should complete
        results = await asyncio.gather(*tasks)

        # All should get same result
        assert all(r == {"executed": True} for r in results)
        # Only one execution
        assert execution_count == 1


class TestValidation:
    async def test_rejects_non_json_serializable_args(self, store: MemoryStore) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store))

        with pytest.raises(EffectLedgerValidationError) as exc_info:
            await ledger.begin(make_call(args={"func": lambda x: x}))

        assert exc_info.value.field == "args"
        assert "JSON-serializable" in str(exc_info.value)

    async def test_rejects_args_exceeding_size_limit(self, store: MemoryStore) -> None:
        ledger = EffectLedger(EffectLedgerOptions(store=store, max_args_size_bytes=100))

        with pytest.raises(EffectLedgerValidationError) as exc_info:
            await ledger.begin(make_call(args={"data": "x" * 200}))

        assert exc_info.value.field == "args"
        assert "exceed maximum size" in str(exc_info.value)

    async def test_rejects_non_dict_args(self, store: MemoryStore) -> None:
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
