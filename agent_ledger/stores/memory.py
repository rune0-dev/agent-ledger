from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from cachetools import TTLCache

from agent_ledger.errors import EffectStoreError
from agent_ledger.types import (
    Effect,
    EffectError,
    EffectStatus,
    UpsertEffectResult,
    is_terminal_status,
    is_valid_transition,
)
from agent_ledger.utils import generate_id

if TYPE_CHECKING:
    from agent_ledger.types import JsonValue, UpsertEffectInput

DEFAULT_MAX_SIZE = 10_000
DEFAULT_TTL_SECONDS = 60 * 60
STALE_THRESHOLD_MS = 60_000


class MemoryStore:
    """In-memory effect store with TTL-based eviction.

    Uses a single cache keyed by idem_key (not effect_id) to avoid
    index synchronization issues when TTLCache evicts entries.
    """

    def __init__(
        self,
        max_size: int = DEFAULT_MAX_SIZE,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
    ) -> None:
        # Single cache keyed by idem_key - eliminates dual-index sync issues
        self._cache: TTLCache[str, Effect] = TTLCache(
            maxsize=max_size,
            ttl=ttl_seconds,
        )
        # Secondary index for id -> idem_key lookups (cleaned on access)
        self._id_to_idem_key: dict[str, str] = {}
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        yield None

    async def find_by_idem_key(
        self,
        idem_key: str,
        tx: None = None,
    ) -> Effect | None:
        try:
            async with self._lock:
                result: Effect | None = self._cache.get(idem_key)
                return result
        except EffectStoreError:
            raise
        except Exception as err:
            raise EffectStoreError(
                f"Failed to find effect by idem_key: {err}",
                operation="find_by_idem_key",
                idem_key=idem_key,
            ) from err

    async def find_by_id(
        self,
        effect_id: str,
        tx: None = None,
    ) -> Effect | None:
        try:
            async with self._lock:
                idem_key = self._id_to_idem_key.get(effect_id)
                if idem_key is None:
                    return None
                result: Effect | None = self._cache.get(idem_key)
                if result is None:
                    del self._id_to_idem_key[effect_id]
                return result
        except EffectStoreError:
            raise
        except Exception as err:
            raise EffectStoreError(
                f"Failed to find effect by id: {err}",
                operation="find_by_id",
                effect_id=effect_id,
            ) from err

    async def upsert(
        self,
        input_data: UpsertEffectInput,
        tx: None = None,
    ) -> UpsertEffectResult:
        try:
            async with self._lock:
                existing = self._cache.get(input_data.idem_key)

                if existing is not None:
                    return UpsertEffectResult(effect=existing, created=False)

                now = datetime.now(tz=timezone.utc)
                effect_id = generate_id()

                effect = Effect(
                    id=effect_id,
                    idem_key=input_data.idem_key,
                    workflow_id=input_data.workflow_id,
                    call_id=input_data.call_id,
                    tool=input_data.tool,
                    status=input_data.status,
                    args_canonical=input_data.args_canonical,
                    resource_id_canonical=input_data.resource_id_canonical,
                    result=input_data.result,
                    error=input_data.error,
                    dedup_count=0,
                    created_at=now,
                    updated_at=now,
                    completed_at=now if is_terminal_status(input_data.status) else None,
                )

                self._cache[input_data.idem_key] = effect
                self._id_to_idem_key[effect_id] = input_data.idem_key

                return UpsertEffectResult(effect=effect, created=True)
        except EffectStoreError:
            raise
        except Exception as err:
            raise EffectStoreError(
                f"Failed to upsert effect: {err}",
                operation="upsert",
                idem_key=input_data.idem_key,
            ) from err

    async def transition(
        self,
        effect_id: str,
        from_status: EffectStatus,
        to_status: EffectStatus,
        result: JsonValue = None,
        error: dict[str, str | None] | None = None,
        tx: None = None,
    ) -> bool:
        try:
            async with self._lock:
                idem_key = self._id_to_idem_key.get(effect_id)
                if idem_key is None:
                    return False

                effect = self._cache.get(idem_key)
                if effect is None:
                    del self._id_to_idem_key[effect_id]
                    return False

                if effect.status != from_status:
                    return False

                if not is_valid_transition(from_status, to_status):
                    return False

                now = datetime.now(tz=timezone.utc)
                error_obj = (
                    EffectError(
                        message=error.get("message") or "",
                        code=error.get("code"),
                    )
                    if error
                    else None
                )

                updated = Effect(
                    id=effect.id,
                    idem_key=effect.idem_key,
                    workflow_id=effect.workflow_id,
                    call_id=effect.call_id,
                    tool=effect.tool,
                    status=to_status,
                    args_canonical=effect.args_canonical,
                    resource_id_canonical=effect.resource_id_canonical,
                    result=result if result is not None else effect.result,
                    error=error_obj if error_obj else effect.error,
                    dedup_count=effect.dedup_count,
                    created_at=effect.created_at,
                    updated_at=now,
                    completed_at=now
                    if is_terminal_status(to_status)
                    else effect.completed_at,
                )

                self._cache[idem_key] = updated
                return True
        except EffectStoreError:
            raise
        except Exception as err:
            raise EffectStoreError(
                f"Failed to transition effect: {err}",
                operation="transition",
                effect_id=effect_id,
                details={
                    "from_status": from_status.value,
                    "to_status": to_status.value,
                },
            ) from err

    async def increment_dedup_count(
        self,
        idem_key: str,
        tx: None = None,
    ) -> None:
        try:
            async with self._lock:
                effect = self._cache.get(idem_key)
                if effect is None:
                    return

                now = datetime.now(tz=timezone.utc)
                updated = Effect(
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
                    dedup_count=effect.dedup_count + 1,
                    created_at=effect.created_at,
                    updated_at=now,
                    completed_at=effect.completed_at,
                )
                self._cache[idem_key] = updated
        except EffectStoreError:
            raise
        except Exception as err:
            raise EffectStoreError(
                f"Failed to increment dedup count: {err}",
                operation="increment_dedup_count",
                idem_key=idem_key,
            ) from err

    async def claim_for_processing(
        self,
        effect_id: str,
        from_status: EffectStatus,
        stale_threshold_ms: int | None = None,
        tx: None = None,
    ) -> bool:
        try:
            async with self._lock:
                idem_key = self._id_to_idem_key.get(effect_id)
                if idem_key is None:
                    return False

                effect = self._cache.get(idem_key)
                if effect is None:
                    del self._id_to_idem_key[effect_id]
                    return False

                now = datetime.now(tz=timezone.utc)

                if (
                    from_status == EffectStatus.PROCESSING
                    and stale_threshold_ms is not None
                ):
                    if effect.status != EffectStatus.PROCESSING:
                        return False
                    age_ms = (now - effect.updated_at).total_seconds() * 1000
                    if age_ms <= stale_threshold_ms:
                        return False
                else:
                    if effect.status != from_status:
                        return False
                    if is_terminal_status(effect.status):
                        return False

                updated = Effect(
                    id=effect.id,
                    idem_key=effect.idem_key,
                    workflow_id=effect.workflow_id,
                    call_id=effect.call_id,
                    tool=effect.tool,
                    status=EffectStatus.PROCESSING,
                    args_canonical=effect.args_canonical,
                    resource_id_canonical=effect.resource_id_canonical,
                    result=effect.result,
                    error=effect.error,
                    dedup_count=effect.dedup_count,
                    created_at=effect.created_at,
                    updated_at=now,
                    completed_at=effect.completed_at,
                )
                self._cache[idem_key] = updated
                return True
        except EffectStoreError:
            raise
        except Exception as err:
            raise EffectStoreError(
                f"Failed to claim effect for processing: {err}",
                operation="claim_for_processing",
                effect_id=effect_id,
                details={"from_status": from_status.value},
            ) from err

    def clear(self) -> None:
        self._cache.clear()
        self._id_to_idem_key.clear()

    async def list_effects(self) -> list[Effect]:
        async with self._lock:
            return list(self._cache.values())

    @property
    def size(self) -> int:
        return len(self._cache)
