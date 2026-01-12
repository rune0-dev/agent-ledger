from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from cachetools import TTLCache

from effect_ledger.types import (
    Effect,
    EffectError,
    EffectStatus,
    UpsertEffectResult,
    is_terminal_status,
)
from effect_ledger.utils import generate_id

if TYPE_CHECKING:
    from effect_ledger.types import JsonValue, UpsertEffectInput

DEFAULT_MAX_SIZE = 10_000
DEFAULT_TTL_SECONDS = 60 * 60
STALE_THRESHOLD_MS = 60_000


class MemoryStore:
    def __init__(
        self,
        max_size: int = DEFAULT_MAX_SIZE,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
    ) -> None:
        self._cache: TTLCache[str, Effect] = TTLCache(
            maxsize=max_size,
            ttl=ttl_seconds,
        )
        self._idem_key_to_id: dict[str, str] = {}
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        yield None

    async def find_by_idem_key(
        self,
        idem_key: str,
        tx: None = None,
    ) -> Effect | None:
        effect_id = self._idem_key_to_id.get(idem_key)
        if effect_id is None:
            return None
        result: Effect | None = self._cache.get(effect_id)
        return result

    async def find_by_id(
        self,
        effect_id: str,
        tx: None = None,
    ) -> Effect | None:
        result: Effect | None = self._cache.get(effect_id)
        return result

    async def upsert(
        self,
        input_data: UpsertEffectInput,
        tx: None = None,
    ) -> UpsertEffectResult:
        async with self._lock:
            existing_id = self._idem_key_to_id.get(input_data.idem_key)
            existing = self._cache.get(existing_id) if existing_id else None

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

            self._cache[effect_id] = effect
            self._idem_key_to_id[input_data.idem_key] = effect_id

            return UpsertEffectResult(effect=effect, created=True)

    async def transition(
        self,
        effect_id: str,
        from_status: EffectStatus,
        to_status: EffectStatus,
        result: JsonValue = None,
        error: dict[str, str | None] | None = None,
        tx: None = None,
    ) -> bool:
        async with self._lock:
            effect = self._cache.get(effect_id)

            if effect is None:
                return False

            if effect.status != from_status:
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

            self._cache[effect_id] = updated
            return True

    async def increment_dedup_count(
        self,
        idem_key: str,
        tx: None = None,
    ) -> None:
        async with self._lock:
            effect_id = self._idem_key_to_id.get(idem_key)
            if effect_id is None:
                return

            effect = self._cache.get(effect_id)
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
            self._cache[effect_id] = updated

    async def claim_for_processing(
        self,
        effect_id: str,
        from_status: EffectStatus,
        stale_threshold_ms: int | None = None,
        tx: None = None,
    ) -> bool:
        async with self._lock:
            effect = self._cache.get(effect_id)
            if effect is None:
                return False

            now = datetime.now(tz=timezone.utc)

            if (
                from_status == EffectStatus.PROCESSING
                and stale_threshold_ms is not None
            ):
                # Stale claim: check status AND age
                if effect.status != EffectStatus.PROCESSING:
                    return False
                age_ms = (now - effect.updated_at).total_seconds() * 1000
                if age_ms <= stale_threshold_ms:
                    return False
            else:
                # READY claim: just check status
                if effect.status != from_status:
                    return False

            # Claim it by updating to PROCESSING with fresh timestamp
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
            self._cache[effect_id] = updated
            return True

    def clear(self) -> None:
        self._cache.clear()
        self._idem_key_to_id.clear()

    def list_effects(self) -> list[Effect]:
        return list(self._cache.values())

    @property
    def size(self) -> int:
        return len(self._cache)
