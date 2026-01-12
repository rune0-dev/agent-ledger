from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from effect_ledger.types import (
        Effect,
        EffectStatus,
        JsonValue,
        UpsertEffectInput,
        UpsertEffectResult,
    )

__all__ = ["AsyncIterator", "EffectStore", "TxT", "asynccontextmanager"]


TxT = TypeVar("TxT")


@runtime_checkable
class EffectStore(Protocol[TxT]):
    def transaction(self) -> AsyncIterator[TxT]: ...

    async def find_by_idem_key(
        self,
        idem_key: str,
        tx: TxT | None = None,
    ) -> Effect | None: ...

    async def find_by_id(
        self,
        effect_id: str,
        tx: TxT | None = None,
    ) -> Effect | None: ...

    async def upsert(
        self,
        input_data: UpsertEffectInput,
        tx: TxT | None = None,
    ) -> UpsertEffectResult: ...

    async def transition(
        self,
        effect_id: str,
        from_status: EffectStatus,
        to_status: EffectStatus,
        result: JsonValue = None,
        error: dict[str, str | None] | None = None,
        tx: TxT | None = None,
    ) -> bool: ...

    async def increment_dedup_count(
        self,
        idem_key: str,
        tx: TxT | None = None,
    ) -> None: ...

    async def claim_for_processing(
        self,
        effect_id: str,
        from_status: EffectStatus,
        stale_threshold_ms: int | None = None,
        tx: TxT | None = None,
    ) -> bool:
        """Atomically claim an effect for processing.

        For READY: claims if status is READY.
        For stale PROCESSING: claims if status is PROCESSING AND updated_at is older than threshold.

        Returns True if claimed, False if someone else got it.
        """
        ...
