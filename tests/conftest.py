from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import pytest

from agent_ledger import EffectLedger, EffectLedgerOptions, EffectStatus, MemoryStore

if TYPE_CHECKING:
    from agent_ledger.types import Effect

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:password@0.0.0.0:5432/effect_ledger_test",
)


@runtime_checkable
class TestableStore(Protocol):
    async def find_by_idem_key(
        self, idem_key: str, tx: Any = None
    ) -> Effect | None: ...
    async def find_by_id(self, effect_id: str, tx: Any = None) -> Effect | None: ...
    async def transition(
        self,
        effect_id: str,
        from_status: EffectStatus,
        to_status: EffectStatus,
        result: Any = None,
        error: dict[str, str | None] | None = None,
        tx: Any = None,
    ) -> bool: ...


async def _get_first_effect_idem_key(store: Any) -> str | None:
    if hasattr(store, "list_effects"):
        effects = await store.list_effects()
        return effects[0].idem_key if effects else None
    return None


@pytest.fixture
def memory_store() -> MemoryStore:
    return MemoryStore()


@pytest.fixture
async def postgres_store() -> AsyncIterator[Any]:
    try:
        from psycopg_pool import AsyncConnectionPool

        from agent_ledger.stores.postgres import SCHEMA_SQL, PostgresStore
    except ImportError:
        pytest.skip("psycopg not installed")
        return

    pool = AsyncConnectionPool(conninfo=DATABASE_URL, open=False)
    await pool.open()

    async with pool.connection() as conn:
        await conn.execute(SCHEMA_SQL)
        await conn.execute("TRUNCATE effects")

    store = PostgresStore(pool=pool)

    yield store

    await pool.close()


@pytest.fixture(params=["memory", "postgres"])
async def store(
    request: pytest.FixtureRequest, memory_store: MemoryStore
) -> AsyncIterator[Any]:
    if request.param == "memory":
        yield memory_store
    else:
        try:
            from psycopg_pool import AsyncConnectionPool

            from agent_ledger.stores.postgres import SCHEMA_SQL, PostgresStore
        except ImportError:
            pytest.skip("psycopg not installed")
            return

        pool = AsyncConnectionPool(conninfo=DATABASE_URL, open=False)
        await pool.open()

        async with pool.connection() as conn:
            await conn.execute(SCHEMA_SQL)
            await conn.execute("TRUNCATE effects")

        pg_store = PostgresStore(pool=pool)

        yield pg_store

        await pool.close()


@pytest.fixture
def ledger(store: Any) -> EffectLedger[Any]:
    return EffectLedger(EffectLedgerOptions(store=store))
