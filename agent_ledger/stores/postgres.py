from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from psycopg.rows import dict_row

if TYPE_CHECKING:
    from psycopg import AsyncConnection
    from psycopg.rows import DictRow
    from psycopg_pool import AsyncConnectionPool

from agent_ledger.errors import EffectLedgerInvariantError, EffectStoreError
from agent_ledger.types import (
    Effect,
    EffectError,
    EffectStatus,
    UpsertEffectResult,
    is_terminal_status,
    is_valid_transition,
)
from agent_ledger.utils import generate_id

TABLE_NAME = "effects"

IMMUTABLE_STATUSES = (
    "('succeeded', 'failed', 'canceled', 'denied', 'requires_approval', 'ready')"
)
TERMINAL_STATUSES = "('succeeded', 'failed', 'canceled', 'denied')"


class PostgresStore:
    def __init__(
        self,
        pool: AsyncConnectionPool[AsyncConnection[DictRow]],
    ) -> None:
        self._pool = pool

    @asynccontextmanager
    async def _get_conn(
        self, tx: AsyncConnection[DictRow] | None
    ) -> AsyncIterator[AsyncConnection[DictRow]]:
        if tx is not None:
            yield tx
        else:
            async with self._pool.connection() as conn:
                yield conn

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncConnection[DictRow]]:
        try:
            async with self._pool.connection() as conn, conn.transaction():
                yield conn
        except Exception as err:
            raise EffectStoreError(
                f"Transaction failed: {err}",
                operation="transaction",
            ) from err

    async def find_by_idem_key(
        self,
        idem_key: str,
        tx: AsyncConnection[DictRow] | None = None,
    ) -> Effect | None:
        query = f"SELECT * FROM {TABLE_NAME} WHERE idem_key = %s LIMIT 1"

        try:
            async with (
                self._get_conn(tx) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(query, (idem_key,))
                row = await cur.fetchone()

                if row is None:
                    return None
                return self._row_to_effect(dict(row))
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
        tx: AsyncConnection[DictRow] | None = None,
    ) -> Effect | None:
        query = f"SELECT * FROM {TABLE_NAME} WHERE id = %s LIMIT 1"

        try:
            async with (
                self._get_conn(tx) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(query, (effect_id,))
                row = await cur.fetchone()

                if row is None:
                    return None

                return self._row_to_effect(dict(row))
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
        input_data: Any,
        tx: AsyncConnection[DictRow] | None = None,
    ) -> UpsertEffectResult:
        effect_id = generate_id()
        now = datetime.now(tz=timezone.utc)

        query = f"""
            INSERT INTO {TABLE_NAME}
            (id, idem_key, workflow_id, call_id, tool, status, args_canonical,
             resource_id_canonical, result, error, dedup_count, created_at, updated_at, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s)
            ON CONFLICT (idem_key) DO UPDATE SET
              status = CASE
                WHEN {TABLE_NAME}.status IN {IMMUTABLE_STATUSES}
                THEN {TABLE_NAME}.status
                ELSE EXCLUDED.status
              END,
              result = CASE
                WHEN {TABLE_NAME}.status IN {IMMUTABLE_STATUSES}
                THEN {TABLE_NAME}.result
                ELSE EXCLUDED.result
              END,
              error = CASE
                WHEN {TABLE_NAME}.status IN {IMMUTABLE_STATUSES}
                THEN {TABLE_NAME}.error
                ELSE EXCLUDED.error
              END,
              updated_at = CASE
                WHEN {TABLE_NAME}.status IN {IMMUTABLE_STATUSES}
                THEN {TABLE_NAME}.updated_at
                ELSE EXCLUDED.updated_at
              END,
              completed_at = CASE
                WHEN {TABLE_NAME}.status IN {TERMINAL_STATUSES}
                THEN {TABLE_NAME}.completed_at
                WHEN EXCLUDED.status IN {TERMINAL_STATUSES}
                THEN EXCLUDED.updated_at
                ELSE {TABLE_NAME}.completed_at
              END
            RETURNING *, (xmax = 0) AS created
        """

        try:
            async with (
                self._get_conn(tx) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(
                    query,
                    (
                        effect_id,
                        input_data.idem_key,
                        input_data.workflow_id,
                        input_data.call_id,
                        input_data.tool,
                        input_data.status.value,
                        input_data.args_canonical,
                        input_data.resource_id_canonical,
                        json.dumps(input_data.result)
                        if input_data.result is not None
                        else None,
                        json.dumps(
                            {
                                "message": input_data.error.message,
                                "code": input_data.error.code,
                            }
                        )
                        if input_data.error
                        else None,
                        now,
                        now,
                        now if is_terminal_status(input_data.status) else None,
                    ),
                )

                row = await cur.fetchone()
                if row is None:
                    raise EffectLedgerInvariantError(
                        "Upsert RETURNING clause returned no rows"
                    )

                row_dict = dict(row)
                created = row_dict.pop("created", False)
                created = created is True or created == "t"

                return UpsertEffectResult(
                    effect=self._row_to_effect(row_dict),
                    created=created,
                )
        except (EffectStoreError, EffectLedgerInvariantError):
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
        result: Any = None,
        error: dict[str, str | None] | None = None,
        tx: AsyncConnection[DictRow] | None = None,
    ) -> bool:
        if not is_valid_transition(from_status, to_status):
            return False

        now = datetime.now(tz=timezone.utc)

        query = f"""
            UPDATE {TABLE_NAME}
            SET status = %s, result = %s, error = %s, updated_at = %s,
                completed_at = CASE WHEN %s THEN %s ELSE completed_at END
            WHERE id = %s
              AND status = %s
              AND status NOT IN {TERMINAL_STATUSES}
        """

        try:
            async with (
                self._get_conn(tx) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(
                    query,
                    (
                        to_status.value,
                        json.dumps(result) if result is not None else None,
                        json.dumps(error) if error else None,
                        now,
                        is_terminal_status(to_status),
                        now,
                        effect_id,
                        from_status.value,
                    ),
                )

                return bool(cur.rowcount == 1)
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
        tx: AsyncConnection[DictRow] | None = None,
    ) -> None:
        query = f"""
            UPDATE {TABLE_NAME}
            SET dedup_count = dedup_count + 1, updated_at = %s
            WHERE idem_key = %s
        """

        try:
            async with (
                self._get_conn(tx) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(query, (datetime.now(tz=timezone.utc), idem_key))
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
        tx: AsyncConnection[DictRow] | None = None,
    ) -> bool:
        now = datetime.now(tz=timezone.utc)

        try:
            async with self._get_conn(tx) as conn:
                if (
                    from_status == EffectStatus.PROCESSING
                    and stale_threshold_ms is not None
                ):
                    cutoff = now - timedelta(milliseconds=stale_threshold_ms)

                    query = f"""
                        UPDATE {TABLE_NAME}
                        SET status = 'processing', updated_at = %s
                        WHERE id = %s AND status = 'processing'
                          AND updated_at < %s
                    """

                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(query, (now, effect_id, cutoff))
                        return bool(cur.rowcount == 1)

                else:
                    query = f"""
                        UPDATE {TABLE_NAME}
                        SET status = 'processing', updated_at = %s
                        WHERE id = %s AND status = %s
                          AND status NOT IN {TERMINAL_STATUSES}
                    """

                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(query, (now, effect_id, from_status.value))
                        return bool(cur.rowcount == 1)
        except EffectStoreError:
            raise
        except Exception as err:
            raise EffectStoreError(
                f"Failed to claim effect for processing: {err}",
                operation="claim_for_processing",
                effect_id=effect_id,
                details={"from_status": from_status.value},
            ) from err

    async def list_effects(
        self,
        tx: AsyncConnection[DictRow] | None = None,
    ) -> list[Effect]:
        query = f"SELECT * FROM {TABLE_NAME} ORDER BY created_at DESC"

        try:
            async with (
                self._get_conn(tx) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(query)
                rows = await cur.fetchall()
                return [self._row_to_effect(dict(row)) for row in rows]
        except EffectStoreError:
            raise
        except Exception as err:
            raise EffectStoreError(
                f"Failed to list effects: {err}",
                operation="list_effects",
            ) from err

    async def count(
        self,
        tx: AsyncConnection[DictRow] | None = None,
    ) -> int:
        query = f"SELECT COUNT(*) as cnt FROM {TABLE_NAME}"

        try:
            async with (
                self._get_conn(tx) as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await cur.execute(query)
                row = await cur.fetchone()
                return int(row["cnt"]) if row else 0
        except EffectStoreError:
            raise
        except Exception as err:
            raise EffectStoreError(
                f"Failed to count effects: {err}",
                operation="count",
            ) from err

    def _row_to_effect(self, row: dict[str, Any]) -> Effect:
        error_data = row.get("error")
        result_data = row.get("result")

        return Effect(
            id=row["id"],
            idem_key=row["idem_key"],
            workflow_id=row["workflow_id"],
            call_id=row["call_id"],
            tool=row["tool"],
            status=EffectStatus(row["status"]),
            args_canonical=row["args_canonical"],
            resource_id_canonical=row["resource_id_canonical"],
            result=result_data,
            error=EffectError(
                message=error_data["message"],
                code=error_data.get("code"),
            )
            if error_data
            else None,
            dedup_count=int(row["dedup_count"]),
            created_at=row["created_at"]
            if isinstance(row["created_at"], datetime)
            else datetime.fromisoformat(str(row["created_at"])),
            updated_at=row["updated_at"]
            if isinstance(row["updated_at"], datetime)
            else datetime.fromisoformat(str(row["updated_at"])),
            completed_at=row["completed_at"]
            if isinstance(row.get("completed_at"), datetime)
            else (
                datetime.fromisoformat(str(row["completed_at"]))
                if row.get("completed_at")
                else None
            ),
        )


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS effects (
    id UUID PRIMARY KEY,
    idem_key TEXT NOT NULL UNIQUE,
    workflow_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    tool TEXT NOT NULL,
    status TEXT NOT NULL,
    args_canonical TEXT NOT NULL,
    resource_id_canonical TEXT NOT NULL,
    result JSONB,
    error JSONB,
    dedup_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_effects_idem_key ON effects (idem_key);
CREATE INDEX IF NOT EXISTS idx_effects_workflow_id ON effects (workflow_id);
CREATE INDEX IF NOT EXISTS idx_effects_status ON effects (status) WHERE status NOT IN ('succeeded', 'failed', 'canceled', 'denied');
"""
