from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Literal

JsonValue = str | int | float | bool | None | list[Any] | dict[str, Any]


class EffectStatus(str, Enum):
    REQUIRES_APPROVAL = "requires_approval"
    READY = "ready"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"
    DENIED = "denied"


TERMINAL_STATUSES: frozenset[EffectStatus] = frozenset(
    {
        EffectStatus.SUCCEEDED,
        EffectStatus.FAILED,
        EffectStatus.CANCELED,
        EffectStatus.DENIED,
    }
)

AWAITING_STATUSES: frozenset[EffectStatus] = frozenset(
    {
        EffectStatus.PROCESSING,
        EffectStatus.REQUIRES_APPROVAL,
    }
)

ALLOWED_TRANSITIONS: dict[EffectStatus, frozenset[EffectStatus]] = {
    EffectStatus.PROCESSING: frozenset(
        {
            EffectStatus.SUCCEEDED,
            EffectStatus.FAILED,
            EffectStatus.REQUIRES_APPROVAL,
        }
    ),
    EffectStatus.REQUIRES_APPROVAL: frozenset(
        {
            EffectStatus.READY,
            EffectStatus.DENIED,
            EffectStatus.CANCELED,
        }
    ),
    EffectStatus.READY: frozenset(
        {
            EffectStatus.PROCESSING,
        }
    ),
}


def is_terminal_status(status: EffectStatus) -> bool:
    return status in TERMINAL_STATUSES


def is_awaiting_status(status: EffectStatus) -> bool:
    return status in AWAITING_STATUSES


def is_valid_transition(from_status: EffectStatus, to_status: EffectStatus) -> bool:
    if from_status in TERMINAL_STATUSES:
        return False
    allowed = ALLOWED_TRANSITIONS.get(from_status)
    if allowed is None:
        return False
    return to_status in allowed


IdempotencyStatus = Literal["fresh", "replayed"]


@dataclass(frozen=True, slots=True)
class ResourceDescriptor:
    namespace: str
    type: str
    id: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ToolCall:
    workflow_id: str
    tool: str
    args: dict[str, Any]
    call_id: str | None = None
    resource: ResourceDescriptor | None = None
    idempotency_keys: list[str] | None = None


@dataclass(frozen=True, slots=True)
class EffectError:
    message: str
    code: str | None = None


@dataclass(slots=True)
class Effect:
    id: str
    idem_key: str
    workflow_id: str
    call_id: str
    tool: str
    status: EffectStatus
    args_canonical: str
    resource_id_canonical: str
    dedup_count: int
    created_at: datetime
    updated_at: datetime
    result: JsonValue = None
    error: EffectError | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class CommitSucceeded:
    status: Literal["succeeded"] = field(default="succeeded", init=False)
    result: JsonValue = None


@dataclass(frozen=True, slots=True)
class CommitFailed:
    error: EffectError
    status: Literal["failed"] = field(default="failed", init=False)


CommitOutcome = CommitSucceeded | CommitFailed


@dataclass(frozen=True, slots=True)
class BeginResult:
    effect: Effect
    cached: bool
    idempotency_status: IdempotencyStatus
    cached_result: JsonValue = None


@dataclass(frozen=True, slots=True)
class UpsertEffectInput:
    idem_key: str
    workflow_id: str
    call_id: str
    tool: str
    status: EffectStatus
    args_canonical: str
    resource_id_canonical: str
    result: JsonValue = None
    error: EffectError | None = None


@dataclass(frozen=True, slots=True)
class UpsertEffectResult:
    effect: Effect
    created: bool


@dataclass(frozen=True, slots=True)
class ConcurrencyOptions:
    wait_timeout_ms: int = 30_000
    initial_interval_ms: int = 50
    max_interval_ms: int = 1_000
    backoff_multiplier: float = 1.5
    jitter_factor: float = 0.3


@dataclass(frozen=True, slots=True)
class StaleOptions:
    after_ms: int = 0


@dataclass(frozen=True, slots=True)
class RunOptions:
    concurrency: ConcurrencyOptions | None = None
    stale: StaleOptions | None = None
    requires_approval: bool = False


@dataclass(frozen=True, slots=True)
class LedgerDefaults:
    run: RunOptions | None = None
