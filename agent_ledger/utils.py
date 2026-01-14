from __future__ import annotations

import hashlib
import uuid
from typing import TYPE_CHECKING, Any

import rfc8785

from agent_ledger.errors import EffectLedgerValidationError

if TYPE_CHECKING:
    from agent_ledger.types import ResourceDescriptor, ToolCall

DEFAULT_MAX_ARGS_SIZE_BYTES = 1024 * 1024  # 1MB


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonicalize(obj: Any) -> str:
    try:
        return rfc8785.dumps(obj).decode("utf-8")
    except (TypeError, ValueError) as e:
        raise EffectLedgerValidationError(
            f"Args must be JSON-serializable: {e}", field="args"
        ) from e


def validate_args(args: dict[str, Any], max_size_bytes: int | None = None) -> str:
    if not isinstance(args, dict):
        raise EffectLedgerValidationError(
            f"Args must be a dict, got {type(args).__name__}", field="args"
        )

    canonical = canonicalize(args)
    limit = max_size_bytes or DEFAULT_MAX_ARGS_SIZE_BYTES

    if len(canonical.encode("utf-8")) > limit:
        raise EffectLedgerValidationError(
            f"Args exceed maximum size of {limit} bytes", field="args"
        )

    return canonical


def resource_id_canonical(resource: ResourceDescriptor) -> str:
    """Canonicalize resource descriptor using RFC8785 for stable hashing."""
    # Use RFC8785 canonicalization for the entire id dict to ensure stability
    canonical_id = canonicalize(dict(sorted(resource.id.items())))
    return f"{resource.namespace}/{resource.type}/{canonical_id}"


def _pick(obj: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {k: obj[k] for k in keys if k in obj}


def compute_idem_key(call: ToolCall) -> str:
    parts: list[str] = [call.workflow_id, call.tool]

    if call.resource is not None:
        parts.append(resource_id_canonical(call.resource))
    elif call.idempotency_keys and len(call.idempotency_keys) > 0:
        selected = _pick(call.args, call.idempotency_keys)
        parts.append(canonicalize(selected))
    else:
        parts.append(canonicalize(call.args))

    return _sha256("|".join(parts).encode("utf-8"))


def generate_id() -> str:
    return str(uuid.uuid4())
