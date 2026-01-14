# agent-ledger

Idempotency and audit ledger for AI agent tool calls.

[![PyPI version](https://badge.fury.io/py/agent-ledger.svg)](https://badge.fury.io/py/agent-ledger)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Installation

```bash
pip install agent-ledger

# With PostgreSQL support
pip install agent-ledger[postgres]
```

## Quick Start

```python
import asyncio
from agent_ledger import EffectLedger, EffectLedgerOptions, MemoryStore, ToolCall

async def main():
    store = MemoryStore()
    ledger = EffectLedger(EffectLedgerOptions(store=store))

    # Idempotent tool execution
    result = await ledger.run(
        ToolCall(
            workflow_id="support-ticket-123",
            tool="slack.send_message",
            args={"channel": "#support", "text": "Ticket resolved"},
        ),
        handler=lambda effect: send_slack_message("#support", "Ticket resolved"),
    )

    # If replayed: returns cached result, no duplicate message sent
    result2 = await ledger.run(
        ToolCall(
            workflow_id="support-ticket-123",
            tool="slack.send_message",
            args={"channel": "#support", "text": "Ticket resolved"},
        ),
        handler=lambda effect: send_slack_message("#support", "Ticket resolved"),
    )

    assert result == result2  # Same cached result

asyncio.run(main())
```

## Features

- **Idempotency**: Automatic deduplication of tool calls based on workflow ID, tool name, and arguments
- **Audit Trail**: Every tool execution is recorded with full context
- **Human-in-the-Loop**: Built-in approval workflow for sensitive operations
- **Concurrent Safety**: Handles race conditions when multiple workers execute the same effect
- **Stale Detection**: Automatic takeover of stalled effects
- **Pluggable Storage**: MemoryStore for development, PostgresStore for production

## Core Concepts

### Effect

An Effect represents a single execution of a tool. It tracks:

- Input arguments (canonicalized)
- Status (processing, succeeded, failed, requires_approval, etc.)
- Result or error
- Timing information
- Deduplication count

### Idempotency Key

Computed as `SHA256(workflow_id | tool | canonical_args_or_resource)`. Same inputs = same key = same effect.

### Status Flow

```
processing -> succeeded
           -> failed
           -> requires_approval -> ready -> succeeded/failed
                                -> denied
           -> canceled
```

## Usage Examples

### Basic Idempotent Execution

```python
result = await ledger.run(
    ToolCall(
        workflow_id="order-123",
        tool="stripe.charge",
        args={"amount": 1000, "currency": "usd"},
    ),
    handler=async_stripe_charge,
)
```

### With Resource Descriptor

```python
result = await ledger.run(
    ToolCall(
        workflow_id="order-123",
        tool="github.create_issue",
        args={"title": "Bug report", "body": "Details..."},
        resource=ResourceDescriptor(
            namespace="github",
            type="issue",
            id={"owner": "acme", "repo": "app"},
        ),
    ),
    handler=create_github_issue,
)
```

### With Approval Flow

```python
from agent_ledger import RunOptions

result = await ledger.run(
    ToolCall(
        workflow_id="deploy-prod",
        tool="k8s.deploy",
        args={"image": "app:v2"},
    ),
    handler=deploy_to_k8s,
    run_options=RunOptions(requires_approval=True),
)

# Elsewhere (admin panel, CLI, webhook):
await ledger.approve(idem_key)
# or
await ledger.deny(idem_key, reason="Not authorized")
```

### Manual Begin/Commit

```python
# Fine-grained control
begin_result = await ledger.begin(call)

if begin_result.cached:
    return begin_result.cached_result

try:
    result = await execute_tool(call.args)
    await ledger.commit(begin_result.effect.id, CommitSucceeded(result=result))
    return result
except Exception as e:
    await ledger.commit(
        begin_result.effect.id,
        CommitFailed(error=EffectError(message=str(e))),
    )
    raise
```

## PostgreSQL Setup

```python
from psycopg_pool import AsyncConnectionPool
from agent_ledger.stores.postgres import PostgresStore, SCHEMA_SQL

async def setup():
    pool = AsyncConnectionPool(conninfo="postgresql://localhost/mydb")

    # Create schema
    async with pool.connection() as conn:
        await conn.execute(SCHEMA_SQL)

    store = PostgresStore(pool=pool)
    ledger = EffectLedger(EffectLedgerOptions(store=store))
    return ledger
```

## API Reference

### EffectLedger

| Method                             | Description                           |
| ---------------------------------- | ------------------------------------- |
| `run(call, handler, run_options?)` | Execute tool with idempotency         |
| `begin(call)`                      | Start effect, returns cached or fresh |
| `commit(effect_id, outcome)`       | Complete effect with result/error     |
| `get_effect(effect_id)`            | Get effect by ID                      |
| `find_by_idem_key(idem_key)`       | Get effect by idempotency key         |
| `request_approval(idem_key)`       | Move to requires_approval             |
| `approve(idem_key)`                | Approve and move to ready             |
| `deny(idem_key, reason?)`          | Deny with optional reason             |

### EffectStatus

| Status              | Description                       |
| ------------------- | --------------------------------- |
| `processing`        | Currently executing               |
| `succeeded`         | Completed successfully (terminal) |
| `failed`            | Completed with error (terminal)   |
| `requires_approval` | Waiting for human approval        |
| `ready`             | Approved, ready to execute        |
| `denied`            | Human rejected (terminal)         |
| `canceled`          | System canceled (terminal)        |

## Configuration

### ConcurrencyOptions

```python
ConcurrencyOptions(
    wait_timeout_ms=30_000,      # Max wait for concurrent effect
    initial_interval_ms=50,      # Initial polling interval
    max_interval_ms=1_000,       # Max polling interval
    backoff_multiplier=1.5,      # Exponential backoff
    jitter_factor=0.3,           # Randomization factor
)
```

### StaleOptions

```python
StaleOptions(
    after_ms=60_000,  # Consider stale after 60s (0 = disabled)
)
```

## License

Apache-2.0
