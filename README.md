<div align="center">

[![Pixel fonts](https://see.fontimg.com/api/rf5/BLAPB/YWMxYTMyY2I3MjZmNDUzN2JiZTFiODBiM2E4NjhiNGMudHRm/cnVuZTA/bitrimus.png?r=fs&h=77&w=1000&fg=000000&bg=FFFFFF&tb=1&s=77)](https://www.fontspace.com/category/pixel)

<h3>
AI agents retry. Tool calls shouldn't.
</h3>

</div>

---

<div align="center">

Idempotent tool execution for AI agents (Postgres for crash recovery + multi-worker).<br/>
Human sign-off locked to the exact tool call + args (no "approve X, run Y").

*Idempotent tool calls. First-class human approval gates. Queriable recorded execution history.*

**Works with any async Python tool executor.** Examples: [LangGraph](examples/) / [LangChain](examples/). (more coming)

[![PyPI version](https://badge.fury.io/py/agent-ledger.svg)](https://badge.fury.io/py/agent-ledger)
[![CI](https://github.com/rune0/agent-ledger-py/actions/workflows/ci.yml/badge.svg)](https://github.com/rune0/agent-ledger-py/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

</div>

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

async def charge_customer(effect):
    print(f"Charging {effect.args_canonical}...")
    return {"status": "charged", "id": "ch_123"}

async def main():
    store = MemoryStore()
    ledger = EffectLedger(EffectLedgerOptions(store=store))

    # First call: executes the handler
    result = await ledger.run(
        ToolCall(
            workflow_id="order-123",
            tool="stripe.charge",
            args={"amount": 1000, "currency": "usd"},
        ),
        handler=charge_customer,
    )
    print(f"First call: {result}")

    # Second call: same inputs → returns recorded result
    result2 = await ledger.run(
        ToolCall(
            workflow_id="order-123",
            tool="stripe.charge",
            args={"amount": 1000, "currency": "usd"},
        ),
        handler=charge_customer,
    )
    print(f"Second call: {result2}")
    # Handler only executed once. No double charge.

asyncio.run(main())
```

**Output:**

```
Charging {"amount":1000,"currency":"usd"}...
First call: {'status': 'charged', 'id': 'ch_123'}
Second call: {'status': 'charged', 'id': 'ch_123'}
```

Same inputs → same hash → same result. The handler only runs once.

---

## Why This Exists

LLMs are non-deterministic. Your payment API isn't.

When an agent crashes or retries, it doesn't remember what it already did. A timeout becomes a double charge. A retry loop becomes inbox spam. A crashed deploy leaves you guessing what actually ran.

`agent-ledger` sits between your agent and the outside world:

| Problem | How agent-ledger helps |
|---------|----------------------|
| Agent retried → customer charged twice | **Idempotency**: Same inputs → same hash → recorded result replayed, handler skipped |
| Agent deployed without permission | **Approvals**: Sensitive tools pause until a human approves |
| "What did the agent actually do?" | **Audit trail**: Every call recorded with inputs, outputs, timing |
| Process crashed mid-execution | **Crash recovery**: New process reads ledger, resumes safely |
| Two workers hit the same task | **Concurrency**: First writer wins, others wait for result |

---

## The "Pause Button" for Your Agent (Human-in-the-loop)

High-stakes operations can require human approval before execution (example of a Slack message as approval request integration):

```python
from agent_ledger import RunOptions, LedgerHooks
import json

# Agent side: request approval and wait
async def notify_slack_approval_hook(effect):
    """Hook fires once when approval is required."""
    await slack.post_message(
        channel="#deployments",
        text=f"Approval needed: {effect.tool}",
        blocks=[...],
    )


result = await ledger.run(
    ToolCall(
        workflow_id="deploy-prod",
        tool="k8s.deploy",
        args={"image": "app:v2"},
    ),
    handler=deploy_to_k8s,
    run_options=RunOptions(requires_approval=True),
    hooks=LedgerHooks(on_approval_required=notify_slack_approval_hook),
)

# Slack bot side: handle button click
@slack_app.action("approve")
async def handle_approve(ack, body):
    await ack()
    idem_key = body["actions"][0]["value"]  # From button payload

    await ledger.approve(idem_key)
    await slack.post_message(channel="#deployments", text=f"✅ Approved: {idem_key}")

@slack_app.action("deny")
async def handle_deny(ack, body):
    await ack()
    idem_key = body["actions"][0]["value"]

    await ledger.deny(idem_key, reason="Denied by operator")
    await slack.post_message(channel="#deployments", text=f"❌ Denied: {idem_key}")
```

The agent waits. The human decides. The ledger records everything.

`run()` polls until the effect is approved, with exponential backoff (default: 30s timeout, 50ms initial interval, 1.5x backoff multiplier). Configurable via `RunOptions.concurrency` (see [Configuration](#configuration)). After approval, the handler executes and the result is returned.

The `on_approval_required` hook fires once when the approval request is created—not on retries or replays. Hook errors are logged but don't abort the run.

**Key flow**: The hook receives `effect.idem_key`—this is the approval handle. External systems (Slack, admin panels, CLIs) store this key in button payloads/URLs and pass it to `approve(idem_key)` or `deny(idem_key, reason)`.

> **Intent-bound approval**: The approval is tied to the exact payload hash. If the agent retries with different arguments, that's a *new* approval request—not a bypass of the previous one.

---

## With LangChain

```python
from agent_ledger import EffectLedger, EffectLedgerOptions, MemoryStore, ToolCall

ledger = EffectLedger(EffectLedgerOptions(store=MemoryStore()))

async def send_email(to: str, subject: str, body: str) -> str:
    # Your actual email-sending logic
    return f"Email sent to {to}"

async def execute_tool_safely(tool_name: str, args: dict, workflow_id: str):
    """Wrap any async function with idempotency."""
    async def handler(_):
        return await send_email(**args)

    return await ledger.run(
        ToolCall(workflow_id=workflow_id, tool=tool_name, args=args),
        handler=handler,
    )

# In your agent's tool execution loop:
result = await execute_tool_safely(
    tool_name="send_email",
    args={"to": "customer@example.com", "subject": "Order confirmed", "body": "..."},
    workflow_id="order-456",
)
# Retries won't send duplicate emails
```

This pattern works with LangChain, LangGraph, CrewAI, or any other framework. See [`examples/`](examples/) for framework-specific integrations.

---

## How It Works

Every tool call becomes a transaction in the ledger:

```
ToolCall(workflow_id, tool, args)
              │
              ▼
    SHA256(workflow_id | tool | args) → idem_key
              │
              ▼
         ┌─────────┐
         │ LEDGER  │
         └────┬────┘
              │
   ┌──────────┼──────────┐
   │          │          │
 fresh    in-flight   terminal
   │          │          │
   ▼          ▼          ▼
execute     wait       replay
handler   for result   recorded
```

**Effect lifecycle:**

```
processing → succeeded
           → failed
           → requires_approval → ready → succeeded/failed
                               → denied
           → canceled
```

### Design Constraints

- **At-most-once commit per idem_key**: Each unique `(workflow_id, tool, args)` tuple is recorded at most once, enforced by the store's unique constraint on `idem_key` and atomic upsert semantics
- **Downstream exactly-once**: Depends on your handler being idempotent or the downstream API supporting idempotency keys
- **Deterministic canonicalization**: Args are JSON-serialized with sorted keys; non-deterministic values (timestamps, UUIDs) in args will create new records
- **Multi-tenant isolation**: Include tenant/user/principal in `workflow_id` (or in `args`) to prevent cross-actor deduplication. The library does not enforce tenant boundaries—your application must scope `workflow_id` appropriately

---

## More Examples

### Custom Idempotency Keys

Hash only specific fields—ignore the rest:

```python
await ledger.run(
    ToolCall(
        workflow_id="ticket-456",
        tool="github.create_issue",
        args={"owner": "acme", "repo": "app", "title": "Bug", "body": "Details..."},
        idempotency_keys=["owner", "repo", "title"],  # body changes won't re-execute
    ),
    handler=create_issue,
)
```

### PostgreSQL for Production

Your agent's state belongs in Postgres, not ephemeral memory:

```python
from psycopg_pool import AsyncConnectionPool
from agent_ledger.stores.postgres import PostgresStore, SCHEMA_SQL

pool = AsyncConnectionPool(conninfo="postgresql://localhost/mydb")
async with pool.connection() as conn:
    await conn.execute(SCHEMA_SQL)

store = PostgresStore(pool=pool)
ledger = EffectLedger(EffectLedgerOptions(store=store))
# Query your audit trail with SQL
```

### Fine-Grained Control

For custom execution logic:

```python
from agent_ledger import CommitSucceeded, CommitFailed, EffectError

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

---

## When to Use This

**Good fit:**
- Agents calling payment APIs, sending emails, creating tickets
- Workflows requiring human-in-the-loop oversight
- Workflows that retry on failure or resume after crashes
- Operations requiring human sign-off before execution
- Systems needing audit trails of what the agent did

**Probably not needed:**
- Read-only agents (RAG, summarization, search)
- One-off scripts without retry logic
- Prototypes where duplicates are acceptable

---

## Why Not...?

| Alternative | What's missing |
|-------------|----------------|
| **Retry libraries** (Tenacity, Stamina) | Retry the call, but don't deduplicate across processes or restarts |
| **In-memory cache** | Lost on restart, can't coordinate multiple workers |
| **DB unique constraints** | Good start, but no lifecycle states, result caching, or approval flows |
| **Workflow engines** (Temporal, Celery) | Full orchestration systems; `agent-ledger` is a lightweight layer you can use *inside* them |

**Library-only**: no sidecar, no agent runtime, no SaaS. Bring your own store (Memory/Postgres). `pip install` and go.

---

## FAQ

**Does this replace Temporal?**
No. Temporal is a full workflow orchestration engine. `agent-ledger` is a lightweight idempotency layer you can use inside Temporal activities, or standalone.

**Can it prevent double Stripe charges?**
Yes—by replaying the recorded result instead of re-executing the handler. For extra safety, also pass Stripe's own `idempotency_key` in your API call.

**What is `workflow_id`?**
A scope boundary for idempotency. Same `(workflow_id, tool, args)` = same effect. Different workflow_id = independent effects, even with identical tool+args. You can use this, for example, if an agent is invoked via a webhook to deduplicate all side effects across multiple retries by passing the webhook's `id` as the `workflow_id`.

**Important**: In multi-tenant systems, include the tenant/user identifier in `workflow_id` (e.g., `"tenant-123:order-456"`) to prevent unintended deduplication across different actors or security boundaries.

---

## Core API

| Method | Purpose |
|--------|---------|
| `run(call, handler)` | Execute with idempotency—the main entry point |
| `begin(call)` / `commit(id, outcome)` | Manual transaction control |
| `approve(key)` / `deny(key)` | Human-in-the-loop approval |
| `get_effect(id)` / `find_by_idem_key(key)` | Query ledger state |

See [ledger.py](agent_ledger/ledger.py) for full API with type signatures.

### Configuration

Control polling, timeouts, and stale effect handling:

```python
from agent_ledger import RunOptions, ConcurrencyOptions, StaleOptions

# Per-call configuration
await ledger.run(
    call,
    handler=my_handler,
    run_options=RunOptions(
        requires_approval=False,
        concurrency=ConcurrencyOptions(
            wait_timeout_ms=30_000,      # Max wait time for in-flight effects (default: 30s)
            initial_interval_ms=50,      # First poll interval (default: 50ms)
            max_interval_ms=1_000,       # Cap for backoff interval (default: 1s)
            backoff_multiplier=1.5,      # Exponential backoff rate (default: 1.5x)
            jitter_factor=0.3,           # Random jitter to avoid thundering herd (default: 0.3)
        ),
        stale=StaleOptions(
            after_ms=60_000,             # Take over stale PROCESSING effects (default: 0 = disabled)
        ),
    ),
)

# Global defaults (applied to all run() calls unless overridden)
from agent_ledger import EffectLedger, EffectLedgerOptions, LedgerDefaults

ledger = EffectLedger(
    EffectLedgerOptions(
        store=store,
        defaults=LedgerDefaults(
            run=RunOptions(
                requires_approval=True,  # All calls need approval by default
                concurrency=ConcurrencyOptions(wait_timeout_ms=60_000),  # 60s timeout
            ),
        ),
    ),
)
```

**Polling behavior**: When waiting for an in-flight or approval-required effect, `run()` polls the store with exponential backoff. Initial interval is 50ms, increasing by 1.5x each retry (up to 1s max), with 30% jitter. Times out after 30s by default.

---

## License

Apache-2.0

---

<sub>**Disclaimer:** This library is designed to help reduce duplicate executions through idempotency patterns. It does not guarantee exactly-once semantics in all failure scenarios—correct behavior depends on proper integration, idempotent handlers, and appropriate storage configuration. The authors are not liable for any damages arising from the use of this software. Always test thoroughly before deploying to production. See [LICENSE](LICENSE) for full terms.</sub>
