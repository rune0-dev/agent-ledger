# LangChain + agent-ledger

Idempotent tool execution for LangChain agents.

Note: tool side effects in this example are mocked (`print` + fake IDs). Replace them with your real API calls.

## The Problem

When LangChain agents retry (due to timeouts, crashes, or LLM non-determinism), tools execute multiple times:

```
Agent decides to charge $50
→ Tool executes, charges $50
→ Network timeout, agent retries
→ Tool executes AGAIN
→ Customer charged $100 instead of $50
```

## The Solution

Wrap tool execution with `agent-ledger`:

```python
@tool
async def charge_customer(amount: int) -> dict:
    return await ledger.run(
        ToolCall(workflow_id="order-123", tool="stripe.charge", args={"amount": amount}),
        handler=lambda _: stripe.charge(amount),
    )
```

Same `(workflow_id, tool, args)` → cached result, no duplicate execution.

## Setup

```bash
cd examples/langchain
pip install -r requirements.txt
export OPENAI_API_KEY=sk-...
```

## Run

```bash
python agent.py
```

## How It Works

The example creates a LangGraph ReAct agent with three tools:
- `charge_customer` - Charge a credit card
- `send_email` - Send an email
- `create_ticket` - Create a support ticket

Each tool wraps its side effect with `ledger.run()`. The LLM decides which tools to call, and agent-ledger deduplicates repeated calls by replaying the recorded result.

## Production Notes

1. **Use PostgresStore** instead of MemoryStore for durability
2. **Set workflow_id** to something meaningful (order ID, session ID, webhook ID)
3. **Add approval hooks** for sensitive operations (see main README)
