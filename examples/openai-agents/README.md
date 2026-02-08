# OpenAI Agents SDK + agent-ledger

Idempotent tool execution for the official OpenAI Agents SDK.

Note: tool side effects in this example are mocked (`print` + fake IDs). Replace them with your real API calls.

## Setup

```bash
cd examples/openai-agents
pip install -r requirements.txt
export OPENAI_API_KEY=sk-...
```

## Run

```bash
python agent.py
```

## Key Pattern

Wrap tool execution inside `@function_tool` with `ledger.run()`:

```python
@function_tool
async def charge_customer(amount: Annotated[int, "Amount in cents"]) -> str:
    result = await ledger.run(
        ToolCall(workflow_id=WORKFLOW_ID, tool="stripe.charge", args={"amount": amount}),
        handler=lambda _: stripe.charge(amount),
    )
    return f"Charged. ID: {result['charge_id']}"
```

Repeated calls with the same `(workflow_id, tool, args)` are deduplicated and return the recorded result.
