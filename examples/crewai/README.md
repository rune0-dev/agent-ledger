# CrewAI + agent-ledger

Idempotent tool execution for CrewAI agents.

Note: tool side effects in this example are mocked (`print` + fake IDs). Replace them with your real API calls.

## Setup

```bash
cd examples/crewai
pip install -r requirements.txt
export OPENAI_API_KEY=sk-...
```

## Run

```bash
python agent.py
```

## Key Pattern

Wrap tool execution with the `@idempotent` decorator:

```python
@tool("Charge Customer")
@idempotent("stripe.charge")
def charge_customer(amount_cents: int) -> str:
    # This only executes once per unique args
    return stripe.charge(amount_cents)
```

Repeated calls with the same `(workflow_id, tool, args)` are deduplicated and return the recorded result.
