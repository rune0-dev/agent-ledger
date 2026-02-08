# Vanilla OpenAI Function Calling + agent-ledger

The simplest integration - no framework, just OpenAI SDK with idempotent tools.

Note: tool side effects in this example are mocked (`print` + fake IDs). Replace them with your real API calls.

## Setup

```bash
cd examples/openai-functions
pip install -r requirements.txt
export OPENAI_API_KEY=sk-...
```

## Run

```bash
python agent.py
```

## Key Pattern

Wrap tool execution with `ledger.run()`:

```python
async def execute_tool(name: str, args: dict) -> str:
    result = await ledger.run(
        ToolCall(workflow_id=WORKFLOW_ID, tool=name, args=args),
        handler=lambda _: tool_functions[name](**args),
    )
    return json.dumps(result)
```

This deduplicates repeated calls with the same `(workflow_id, tool, args)` and replays the recorded result.
