# Examples

Integration examples for `agent-ledger` with various agent frameworks.

These are integration-pattern examples: frameworks and agent loops are real, while side-effect implementations are mocked for safety.

Each example is self-contained with its own `requirements.txt`.

## Available Examples

| Example | Framework | Description |
|---------|-----------|-------------|
| [basic_usage.py](./basic_usage.py) | None | Core concepts without external dependencies |
| [openai-functions/](./openai-functions/) | OpenAI SDK | Vanilla function calling - no framework |
| [langchain/](./langchain/) | LangGraph | LangChain/LangGraph ReAct agent |
| [crewai/](./crewai/) | CrewAI | Multi-agent crew with tools |
| [openai-agents/](./openai-agents/) | OpenAI Agents SDK | Official OpenAI agent framework |

## Running Examples

```bash
# Basic usage (no extra deps)
python examples/basic_usage.py

# Framework examples
cd examples/<framework>
pip install -r requirements.txt
export OPENAI_API_KEY=sk-...
python agent.py
```

## Integration Pattern

All examples follow the same pattern - wrap tool execution with `ledger.run()`:

```python
result = await ledger.run(
    ToolCall(
        workflow_id="order-123",  # Scope for idempotency
        tool="stripe.charge",      # Tool name
        args={"amount": 5000},     # Arguments
    ),
    handler=lambda _: stripe.charge(5000),  # Actual execution
)
```

Same `(workflow_id, tool, args)` → cached result, no duplicate execution.
