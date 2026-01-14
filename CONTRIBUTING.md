# Contributing to agent-ledger

Thank you for your interest in contributing to agent-ledger!

## Development Setup

### Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

### Quick Start

```bash
# Clone the repository
git clone https://github.com/rune0/agent-ledger-py.git
cd agent-ledger-py

# Create virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e ".[dev]"

# Run tests
pytest

# Run all checks
ruff check agent_ledger/ tests/
ruff format --check agent_ledger/ tests/
mypy agent_ledger/
```

### Using Hatch (Alternative)

```bash
# Install hatch
pip install hatch

# Run all checks
hatch run check

# Run with coverage
hatch run test-cov
```

## Code Style

We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
# Lint
ruff check agent_ledger/ tests/

# Auto-fix lint issues
ruff check agent_ledger/ tests/ --fix

# Format
ruff format agent_ledger/ tests/
```

### Type Checking

We use strict mypy configuration:

```bash
mypy agent_ledger/
```

All code must pass type checking with no errors.

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=agent_ledger --cov-report=term-missing

# Run specific test
pytest tests/test_ledger.py::TestRun::test_executes_handler_and_commits_success
```

### Writing Tests

- All tests are async and use `pytest-asyncio`
- Use `MemoryStore` for unit tests
- Place tests in `tests/` directory
- Follow existing test structure (group by feature)

## Pull Request Process

1. **Fork** the repository and create your branch from `main`
2. **Write tests** for any new functionality
3. **Ensure all checks pass**: `hatch run check`
4. **Update documentation** if needed
5. **Submit a PR** with a clear description

### Commit Messages

- Use present tense ("Add feature" not "Added feature")
- Use imperative mood ("Fix bug" not "Fixes bug")
- Keep the first line under 72 characters
- Reference issues when applicable ("Fix #123")

### PR Title Format

```
type: description

Examples:
fix: handle concurrent approval race condition
feat: add OpenTelemetry tracing support
docs: update README with PostgreSQL setup
test: add concurrency tests for stale takeover
```

## Architecture Overview

```
agent_ledger/
├── __init__.py      # Public API exports
├── types.py         # Core types (Effect, ToolCall, Status, etc.)
├── errors.py        # Exception types
├── ledger.py        # Main EffectLedger class
├── utils.py         # Canonicalization, validation helpers
├── observability.py # Tracing and logging
└── stores/
    ├── base.py      # EffectStore protocol
    ├── memory.py    # In-memory implementation (dev/test)
    └── postgres.py  # PostgreSQL implementation (production)
```

### Key Concepts

- **Effect**: A recorded tool execution with status, result, and metadata
- **Idempotency Key**: SHA256 hash of (workflow_id, tool, canonicalized args)
- **CAS (Compare-And-Swap)**: All state changes use optimistic concurrency control
- **Terminal States**: `succeeded`, `failed`, `canceled`, `denied` are immutable

### Adding a New Store

1. Implement the `EffectStore` protocol in `stores/base.py`
2. Ensure all CAS operations use proper WHERE clause guards
3. Add integration tests (see `tests/test_ledger.py` for patterns)

## Questions?

Open an issue or start a discussion on GitHub.
