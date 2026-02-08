"""
CrewAI with Idempotent Tools

A CrewAI crew where tool executions are wrapped with agent-ledger
to prevent duplicate side effects.

Usage:
    export OPENAI_API_KEY=sk-...
    python agent.py
"""

import asyncio
import inspect
import os
from functools import wraps

from agent_ledger import (
    EffectLedger,
    EffectLedgerOptions,
    LedgerHooks,
    MemoryStore,
    ToolCall,
)

# --- Setup ---

store = MemoryStore()
ledger = EffectLedger(EffectLedgerOptions(store=store))

WORKFLOW_ID = "order-42"


# --- Idempotent tool wrapper ---


def idempotent(
    tool_name: str,
    idempotency_keys: list[str] | None = None,
    hooks: LedgerHooks | None = None,
):
    """
    Decorator to wrap a tool function with agent-ledger idempotency.

    Args:
        tool_name: Unique identifier for this tool in the ledger
        idempotency_keys: Subset of arg names to use for deduplication (default: all args)
        hooks: LedgerHooks for approval flows and notifications
    """

    def decorator(func):
        @wraps(func)  # Preserves function signature for CrewAI schema generation
        def wrapper(*args, **kwargs):
            # Normalize positional/keyword args to a stable dict for idempotency hashing.
            bound = inspect.signature(func).bind_partial(*args, **kwargs)
            bound.apply_defaults()
            call_args = dict(bound.arguments)

            async def _run():
                async def _handler(effect):
                    result = func(**call_args)
                    if inspect.isawaitable(result):
                        return await result
                    return result

                return await ledger.run(
                    ToolCall(
                        workflow_id=WORKFLOW_ID,
                        tool=tool_name,
                        args=call_args,
                        idempotency_keys=idempotency_keys,
                    ),
                    handler=_handler,
                    hooks=hooks,
                )

            return asyncio.run(_run())

        return wrapper

    return decorator


# --- Tools ---


# ADVANCED: Require approval for large charges (> $100)
charge_hooks = LedgerHooks(
    requires_approval=lambda call: call.args.get("amount_cents", 0) > 10000,
    on_approval_required=lambda effect: print(
        f"  ⏸️  APPROVAL REQUIRED - call ledger.approve('{effect.idem_key}')"
    ),
)


@idempotent("stripe.charge", hooks=charge_hooks)
def charge_customer(amount_cents: int, currency: str = "usd") -> str:
    """
    Charge a customer's credit card.
    Amount is in cents (e.g., 5000 = $50.00).
    """
    print(f"  💳 Charging ${amount_cents / 100:.2f} {currency.upper()}...")
    return f"Charged ${amount_cents / 100:.2f} {currency.upper()}. Charge ID: ch_xxx"


# ADVANCED: Only use recipient + subject for idempotency (ignore body changes)
@idempotent("email.send", idempotency_keys=["to", "subject"])
def send_email(to: str, subject: str, body: str) -> str:
    """Send an email to a recipient."""
    print(f"  📧 Sending email to {to}: {subject}")
    return f"Email sent to {to}. Message ID: msg_xxx"


@idempotent("tickets.create")
def create_ticket(title: str, description: str) -> str:
    """Create a support ticket."""
    print(f"  🎫 Creating ticket: {title}")
    return f"Ticket created: TKT-123 - {title}"


# --- Crew ---


def create_crew():
    from crewai import Agent, Crew, Process, Task
    from crewai.tools import tool

    charge_customer_tool = tool("Charge Customer")(charge_customer)
    send_email_tool = tool("Send Email")(send_email)
    create_ticket_tool = tool("Create Ticket")(create_ticket)

    order_agent = Agent(
        role="Order Processor",
        goal="Process customer orders by charging, sending confirmation, and creating tickets",
        backstory="You are an efficient order processor who handles e-commerce transactions.",
        tools=[charge_customer_tool, send_email_tool, create_ticket_tool],
        verbose=True,
    )

    task = Task(
        description="""
        Process order #42 for customer alice@example.com:
        1. Charge $75.00 to their card
        2. Send an order confirmation email
        3. Create a support ticket for order tracking
        """,
        expected_output="Confirmation that all three steps completed successfully",
        agent=order_agent,
    )

    return Crew(agents=[order_agent], tasks=[task], process=Process.sequential)


# --- Main ---


def main():
    if not os.environ.get("OPENAI_API_KEY"):
        print("Set OPENAI_API_KEY environment variable")
        return

    print("=" * 60)
    print("CREWAI EXECUTION")
    print("=" * 60)

    crew = create_crew()
    result = crew.kickoff()

    print()
    print("=" * 60)
    print("RESULT")
    print("=" * 60)
    print(result)

    print()
    print("=" * 60)
    print("LEDGER (audit trail)")
    print("=" * 60)

    import asyncio

    async def show_ledger():
        for effect in await store.list_effects():
            print(f"  {effect.tool}: {effect.args_canonical} → {effect.result}")

    asyncio.run(show_ledger())


if __name__ == "__main__":
    main()
