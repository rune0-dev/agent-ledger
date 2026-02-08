"""
Vanilla OpenAI Function Calling with Idempotent Tools

No framework - just the OpenAI SDK with agent-ledger for idempotency.
This is the simplest possible integration pattern.

Usage:
    export OPENAI_API_KEY=sk-...
    python agent.py
"""

import asyncio
import json
import os
from typing import cast

from openai import AsyncOpenAI
from openai.types.chat import (
    ChatCompletionMessageParam,
    ChatCompletionMessageToolCall,
    ChatCompletionToolMessageParam,
    ChatCompletionToolParam,
)

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

# --- Tool implementations ---


async def charge_customer(amount_cents: int, currency: str = "usd") -> dict:
    print(f"  💳 Charging ${amount_cents / 100:.2f} {currency.upper()}...")
    return {"charge_id": "ch_xxx", "amount": amount_cents, "status": "succeeded"}


async def send_email(to: str, subject: str, body: str) -> dict:
    print(f"  📧 Sending email to {to}: {subject}")
    return {"message_id": "msg_xxx", "status": "sent"}


async def create_ticket(title: str, description: str) -> dict:
    print(f"  🎫 Creating ticket: {title}")
    return {"ticket_id": "TKT-123", "status": "open"}


# --- Tool registry with idempotency ---

TOOLS = {
    "charge_customer": charge_customer,
    "send_email": send_email,
    "create_ticket": create_ticket,
}

TOOL_SCHEMAS: list[ChatCompletionToolParam] = [
    {
        "type": "function",
        "function": {
            "name": "charge_customer",
            "description": "Charge a customer's credit card",
            "parameters": {
                "type": "object",
                "properties": {
                    "amount_cents": {
                        "type": "integer",
                        "description": "Amount in cents",
                    },
                    "currency": {"type": "string", "default": "usd"},
                },
                "required": ["amount_cents"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "send_email",
            "description": "Send an email",
            "parameters": {
                "type": "object",
                "properties": {
                    "to": {"type": "string"},
                    "subject": {"type": "string"},
                    "body": {"type": "string"},
                },
                "required": ["to", "subject", "body"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_ticket",
            "description": "Create a support ticket",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "description": {"type": "string"},
                },
                "required": ["title", "description"],
            },
        },
    },
]


async def execute_tool(name: str, args: dict) -> str:
    """Execute a tool with idempotency via agent-ledger."""
    tool_fn = TOOLS[name]

    async def _handler(effect):
        return await tool_fn(**args)

    # ADVANCED: Configure hooks for approval flows and custom idempotency keys
    hooks = None
    idempotency_keys = None

    if name == "charge_customer":
        # Require human approval for large charges (> $100)
        hooks = LedgerHooks(
            requires_approval=lambda call: call.args.get("amount_cents", 0) > 10000,
            on_approval_required=lambda effect: print(
                f"  ⏸️  APPROVAL REQUIRED - call ledger.approve('{effect.idem_key}')"
            ),
        )

    if name == "send_email":
        # Only use recipient + subject for idempotency (ignore body changes)
        idempotency_keys = ["to", "subject"]

    result = await ledger.run(
        ToolCall(
            workflow_id=WORKFLOW_ID,
            tool=name,
            args=args,
            idempotency_keys=idempotency_keys,  # None = use all args (default)
        ),
        handler=_handler,
        hooks=hooks,
    )
    return json.dumps(result)


# --- Agent loop ---


async def run_agent(user_message: str, client: AsyncOpenAI) -> str:
    messages: list[ChatCompletionMessageParam] = [
        {"role": "user", "content": user_message}
    ]

    while True:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=TOOL_SCHEMAS,
        )

        msg = response.choices[0].message
        messages.append(cast("ChatCompletionMessageParam", msg.model_dump()))

        if not msg.tool_calls:
            return msg.content or ""

        for tool_call in msg.tool_calls:
            # Only handle function tool calls (not custom tools)
            if not isinstance(tool_call, ChatCompletionMessageToolCall):
                continue
            name = tool_call.function.name
            args = json.loads(tool_call.function.arguments)

            print(f"\n[Tool: {name}]")
            result = await execute_tool(name, args)

            tool_message: ChatCompletionToolMessageParam = {
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": result,
            }
            messages.append(tool_message)


# --- Main ---


async def main():
    if not os.environ.get("OPENAI_API_KEY"):
        print("Set OPENAI_API_KEY environment variable")
        return
    client = AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])

    query = """
    Process order #42 for alice@example.com:
    1. Charge $75.00
    2. Send confirmation email
    3. Create a support ticket
    """

    print("=" * 60)
    print("USER REQUEST")
    print("=" * 60)
    print(query.strip())
    print()

    print("=" * 60)
    print("AGENT EXECUTION")
    print("=" * 60)

    response = await run_agent(query, client)

    print()
    print("=" * 60)
    print("AGENT RESPONSE")
    print("=" * 60)
    print(response)

    print()
    print("=" * 60)
    print("LEDGER (audit trail)")
    print("=" * 60)
    for effect in await store.list_effects():
        print(f"  {effect.tool}: {effect.args_canonical} → {effect.result}")


if __name__ == "__main__":
    asyncio.run(main())
