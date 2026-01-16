import asyncio

from agent_ledger import (
    EffectLedger,
    EffectLedgerOptions,
    MemoryStore,
    ResourceDescriptor,
    ToolCall,
)


async def mock_github_create_issue(params: dict) -> dict:
    print(f"  [GitHub API] Creating issue: {params['title']}")
    return {
        "id": 123,
        "owner": params["owner"],
        "repo": params["repo"],
        "title": params["title"],
        "body": params["body"],
    }


async def main() -> None:
    print("=== Effect Ledger Usage Example ===\n")

    store = MemoryStore()
    ledger = EffectLedger(EffectLedgerOptions(store=store))

    params = {
        "owner": "acme",
        "repo": "my-project",
        "title": "Bug: Login fails on mobile",
        "body": "Users report login issues on iOS Safari",
    }

    print("1. First call - should execute:")
    result1 = await ledger.run(
        ToolCall(
            workflow_id="support-ticket-123",
            tool="github.issues.create",
            args=params,
            resource=ResourceDescriptor(
                namespace="github",
                type="issue",
                id={
                    "owner": params["owner"],
                    "repo": params["repo"],
                    "title": params["title"],
                },
            ),
        ),
        handler=lambda _: mock_github_create_issue(params),
    )
    print(f"  Result: Issue #{result1['id']}\n")

    print("2. Duplicate call (same idem_key) - should return cached:")
    result2 = await ledger.run(
        ToolCall(
            workflow_id="support-ticket-123",
            tool="github.issues.create",
            args=params,
            resource=ResourceDescriptor(
                namespace="github",
                type="issue",
                id={
                    "owner": params["owner"],
                    "repo": params["repo"],
                    "title": params["title"],
                },
            ),
        ),
        handler=lambda _: mock_github_create_issue(params),
    )
    print(f"  Result: Issue #{result2['id']} (cached, no API call)\n")

    print("3. Different args - should execute:")
    params2 = {
        "owner": "acme",
        "repo": "my-project",
        "title": "Feature: Dark mode",
        "body": "Users want dark mode support",
    }
    result3 = await ledger.run(
        ToolCall(
            workflow_id="support-ticket-123",
            tool="github.issues.create",
            args=params2,
            resource=ResourceDescriptor(
                namespace="github",
                type="issue",
                id={
                    "owner": params2["owner"],
                    "repo": params2["repo"],
                    "title": params2["title"],
                },
            ),
        ),
        handler=lambda _: mock_github_create_issue(params2),
    )
    print(f"  Result: Issue #{result3['id']}\n")

    print("4. Using idempotency_keys (subset of args):")
    params3 = {
        "owner": "acme",
        "repo": "my-project",
        "title": "Bug: Login fails on mobile",
        "body": "UPDATED: More details added",
    }
    result4 = await ledger.run(
        ToolCall(
            workflow_id="support-ticket-456",
            tool="github.issues.create",
            args=params3,
            idempotency_keys=["owner", "repo", "title"],
        ),
        handler=lambda _: mock_github_create_issue(params3),
    )
    print(f"  Result: Issue #{result4['id']}\n")

    print("5. Same idempotency_keys, different body - cached:")
    params4 = {
        "owner": "acme",
        "repo": "my-project",
        "title": "Bug: Login fails on mobile",
        "body": "ANOTHER UPDATE - should not execute",
    }
    result5 = await ledger.run(
        ToolCall(
            workflow_id="support-ticket-456",
            tool="github.issues.create",
            args=params4,
            idempotency_keys=["owner", "repo", "title"],
        ),
        handler=lambda _: mock_github_create_issue(params4),
    )
    print(f"  Result: Issue #{result5['id']} (cached)\n")

    print("=== Summary ===")
    print(f"Total effects stored: {store.size}")
    print("\nEffects in ledger:")
    for effect in await store.list_effects():
        print(
            f"  - {effect.tool} [{effect.status.value}] dedup_count={effect.dedup_count}"
        )


if __name__ == "__main__":
    asyncio.run(main())
