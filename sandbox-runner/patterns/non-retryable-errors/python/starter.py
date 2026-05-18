import asyncio
import time

from temporalio.client import Client

from shared import TASK_QUEUE, WORKFLOW_ID_PREFIX
from workflows import OrderWorkflow


async def run_order(client: Client, order_id: str, label: str) -> None:
    print(f"--- {label} ---")
    print(f"Starting workflow for order: {order_id}")

    handle = await client.start_workflow(
        OrderWorkflow.run,
        order_id,
        id=f"{WORKFLOW_ID_PREFIX}-{order_id}-{int(time.time() * 1000)}",
        task_queue=TASK_QUEUE,
    )

    result = await handle.result()
    print(f"Result: {result}")
    print()


async def main() -> None:
    client = await Client.connect("localhost:7233")

    print("This demo runs two workflows back-to-back to contrast retryable vs non-retryable errors.")
    print()

    # Test 1: non-retryable error — fails immediately on attempt 1
    await run_order(
        client,
        "INVALID-order-999",
        "Test 1: Non-retryable error (invalid order ID — fails immediately)",
    )

    # Test 2: transient error — retries on attempts 1-2, succeeds on attempt 3
    await run_order(
        client,
        f"order-{int(time.time() * 1000)}",
        "Test 2: Retryable error (transient DB timeout — succeeds on attempt 3)",
    )

    print("Open the Temporal UI and compare the two workflow histories:")
    print("  • The first has 1 activity attempt (non-retryable, failed immediately).")
    print("  • The second has 3 activity attempts (retried until success).")


if __name__ == "__main__":
    asyncio.run(main())
