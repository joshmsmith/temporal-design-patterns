import asyncio
import time

from temporalio.client import Client

from shared import TASK_QUEUE, WORKFLOW_ID_PREFIX
from workflows import FastSlowRetryWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    run_id = f"{WORKFLOW_ID_PREFIX}-{int(time.time() * 1000)}"

    print("Starting fast/slow retry workflow.")
    print("  Phase 1: up to 5 fast attempts (1s interval) — service is down, all fail")
    print("  Phase 2: unlimited slow attempts (3s interval) — service recovering, succeeds on attempt 3")
    print()

    handle = await client.start_workflow(
        FastSlowRetryWorkflow.run,
        "demo-request",
        id=run_id,
        task_queue=TASK_QUEUE,
    )
    print(f"Started workflow: {handle.id}")

    result = await handle.result()
    print(f"Workflow result: {result}")
    print()
    print(f"Open the Temporal UI and search for '{handle.id}' to see the two retry phases.")


if __name__ == "__main__":
    asyncio.run(main())
