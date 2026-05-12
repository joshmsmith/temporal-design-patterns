import asyncio
import time

from temporalio.client import Client

from shared import ALERT_THRESHOLD, TASK_QUEUE, WORKFLOW_ID_PREFIX
from workflows import MonitoredRetryWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    run_id = f"{WORKFLOW_ID_PREFIX}-{int(time.time() * 1000)}"

    print("Starting monitored retry workflow.")
    print(f"  Alert threshold: attempt > {ALERT_THRESHOLD}")
    print("  Service recovers on attempt 6 — metric will be emitted on attempts 4 and 5.")
    print()

    handle = await client.start_workflow(
        MonitoredRetryWorkflow.run,
        "https://api.example.com/data",
        id=run_id,
        task_queue=TASK_QUEUE,
    )
    print(f"Started workflow: {handle.id}")

    result = await handle.result()
    print(f"Workflow result: {result}")
    print()
    print(f"Open the Temporal UI and search for '{handle.id}' to inspect the retry history.")


if __name__ == "__main__":
    asyncio.run(main())
