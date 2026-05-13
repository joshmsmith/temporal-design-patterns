import asyncio
import time

from temporalio.client import Client

from shared import RECORDS, TASK_QUEUE, WORKFLOW_ID_PREFIX
from workflows import NodeWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    workflow_id = f"{WORKFLOW_ID_PREFIX}-{int(time.time() * 1000)}"
    handle = await client.start_workflow(
        NodeWorkflow.run,
        args=[RECORDS, 0, ""],
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )
    print(f"Started workflow: {workflow_id}")
    print(f"Processing {len(RECORDS)} records via MapReduce Tree…")

    results = await handle.result()
    print(f"MapReduce Tree complete: {len(results)} results")
    print(f"Results: {', '.join(results)}")
    print(
        f"Open the Temporal UI and search for '{workflow_id}' to see the full workflow tree."
    )


if __name__ == "__main__":
    asyncio.run(main())
