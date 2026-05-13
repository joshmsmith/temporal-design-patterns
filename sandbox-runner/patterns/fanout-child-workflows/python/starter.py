import asyncio
import time

from temporalio.client import Client

from shared import CHUNK_SIZE, TASK_QUEUE, TOTAL_RECORDS, WORKFLOW_ID_PREFIX
from workflows import FanOutWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    workflow_id = f"{WORKFLOW_ID_PREFIX}-{int(time.time() * 1000)}"
    handle = await client.start_workflow(
        FanOutWorkflow.run,
        args=[TOTAL_RECORDS, CHUNK_SIZE],
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )
    print(f"Started workflow: {workflow_id}")
    print(f"Processing {TOTAL_RECORDS} records in chunks of {CHUNK_SIZE}…")

    total = await handle.result()
    print(f"Fan-out complete: processed {total} records")
    print(
        f"Open the Temporal UI and search for '{workflow_id}' to see the parent and child workflows."
    )


if __name__ == "__main__":
    asyncio.run(main())
