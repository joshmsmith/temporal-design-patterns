import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import process_order
from shared import TASK_QUEUE
from workflows import OrderWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderWorkflow],
        activities=[process_order],
    )
    print(f"Worker listening on task queue '{TASK_QUEUE}'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
