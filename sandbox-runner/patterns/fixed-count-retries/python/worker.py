import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import charge_payment_api
from shared import TASK_QUEUE
from workflows import PaymentWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[PaymentWorkflow],
        activities=[charge_payment_api],
    )
    print(f"Worker listening on task queue '{TASK_QUEUE}'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
