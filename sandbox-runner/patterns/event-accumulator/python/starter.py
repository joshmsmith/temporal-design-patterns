import asyncio

from temporalio.client import Client

from shared import TASK_QUEUE, WORKFLOW_ID_PREFIX, OrderItem
from workflows import AccumulatorWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")

    # Items arriving for two separate orders from multiple producers.
    # item-1 is sent twice to demonstrate deduplication.
    items = [
        OrderItem(order_id="order-A", item_id="item-1", name="Widget"),
        OrderItem(order_id="order-B", item_id="item-2", name="Gadget"),
        OrderItem(order_id="order-A", item_id="item-3", name="Gizmo"),
        OrderItem(order_id="order-B", item_id="item-4", name="Doohickey"),
        OrderItem(order_id="order-A", item_id="item-1", name="Widget"),  # duplicate — ignored
        OrderItem(order_id="order-A", item_id="item-5", name="Thingamajig"),
    ]

    for item in items:
        workflow_id = f"{WORKFLOW_ID_PREFIX}-{item.order_id}"
        # Signal-With-Start: start the workflow if not running, otherwise just signal
        await client.start_workflow(
            AccumulatorWorkflow.run,
            args=[item.order_id, [], []],
            id=workflow_id,
            task_queue=TASK_QUEUE,
            start_signal="add-item",
            start_signal_args=[item],
        )
        print(f"Signaled {workflow_id}: {item.name} ({item.item_id})")

    print("\nWaiting for workflows to process their items...")
    for order_id in ["order-A", "order-B"]:
        workflow_id = f"{WORKFLOW_ID_PREFIX}-{order_id}"
        handle = client.get_workflow_handle(workflow_id)
        result = await handle.result()
        print(f"{workflow_id} result: {result}")
    print(f"\nOpen the Temporal UI and search for '{WORKFLOW_ID_PREFIX}' to see the accumulator workflows.")


if __name__ == "__main__":
    asyncio.run(main())
