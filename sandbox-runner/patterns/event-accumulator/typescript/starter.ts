import { Client, Connection } from "@temporalio/client";

import { TASK_QUEUE, WORKFLOW_ID_PREFIX } from "./shared";
import type { OrderItem } from "./shared";
import { accumulatorWorkflow, addItemSignal } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });

    // Items arriving for two separate orders from multiple producers.
    // item-1 is sent twice to demonstrate deduplication.
    const items: OrderItem[] = [
      { orderId: "order-A", itemId: "item-1", name: "Widget" },
      { orderId: "order-B", itemId: "item-2", name: "Gadget" },
      { orderId: "order-A", itemId: "item-3", name: "Gizmo" },
      { orderId: "order-B", itemId: "item-4", name: "Doohickey" },
      { orderId: "order-A", itemId: "item-1", name: "Widget" }, // duplicate — ignored
      { orderId: "order-A", itemId: "item-5", name: "Thingamajig" },
    ];

    for (const item of items) {
      const workflowId = `${WORKFLOW_ID_PREFIX}-${item.orderId}`;
      // Signal-With-Start: start the workflow if not running, otherwise just signal
      await client.workflow.signalWithStart(accumulatorWorkflow, {
        workflowId,
        taskQueue: TASK_QUEUE,
        args: [item.orderId, [], []],
        signal: addItemSignal,
        signalArgs: [item],
      });
      console.log(`Signaled ${workflowId}: ${item.name} (${item.itemId})`);
    }

    console.log("\nWaiting for workflows to process their items...");
    for (const orderId of ["order-A", "order-B"]) {
      const workflowId = `${WORKFLOW_ID_PREFIX}-${orderId}`;
      const handle = client.workflow.getHandle(workflowId);
      const result = await handle.result();
      console.log(`${workflowId} result: ${result}`);
    }
    console.log(`\nOpen the Temporal UI and search for '${WORKFLOW_ID_PREFIX}' to see the accumulator workflows.`);
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
