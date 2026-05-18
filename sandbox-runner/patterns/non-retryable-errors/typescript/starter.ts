import { Client, Connection } from "@temporalio/client";

import { TASK_QUEUE, WORKFLOW_ID_PREFIX } from "./shared";
import { orderWorkflow } from "./workflows";

async function runOrder(
  client: Client,
  orderId: string,
  label: string,
): Promise<void> {
  console.log(`--- ${label} ---`);
  console.log(`Starting workflow for order: ${orderId}`);

  const handle = await client.workflow.start(orderWorkflow, {
    args: [orderId],
    taskQueue: TASK_QUEUE,
    workflowId: `${WORKFLOW_ID_PREFIX}-${orderId}-${Date.now()}`,
  });

  const result = await handle.result();
  console.log(`Result: ${result}`);
  console.log();
}

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });

    console.log(
      "This demo runs two workflows back-to-back to contrast retryable vs non-retryable errors.",
    );
    console.log();

    // Test 1: non-retryable error — fails immediately on attempt 1
    await runOrder(
      client,
      "INVALID-order-999",
      "Test 1: Non-retryable error (invalid order ID — fails immediately)",
    );

    // Test 2: transient error — retries on attempts 1-2, succeeds on attempt 3
    await runOrder(
      client,
      `order-${Date.now()}`,
      "Test 2: Retryable error (transient DB timeout — succeeds on attempt 3)",
    );

    console.log("Open the Temporal UI and compare the two workflow histories:");
    console.log(
      "  • The first has 1 activity attempt (non-retryable, failed immediately).",
    );
    console.log(
      "  • The second has 3 activity attempts (retried until success).",
    );
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
