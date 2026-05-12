import { Client, Connection } from "@temporalio/client";

import { TASK_QUEUE, WORKFLOW_ID_PREFIX } from "./shared";
import { paymentWorkflow } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const orderId = `order-${Date.now()}`;

    console.log(`Starting payment workflow for ${orderId}`);
    console.log("The payment gateway is down — all 3 attempts will fail.");
    console.log();

    const handle = await client.workflow.start(paymentWorkflow, {
      args: [orderId],
      taskQueue: TASK_QUEUE,
      workflowId: `${WORKFLOW_ID_PREFIX}-${orderId}`,
    });
    console.log(`Started workflow: ${handle.workflowId}`);

    const result = await handle.result();
    console.log(`Workflow result: ${result}`);
    console.log();
    console.log(
      `Open the Temporal UI and search for '${handle.workflowId}' to inspect the retry history.`,
    );
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
