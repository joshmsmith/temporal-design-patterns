import { Client, Connection } from "@temporalio/client";

import { TASK_QUEUE, WORKFLOW_ID_PREFIX } from "./shared";
import { paymentAuthWorkflow } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const txnId = `txn-${Date.now()}`;

    console.log(`Starting payment authorization for ${txnId}`);
    console.log(
      "The downstream service is slow — each attempt exceeds the 5s per-attempt timeout.",
    );
    console.log("The 12s total budget will be exhausted after ~2 attempts.");
    console.log();

    const handle = await client.workflow.start(paymentAuthWorkflow, {
      args: [txnId],
      taskQueue: TASK_QUEUE,
      workflowId: `${WORKFLOW_ID_PREFIX}-${txnId}`,
    });
    console.log(`Started workflow: ${handle.workflowId}`);

    const result = await handle.result();
    console.log(`Workflow result: ${result}`);
    console.log();
    console.log(
      `Open the Temporal UI and search for '${handle.workflowId}' to inspect the timeout history.`,
    );
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
