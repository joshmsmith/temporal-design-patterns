import { Client, Connection } from "@temporalio/client";

import { PAGE_SIZE, TASK_QUEUE, TOTAL_RECORDS, WORKFLOW_ID_PREFIX } from "./shared";
import { batchIteratorWorkflow } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const workflowId = `${WORKFLOW_ID_PREFIX}-${Date.now()}`;
    const handle = await client.workflow.start(batchIteratorWorkflow, {
      args: [0, 0],
      taskQueue: TASK_QUEUE,
      workflowId,
    });
    console.log(`Started workflow: ${workflowId}`);
    console.log(`Processing ${TOTAL_RECORDS} records (page size ${PAGE_SIZE})…`);

    const total = await handle.result();
    console.log(`Batch iterator complete: processed ${total} records`);
    console.log(
      `Open the Temporal UI and search for '${workflowId}' to see the Continue-As-New chain.`
    );
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
