import { Client, Connection } from "@temporalio/client";

import { CHUNK_SIZE, TASK_QUEUE, TOTAL_RECORDS, WORKFLOW_ID_PREFIX } from "./shared";
import { fanOutWorkflow } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const workflowId = `${WORKFLOW_ID_PREFIX}-${Date.now()}`;
    const handle = await client.workflow.start(fanOutWorkflow, {
      args: [TOTAL_RECORDS, CHUNK_SIZE],
      taskQueue: TASK_QUEUE,
      workflowId,
    });
    console.log(`Started workflow: ${workflowId}`);
    console.log(`Processing ${TOTAL_RECORDS} records in chunks of ${CHUNK_SIZE}…`);

    const total = await handle.result();
    console.log(`Fan-out complete: processed ${total} records`);
    console.log(
      `Open the Temporal UI and search for '${workflowId}' to see the parent and child workflows.`
    );
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
