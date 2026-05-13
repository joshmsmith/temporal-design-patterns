import { Client, Connection } from "@temporalio/client";

import { RECORD_IDS, TASK_QUEUE, WINDOW_SIZE, WORKFLOW_ID_PREFIX } from "./shared";
import { slidingWindowWorkflow } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const workflowId = `${WORKFLOW_ID_PREFIX}-${Date.now()}`;
    const handle = await client.workflow.start(slidingWindowWorkflow, {
      args: [{ recordIds: RECORD_IDS, windowSize: WINDOW_SIZE }],
      taskQueue: TASK_QUEUE,
      workflowId,
    });
    console.log(`Started workflow: ${workflowId}`);
    console.log(`Processing ${RECORD_IDS.length} records with window size ${WINDOW_SIZE}…`);

    const totalProcessed = await handle.result();
    console.log(`Sliding window complete: processed ${totalProcessed} records`);
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
