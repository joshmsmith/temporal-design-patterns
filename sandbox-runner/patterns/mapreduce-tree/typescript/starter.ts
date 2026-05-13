import { Client, Connection } from "@temporalio/client";

import { RECORDS, TASK_QUEUE, WORKFLOW_ID_PREFIX } from "./shared";
import { nodeWorkflow } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const workflowId = `${WORKFLOW_ID_PREFIX}-${Date.now()}`;
    const handle = await client.workflow.start(nodeWorkflow, {
      args: [RECORDS, 0, ""],
      taskQueue: TASK_QUEUE,
      workflowId,
    });
    console.log(`Started workflow: ${workflowId}`);
    console.log(`Processing ${RECORDS.length} records via MapReduce Tree…`);

    const results = await handle.result();
    console.log(`MapReduce Tree complete: ${results.length} results`);
    console.log(`Results: ${results.join(", ")}`);
    console.log(
      `Open the Temporal UI and search for '${workflowId}' to see the full workflow tree.`
    );
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
