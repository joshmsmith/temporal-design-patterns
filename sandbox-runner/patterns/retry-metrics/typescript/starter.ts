import { Client, Connection } from "@temporalio/client";

import { ALERT_THRESHOLD, TASK_QUEUE, WORKFLOW_ID_PREFIX } from "./shared";
import { monitoredRetryWorkflow } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const runId = `${WORKFLOW_ID_PREFIX}-${Date.now()}`;

    console.log("Starting monitored retry workflow.");
    console.log(`  Alert threshold: attempt > ${ALERT_THRESHOLD}`);
    console.log(
      "  Service recovers on attempt 6 — metric will be emitted on attempts 4 and 5.",
    );
    console.log();

    const handle = await client.workflow.start(monitoredRetryWorkflow, {
      args: ["https://api.example.com/data"],
      taskQueue: TASK_QUEUE,
      workflowId: runId,
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
