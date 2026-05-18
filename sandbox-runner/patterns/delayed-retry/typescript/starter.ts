import { Client, Connection } from "@temporalio/client";

import { TASK_QUEUE, WORKFLOW_ID_PREFIX } from "./shared";
import { rateLimitedWorkflow } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const runId = `${WORKFLOW_ID_PREFIX}-${Date.now()}`;

    console.log("Starting rate-limited API workflow.");
    console.log(
      "Attempt 1 will receive a simulated HTTP 429 with Retry-After: 5s.",
    );
    console.log(
      "The nextRetryDelay overrides the RetryPolicy interval for that single retry.",
    );
    console.log();

    const handle = await client.workflow.start(rateLimitedWorkflow, {
      args: ["https://api.example.com/data"],
      taskQueue: TASK_QUEUE,
      workflowId: runId,
    });
    console.log(`Started workflow: ${handle.workflowId}`);

    const result = await handle.result();
    console.log(`Workflow result: ${result}`);
    console.log();
    console.log(
      `Open the Temporal UI and search for '${handle.workflowId}' to see the retry timing.`,
    );
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
