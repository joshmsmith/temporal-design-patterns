import { Client, Connection } from "@temporalio/client";

import { TASK_QUEUE, WORKFLOW_ID_PREFIX } from "./shared";
import { fastSlowRetryWorkflow } from "./workflows";

async function main(): Promise<void> {
  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const runId = `${WORKFLOW_ID_PREFIX}-${Date.now()}`;

    console.log("Starting fast/slow retry workflow.");
    console.log(
      "  Phase 1: up to 5 fast attempts (1s interval) — service is down, all fail",
    );
    console.log(
      "  Phase 2: unlimited slow attempts (3s interval) — service recovering, succeeds on attempt 3",
    );
    console.log();

    const handle = await client.workflow.start(fastSlowRetryWorkflow, {
      args: ["demo-request"],
      taskQueue: TASK_QUEUE,
      workflowId: runId,
    });
    console.log(`Started workflow: ${handle.workflowId}`);

    const result = await handle.result();
    console.log(`Workflow result: ${result}`);
    console.log();
    console.log(
      `Open the Temporal UI and search for '${handle.workflowId}' to see the two retry phases.`,
    );
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
