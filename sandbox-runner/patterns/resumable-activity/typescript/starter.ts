import { execSync } from "node:child_process";

import { Client, Connection } from "@temporalio/client";

import {
  approveSignal,
  getStatusQuery,
  retryWithCorrectionSignal,
  transferWorkflow,
} from "./workflows";
import { TASK_QUEUE, WORKFLOW_ID_PREFIX, type TransferInput } from "./shared";

async function waitForStatus(
  handle: ReturnType<Client["workflow"]["getHandle"]>,
  targetStatus: string,
): Promise<void> {
  while (true) {
    const status = await handle.query(getStatusQuery);
    if (status === targetStatus) return;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

async function main(): Promise<void> {
  // Register the TransferStatus search attribute (idempotent — ignored if already exists).
  try {
    execSync(
      "temporal operator search-attribute create --name TransferStatus --type Keyword",
      { stdio: "pipe" },
    );
  } catch { /* already registered */ }

  const connection = await Connection.connect();
  try {
    const client = new Client({ connection });
    const workflowId = `${WORKFLOW_ID_PREFIX}-${Date.now()}`;

    const transfer: TransferInput = {
      fromAccount: "account-001",
      toAccount: "WRONG-ACCOUNT-999", // Invalid — will fail the first attempt
      amount: 500.0,
    };

    console.log(`Starting transfer workflow: ${workflowId}`);
    console.log(`  From:   ${transfer.fromAccount}`);
    console.log(`  To:     ${transfer.toAccount}  ← invalid account number`);
    console.log(`  Amount: $${transfer.amount.toFixed(2)}`);
    console.log();

    const handle = await client.workflow.start(transferWorkflow, {
      args: [transfer],
      taskQueue: TASK_QUEUE,
      workflowId,
    });

    // Wait for the workflow to park in AWAITING_CORRECTION
    console.log("Waiting for workflow to fail and park...");
    await waitForStatus(handle, "AWAITING_CORRECTION");
    console.log(
      "Workflow is now AWAITING_CORRECTION — parked in Temporal, no polling, zero cost.",
    );
    console.log();

    // Simulate an operator correcting the account number
    const correctedAccount = "account-123";
    console.log(
      `Sending correction signal with corrected account: ${correctedAccount}`,
    );
    await handle.signal(retryWithCorrectionSignal, correctedAccount);

    // Wait for the workflow to park in AWAITING_APPROVAL
    await waitForStatus(handle, "AWAITING_APPROVAL");
    console.log("Transfer succeeded. Workflow is now AWAITING_APPROVAL.");
    console.log();

    // Simulate the client approving the transfer
    console.log("Sending approval signal: approved=true");
    await handle.signal(approveSignal, true);

    const result = await handle.result();
    console.log(`Workflow completed: ${result}`);
    console.log();
    console.log(
      `Open the Temporal UI and search for '${workflowId}' to see the full signal history.`,
    );
  } finally {
    await connection.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
