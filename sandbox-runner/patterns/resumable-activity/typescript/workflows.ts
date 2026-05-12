import {
  condition,
  defineQuery,
  defineSignal,
  log,
  proxyActivities,
  setHandler,
} from "@temporalio/workflow";

import type * as activities from "./activities";
import type { TransferInput } from "./shared";

export const retryWithCorrectionSignal = defineSignal<[string]>(
  "retryWithCorrection",
);
export const approveSignal = defineSignal<[boolean]>("approve");
export const getStatusQuery = defineQuery<string>("getStatus");

const { executeTransfer } = proxyActivities<typeof activities>({
  startToCloseTimeout: "10 seconds",
  retry: { maximumAttempts: 1 },
});

export async function transferWorkflow(
  input: TransferInput,
): Promise<string> {
  let status = "PENDING";
  let correctedAccount: string | undefined;
  let approval: boolean | undefined;

  setHandler(retryWithCorrectionSignal, (account: string) => {
    correctedAccount = account;
  });
  setHandler(approveSignal, (decision: boolean) => {
    approval = decision;
  });
  setHandler(getStatusQuery, () => status);

  let account = input.toAccount;
  let correctionCount = 0;

  while (true) {
    status = "TRANSFERRING";
    try {
      await executeTransfer({ ...input, toAccount: account });
      break; // Activity succeeded — exit the correction loop
    } catch {
      correctionCount++;
      if (correctionCount > 5) {
        status = "FAILED";
        throw new Error("Too many correction attempts — workflow failed");
      }
      status = "AWAITING_CORRECTION";
      log.warn("Transfer failed — waiting for account correction", { account });
      // Park until the admin sends a correction signal
      await condition(() => correctedAccount !== undefined);
      account = correctedAccount!;
      correctedAccount = undefined;
    }
  }

  status = "AWAITING_APPROVAL";
  await condition(() => approval !== undefined);

  if (approval) {
    status = "COMPLETED";
    return `Transfer of ${input.amount} to ${account} completed`;
  }
  status = "REJECTED";
  return "Transfer rejected by client";
}
