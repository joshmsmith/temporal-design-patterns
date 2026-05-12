import { activityInfo, ApplicationFailure } from "@temporalio/activity";

import type { TransferInput } from "./shared";

// Valid accounts for the demo — any other account number fails.
const VALID_ACCOUNTS = new Set(["account-123", "account-456", "account-789"]);

export async function executeTransfer(transfer: TransferInput): Promise<void> {
  const { attempt } = activityInfo();
  console.log(
    `Executing transfer: ${transfer.amount} from ${transfer.fromAccount} ` +
      `to ${transfer.toAccount} (attempt ${attempt})`,
  );

  if (!VALID_ACCOUNTS.has(transfer.toAccount)) {
    throw ApplicationFailure.nonRetryable(
      `Account ${transfer.toAccount} not found`,
      "AccountNotFoundError",
    );
  }

  console.log(
    `Transfer of ${transfer.amount} to ${transfer.toAccount} completed`,
  );
}
