import { log, proxyActivities } from "@temporalio/workflow";

import type * as activities from "./activities";

const { authorizeTransaction } = proxyActivities<typeof activities>({
  scheduleToCloseTimeout: "12 seconds", // total budget
  startToCloseTimeout: "5 seconds", // per attempt
  retry: {
    initialInterval: "1s",
    backoffCoefficient: 1,
  },
});

export async function paymentAuthWorkflow(transactionId: string): Promise<string> {
  try {
    return await authorizeTransaction(transactionId);
  } catch (err) {
    log.error("Authorization failed — 12-second SLA breached", { transactionId });
    return `Transaction ${transactionId}: SLA breached — authorization failed`;
  }
}
