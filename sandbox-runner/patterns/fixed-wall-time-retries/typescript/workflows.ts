import { ActivityFailure, log, proxyActivities, TimeoutFailure } from "@temporalio/workflow";
import { TimeoutType } from "@temporalio/common";

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
    if (err instanceof ActivityFailure) {
      const cause = err.cause;
      if (cause instanceof TimeoutFailure && cause.type === TimeoutType.SCHEDULE_TO_CLOSE) {
        log.error("Authorization failed — 12-second SLA breached", { transactionId });
      }
    }
    return `Transaction ${transactionId}: SLA breached — authorization failed`;
  }
}
