import { log, proxyActivities } from "@temporalio/workflow";

import type * as activities from "./activities";

const { processOrder } = proxyActivities<typeof activities>({
  startToCloseTimeout: "5 seconds",
  retry: {
    maximumAttempts: 5,
    initialInterval: "1s",
    backoffCoefficient: 1,
  },
});

export async function orderWorkflow(orderId: string): Promise<string> {
  try {
    return await processOrder(orderId);
  } catch (err) {
    log.error("Order processing failed", { orderId, error: err });
    return `Order ${orderId} failed: ${(err as Error).message}`;
  }
}
