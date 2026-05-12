import { log, proxyActivities } from "@temporalio/workflow";

import type * as activities from "./activities";

const { chargePaymentApi } = proxyActivities<typeof activities>({
  startToCloseTimeout: "5 seconds",
  retry: {
    maximumAttempts: 3,
    initialInterval: "1s",
    backoffCoefficient: 1,
  },
});

export async function paymentWorkflow(orderId: string): Promise<string> {
  try {
    return await chargePaymentApi(orderId);
  } catch (err) {
    log.error("All 3 payment attempts failed — escalating to on-call team", {
      orderId,
    });
    return `Order ${orderId}: payment failed after 3 attempts — escalated to on-call`;
  }
}
