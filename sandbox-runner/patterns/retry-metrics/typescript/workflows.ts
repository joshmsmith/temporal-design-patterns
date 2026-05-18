import { proxyActivities } from "@temporalio/workflow";

import type * as activities from "./activities";

const { callDownstreamService } = proxyActivities<typeof activities>({
  startToCloseTimeout: "5 seconds",
  retry: {
    initialInterval: "1s",
    backoffCoefficient: 1,
    // No maximumAttempts — retries until success.
    // The metric counter alerts on-call before the SLA is breached.
  },
});

export async function monitoredRetryWorkflow(endpoint: string): Promise<string> {
  return await callDownstreamService(endpoint);
}
