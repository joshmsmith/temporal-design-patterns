import { log, proxyActivities } from "@temporalio/workflow";

import type * as activities from "./activities";

// Phase 1: fast retries — short interval, bounded attempt count.
const fastActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: "5 seconds",
  retry: {
    initialInterval: "1s",
    backoffCoefficient: 1,
    maximumInterval: "1s",
    maximumAttempts: 5,
  },
});

// Phase 2: slow retries — long fixed interval, unlimited attempts.
const slowActivities = proxyActivities<typeof activities>({
  startToCloseTimeout: "5 seconds",
  retry: {
    initialInterval: "3s",
    backoffCoefficient: 1,
    // maximumAttempts not set — unlimited
  },
});

export async function fastSlowRetryWorkflow(request: string): Promise<string> {
  // Phase 1: fast retries — service is down, all attempts fail
  try {
    return await fastActivities.callDownstream(request, 1);
  } catch {
    log.warn("Fast retries exhausted — switching to slow retry phase", { request });
  }

  // Phase 2: slow retries — service is recovering, succeeds after a few attempts
  return await slowActivities.callDownstream(request, 2);
}
