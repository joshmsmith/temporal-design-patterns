import { log, proxyActivities } from "@temporalio/workflow";

import type * as activities from "./activities";

// Normal RetryPolicy: 1s initial interval.
// The nextRetryDelay set in the activity overrides this for the rate-limited attempt.
const { callRateLimitedApi } = proxyActivities<typeof activities>({
  startToCloseTimeout: "10 seconds",
  retry: {
    initialInterval: "1s",
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
});

export async function rateLimitedWorkflow(endpoint: string): Promise<string> {
  log.info("Calling rate-limited API", { endpoint });
  const result = await callRateLimitedApi(endpoint);
  log.info("API call succeeded", { result });
  return result;
}
