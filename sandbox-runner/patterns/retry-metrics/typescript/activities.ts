import { activityInfo, Context } from "@temporalio/activity";

import { ALERT_THRESHOLD } from "./shared";

// Fails the first 5 attempts, then succeeds.
// Emits a metric counter on each attempt past the threshold (attempts 4 and 5).
const SUCCEED_ON_ATTEMPT = 6;

export async function callDownstreamService(endpoint: string): Promise<string> {
  const { attempt } = activityInfo();
  console.log(`Calling downstream service (attempt ${attempt})`);

  if (attempt > ALERT_THRESHOLD) {
    Context.current().metricMeter
      .createCounter("high_activity_error_count")
      .add(1);
    console.warn(
      `Attempt ${attempt} exceeds threshold ${ALERT_THRESHOLD} — ` +
        "emitted high_activity_error_count metric. " +
        "In production, your alerting system would fire here.",
    );
  }

  if (attempt < SUCCEED_ON_ATTEMPT) {
    throw new Error(`downstream service unavailable (attempt ${attempt})`);
  }

  return `Downstream call succeeded on attempt ${attempt}`;
}
