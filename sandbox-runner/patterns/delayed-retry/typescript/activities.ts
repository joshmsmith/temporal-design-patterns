import { activityInfo, ApplicationFailure } from "@temporalio/activity";

export async function callRateLimitedApi(endpoint: string): Promise<string> {
  const { attempt } = activityInfo();

  // Simulate an HTTP 429 on the first attempt with a Retry-After: 5s header.
  // The nextRetryDelay overrides the RetryPolicy interval for this single retry.
  if (attempt === 1) {
    const retryAfterSeconds = 5;
    console.log(
      `Attempt ${attempt}: received HTTP 429 — Retry-After: ${retryAfterSeconds}s`,
    );
    console.log(
      `  Throwing ApplicationFailure with nextRetryDelay=${retryAfterSeconds}s`,
    );
    console.log(
      `  (RetryPolicy initialInterval=1s is overridden for this retry only)`,
    );
    throw ApplicationFailure.create({
      message: `Rate limited — Retry-After: ${retryAfterSeconds}s`,
      type: "RateLimitError",
      nextRetryDelay: `${retryAfterSeconds}s`,
    });
  }

  console.log(`Attempt ${attempt}: API call succeeded`);
  return `{"status": "ok", "data": "result-from-${endpoint}"}`;
}
