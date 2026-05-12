import { activityInfo, ApplicationFailure } from "@temporalio/activity";

export async function processOrder(orderId: string): Promise<string> {
  const { attempt } = activityInfo();
  console.log(`Processing order ${orderId} (attempt ${attempt})`);

  // Permanent failure — non-retryable.
  // Temporal delivers this to the Workflow immediately without further retries.
  if (orderId.startsWith("INVALID-")) {
    throw ApplicationFailure.nonRetryable(
      `Order ${orderId} not found in database`,
      "OrderNotFoundError",
    );
  }

  // Transient failure — retryable.
  // Temporal retries this automatically until it succeeds (attempt 3).
  if (attempt < 3) {
    throw new Error(`database connection timeout (attempt ${attempt}) — will retry`);
  }

  return `Order ${orderId} processed successfully on attempt ${attempt}`;
}
