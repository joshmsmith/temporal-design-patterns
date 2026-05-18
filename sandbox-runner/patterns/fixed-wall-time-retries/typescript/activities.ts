import { activityInfo } from "@temporalio/activity";

export async function authorizeTransaction(transactionId: string): Promise<string> {
  const { attempt } = activityInfo();
  console.log(
    `Authorizing transaction ${transactionId} (attempt ${attempt}) — simulating a slow downstream service`,
  );
  // Simulate a service that takes longer than the 5s StartToCloseTimeout.
  // Each attempt will be cancelled by Temporal, consuming the time budget.
  // Change the sleep to 1000ms to watch a successful authorization.
  await new Promise((resolve) => setTimeout(resolve, 10_000));
  return `authorized-${transactionId}`;
}
