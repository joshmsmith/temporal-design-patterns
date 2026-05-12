import { activityInfo } from "@temporalio/activity";

export async function chargePaymentApi(orderId: string): Promise<string> {
  const { attempt } = activityInfo();
  console.log(`Charging payment API for order ${orderId} (attempt ${attempt})`);
  // The payment gateway is down — every attempt fails.
  // Change this to return a value immediately to watch the happy path.
  throw new Error(`payment gateway unavailable (attempt ${attempt})`);
}
