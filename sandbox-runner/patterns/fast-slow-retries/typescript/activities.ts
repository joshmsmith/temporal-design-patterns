import { activityInfo } from "@temporalio/activity";

/**
 * Call a downstream service.
 *
 * phase=1 (fast): always fails — simulates a service that is completely down.
 * phase=2 (slow): fails on attempts 1-2, succeeds on attempt 3 — simulates recovery.
 */
export async function callDownstream(request: string, phase: number): Promise<string> {
  const { attempt } = activityInfo();
  console.log(`Calling downstream service (phase ${phase}, attempt ${attempt})`);

  if (phase === 1) {
    throw new Error(`service down — fast phase exhausted (attempt ${attempt})`);
  }

  // Phase 2: service is recovering — succeeds on attempt 3
  if (attempt < 3) {
    throw new Error(`still recovering (phase 2, attempt ${attempt})`);
  }

  return `Success for '${request}' (phase 2, recovered on attempt ${attempt})`;
}
