import {
  condition,
  continueAsNew,
  defineSignal,
  log,
  proxyActivities,
  setHandler,
  workflowInfo,
} from "@temporalio/workflow";

import type * as activities from "./activities";
import type { OrderItem } from "./shared";

const { processItems } = proxyActivities<typeof activities>({
  startToCloseTimeout: "10 seconds",
});

export const addItemSignal = defineSignal<[OrderItem]>("add-item");
export const exitSignal = defineSignal("exit");

/**
 * Accumulator workflow: collects order items sent via signals, deduplicates
 * them, and processes them together after a sliding inactivity timeout.
 *
 * Workflow ID should be deterministic per bucket key so Signal-With-Start
 * routes all signals for the same order to the same running instance.
 */
export async function accumulatorWorkflow(
  bucketKey: string,
  accumulated: OrderItem[] = [],
  seenKeys: string[] = [],
): Promise<string> {
  const seenSet = new Set(seenKeys);
  const items: OrderItem[] = [...accumulated];
  // Buffer for signals received before the main loop drains them
  const unprocessed: OrderItem[] = [];
  let exitRequested = false;

  // Signal handlers just buffer — no activity calls inside handlers
  setHandler(addItemSignal, (item: OrderItem) => {
    unprocessed.push(item);
  });
  setHandler(exitSignal, () => {
    exitRequested = true;
  });

  do {
    // Sliding window: block until a signal arrives or the timer expires
    const timedOut = !(await condition(
      () => unprocessed.length > 0 || exitRequested,
      "10 seconds",
    ));

    // Drain and deduplicate incoming signals
    while (unprocessed.length > 0) {
      const item = unprocessed.shift()!;
      if (item.orderId === bucketKey && !seenSet.has(item.itemId)) {
        seenSet.add(item.itemId);
        items.push(item);
      }
    }

    if (timedOut || exitRequested) {
      const result = await processItems(bucketKey, items);
      log.info("Processed order batch", { bucketKey, count: items.length });
      if (unprocessed.length === 0) return result;
      // More signals arrived after timeout/exit — loop to process them
    }
  } while (unprocessed.length > 0 || !workflowInfo().continueAsNewSuggested);

  // History growing large — continue as new, carrying accumulated state forward
  log.info("Continuing as new", { bucketKey, count: items.length });
  await continueAsNew<typeof accumulatorWorkflow>(bucketKey, items, [...seenSet]);
  return ""; // unreachable — continueAsNew throws
}
