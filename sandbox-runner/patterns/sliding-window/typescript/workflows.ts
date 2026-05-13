import {
  ApplicationFailure,
  ParentClosePolicy,
  condition,
  continueAsNew,
  defineSignal,
  getExternalWorkflowHandle,
  log,
  proxyActivities,
  setHandler,
  startChild,
  workflowInfo,
} from "@temporalio/workflow";

import type * as activities from "./activities";
import { COMPLETION_SIGNAL, TASK_QUEUE, WINDOW_SIZE, type SlidingWindowInput } from "./shared";

const { processRecord } = proxyActivities<typeof activities>({
  startToCloseTimeout: "30 seconds",
});

export const completionSignal = defineSignal<[string]>(COMPLETION_SIGNAL);

/**
 * Child workflow: processes one record and signals the parent on completion.
 * The parent's workflow ID is stable across continueAsNew runs.
 */
export async function recordProcessorWorkflow(
  recordId: string,
  parentWorkflowId: string
): Promise<void> {
  await processRecord(recordId);
  log.info(`Processed record`, { recordId });

  // Signal the parent that this slot is now free.
  // Ignore if the parent has already completed (final run finished before us).
  try {
    const parent = getExternalWorkflowHandle(parentWorkflowId);
    await parent.signal(completionSignal, recordId);
  } catch (err) {
    if (err instanceof ApplicationFailure && err.type === 'NOT_FOUND') {
      log.info('Parent already completed, signal not needed', { recordId });
    } else {
      throw err;
    }
  }
}

/**
 * Parent workflow: maintains a fixed window of concurrent child workflows.
 * Calls continueAsNew after dispatching windowSize children so history stays bounded.
 * Children signal back to free a slot; the parent starts the next child immediately.
 */
export async function slidingWindowWorkflow(input: SlidingWindowInput): Promise<number> {
  const {
    recordIds,
    windowSize = WINDOW_SIZE,
    startIndex = 0,
    inFlight = 0,
  } = input;
  let totalProcessed = input.totalProcessed ?? 0;
  const parentId = workflowInfo().workflowId;
  let pendingSignals = 0;
  let dispatched = 0;

  setHandler(completionSignal, (_recordId: string) => {
    pendingSignals++;
    totalProcessed++;
  });

  // Only start (windowSize - inFlight) new children. The carried-over in-flight
  // children from the previous run will signal us when they complete.
  const newFill = Math.min(windowSize - inFlight, recordIds.length - startIndex);
  let nextIndex = startIndex;
  let active = inFlight;

  for (let i = 0; i < newFill; i++) {
    void startChild(recordProcessorWorkflow, {
      args: [recordIds[nextIndex], parentId],
      workflowId: `${parentId}/record-${recordIds[nextIndex]}`,
      taskQueue: TASK_QUEUE,
      parentClosePolicy: ParentClosePolicy.ABANDON,
    });
    nextIndex++;
    dispatched++;
    active++;
  }

  // If the window is full after the initial fill, continue-as-new immediately
  // so the parent doesn't wait before handing off to the next run.
  if (dispatched >= windowSize) {
    log.info(`ContinueAsNew`, { nextIndex, totalProcessed });
    await continueAsNew<typeof slidingWindowWorkflow>({ recordIds, windowSize, startIndex: nextIndex, totalProcessed, inFlight: windowSize });
    return;
  }

  // Slide the window: as each slot frees, start the next child.
  while (nextIndex < recordIds.length) {
    await condition(() => pendingSignals > 0);
    pendingSignals--;
    active--;
    void startChild(recordProcessorWorkflow, {
      args: [recordIds[nextIndex], parentId],
      workflowId: `${parentId}/record-${recordIds[nextIndex]}`,
      taskQueue: TASK_QUEUE,
      parentClosePolicy: ParentClosePolicy.ABANDON,
    });
    nextIndex++;
    dispatched++;
    active++;

    if (dispatched >= windowSize) {
      log.info(`ContinueAsNew`, { nextIndex, totalProcessed });
      // Pass nextIndex as the next unstarted record; inFlight=windowSize because
      // the window is always full at CAN time.
      await continueAsNew<typeof slidingWindowWorkflow>({ recordIds, windowSize, startIndex: nextIndex, totalProcessed, inFlight: windowSize });
      return;
    }
  }

  // Wait for all remaining in-flight children to complete.
  await condition(() => pendingSignals >= active);
  log.info(`Sliding window complete`, { total: recordIds.length, totalProcessed });
  return totalProcessed;
}
