import {
  condition,
  defineSignal,
  getExternalWorkflowHandle,
  log,
  proxyActivities,
  setHandler,
  startChild,
  workflowInfo,
} from "@temporalio/workflow";

import type * as activities from "./activities";
import { LEAF_THRESHOLD, MAX_DEPTH, RESULT_SIGNAL, TASK_QUEUE } from "./shared";

const { processLeaf } = proxyActivities<typeof activities>({
  startToCloseTimeout: "30 seconds",
});

/** Signal payload sent from child to parent. */
export interface ResultPayload {
  id: string;
  results: string[];
}

export const resultSignal = defineSignal<[ResultPayload]>(RESULT_SIGNAL);

/**
 * Leaf workflow: processes one record via an Activity and signals the result
 * back to the parent Node workflow.
 */
export async function leafWorkflow(record: string, parentWorkflowId: string): Promise<void> {
  const result = await processLeaf(record);
  log.info(`Leaf processed`, { record, result });

  const parent = getExternalWorkflowHandle(parentWorkflowId);
  await parent.signal(resultSignal, { id: record, results: [result] });
}

/**
 * Node workflow: recursively splits the record set or fans out to leaf workflows,
 * collects results via signals, then signals its own result up to its parent.
 */
export async function nodeWorkflow(
  records: string[],
  depth: number = 0,
  parentWorkflowId: string = ""
): Promise<string[]> {
  if (depth > MAX_DEPTH) {
    throw new Error(`Tree depth exceeded ${MAX_DEPTH}`);
  }

  const myId = workflowInfo().workflowId;
  const collectedResults: string[] = [];
  let received = 0;
  let expected = 0;

  setHandler(resultSignal, (payload: ResultPayload) => {
    collectedResults.push(...payload.results);
    received++;
  });

  if (records.length <= LEAF_THRESHOLD) {
    // Fan out to leaf workflows — one per record.
    expected = records.length;
    for (const record of records) {
      void startChild(leafWorkflow, {
        args: [record, myId],
        workflowId: `${myId}/leaf-${record}`,
        taskQueue: TASK_QUEUE,
      });
    }
  } else {
    // Split and recurse into child node workflows.
    const mid = Math.floor(records.length / 2);
    const chunks = [records.slice(0, mid), records.slice(mid)];
    expected = chunks.length;
    for (let i = 0; i < chunks.length; i++) {
      void startChild(nodeWorkflow, {
        args: [chunks[i], depth + 1, myId],
        workflowId: `${myId}/node-d${depth + 1}-${i}`,
        taskQueue: TASK_QUEUE,
      });
    }
  }

  // Wait until all expected signals have arrived.
  await condition(() => received >= expected);

  log.info(`Node complete`, { depth, records: records.length, results: collectedResults.length });

  // Signal aggregated results up to parent (if this is not the root).
  if (parentWorkflowId) {
    const parent = getExternalWorkflowHandle(parentWorkflowId);
    await parent.signal(resultSignal, { id: myId, results: collectedResults });
  }

  return collectedResults;
}
