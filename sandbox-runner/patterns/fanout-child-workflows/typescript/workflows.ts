import {
  executeChild,
  log,
  proxyActivities,
  workflowInfo,
} from "@temporalio/workflow";

import type * as activities from "./activities";
import { CHUNK_SIZE, TASK_QUEUE } from "./shared";

const { processRecord } = proxyActivities<typeof activities>({
  startToCloseTimeout: "10 seconds",
});

/**
 * Child workflow: processes a contiguous slice of records [offset, offset+length).
 */
export async function recordBatchWorkflow(
  offset: number,
  length: number
): Promise<number> {
  let processed = 0;
  for (let i = offset; i < offset + length; i++) {
    await processRecord(i);
    processed++;
  }
  log.info(`Batch complete`, { offset, length, processed });
  return processed;
}

/**
 * Parent workflow: splits the total record set into chunks and fans out to
 * one child workflow per chunk.
 */
export async function fanOutWorkflow(
  totalRecords: number,
  chunkSize: number = CHUNK_SIZE
): Promise<number> {
  const parentId = workflowInfo().workflowId;
  const children: Promise<number>[] = [];

  for (let offset = 0; offset < totalRecords; offset += chunkSize) {
    const length = Math.min(chunkSize, totalRecords - offset);
    children.push(
      executeChild(recordBatchWorkflow, {
        args: [offset, length],
        taskQueue: TASK_QUEUE,
        workflowId: `${parentId}/batch-${offset}`,
      })
    );
  }

  const results = await Promise.all(children);
  const total = results.reduce((sum, n) => sum + n, 0);
  log.info(`Fan-out complete`, { totalRecords, chunks: children.length, total });
  return total;
}
