import { continueAsNew, log, proxyActivities } from "@temporalio/workflow";

import type * as activities from "./activities";
import { PAGE_SIZE } from "./shared";

const { fetchPage, processRecord } = proxyActivities<typeof activities>({
  startToCloseTimeout: "10 seconds",
});

/**
 * Batch Iterator Workflow: processes PAGE_SIZE records per run, then calls
 * continueAsNew with the next offset so history stays bounded.
 */
export async function batchIteratorWorkflow(
  offset: number = 0,
  totalProcessed: number = 0
): Promise<number> {
  const page = await fetchPage(offset, PAGE_SIZE);

  for (const recordId of page) {
    await processRecord(recordId);
    totalProcessed++;
  }

  log.info(`Processed page`, { offset, pageSize: page.length, totalProcessed });

  if (page.length === PAGE_SIZE) {
    // More pages remain — continue as new with the next offset.
    await continueAsNew<typeof batchIteratorWorkflow>(offset + PAGE_SIZE, totalProcessed);
  }

  // Reached here only on the final (partial) page.
  return totalProcessed;
}
