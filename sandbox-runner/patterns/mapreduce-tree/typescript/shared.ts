export const TASK_QUEUE = "mapreduce-tree-task-queue";
export const WORKFLOW_ID_PREFIX = "mapreduce-tree";
// When a node has at most this many records, it fans out to leaf workflows.
export const LEAF_THRESHOLD = 3;
// Maximum allowed recursion depth — fail fast if exceeded.
export const MAX_DEPTH = 5;
export const RESULT_SIGNAL = "nodeResult";

// Demo record set.
export const RECORDS: string[] = Array.from({ length: 9 }, (_, i) => `item-${i}`);
