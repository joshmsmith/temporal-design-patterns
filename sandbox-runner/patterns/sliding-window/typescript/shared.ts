export const TASK_QUEUE = "sliding-window-task-queue";
export const WORKFLOW_ID_PREFIX = "sliding-window";
export const WINDOW_SIZE = 3;
// Total records to process in the demo.
export const RECORD_IDS: string[] = Array.from({ length: 12 }, (_, i) => `record-${i}`);
export const COMPLETION_SIGNAL = "recordCompleted";

/** Input for SlidingWindowWorkflow. Bundled as a single object so all runs
 *  (including continues-as-new) share one consistent argument shape. */
export interface SlidingWindowInput {
  recordIds: string[];
  windowSize?: number;
  startIndex?: number;
  totalProcessed?: number;
  inFlight?: number;
}
