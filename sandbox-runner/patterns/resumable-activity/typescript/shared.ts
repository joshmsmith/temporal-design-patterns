export const TASK_QUEUE = "resumable-activity-task-queue";
export const WORKFLOW_ID_PREFIX = "resumable-activity";

export interface TransferInput {
  fromAccount: string;
  toAccount: string;
  amount: number;
}
