import { TOTAL_RECORDS } from "./shared";

export async function processRecord(recordId: number): Promise<void> {
  // Simulate processing work.
  await new Promise((r) => setTimeout(r, 50));
}
