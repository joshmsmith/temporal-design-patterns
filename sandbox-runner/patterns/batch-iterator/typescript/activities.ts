import { TOTAL_RECORDS } from "./shared";

export async function fetchPage(offset: number, pageSize: number): Promise<number[]> {
  const end = Math.min(offset + pageSize, TOTAL_RECORDS);
  const page: number[] = [];
  for (let i = offset; i < end; i++) {
    page.push(i);
  }
  return page;
}

export async function processRecord(recordId: number): Promise<void> {
  // Simulate processing work.
  await new Promise((r) => setTimeout(r, 50));
}
