export async function processRecord(recordId: string): Promise<void> {
  // Simulate processing work.
  await new Promise((r) => setTimeout(r, 300));
}
