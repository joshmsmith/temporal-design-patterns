export async function processLeaf(record: string): Promise<string> {
  // Simulate processing and return a result string.
  await new Promise((r) => setTimeout(r, 50));
  return `processed(${record})`;
}
