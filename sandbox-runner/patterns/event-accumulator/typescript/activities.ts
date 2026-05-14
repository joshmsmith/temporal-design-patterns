import type { OrderItem } from "./shared";

export async function processItems(orderId: string, items: OrderItem[]): Promise<string> {
  const names = items.map((i) => i.name).join(", ");
  return `Order ${orderId} fulfilled with ${items.length} item(s): ${names}`;
}
