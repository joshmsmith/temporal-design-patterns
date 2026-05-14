from temporalio import activity

from shared import OrderItem


@activity.defn
async def process_items(order_id: str, items: list[OrderItem]) -> str:
    names = ", ".join(i.name for i in items)
    return f"Order {order_id} fulfilled with {len(items)} item(s): {names}"
