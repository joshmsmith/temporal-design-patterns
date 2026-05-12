from temporalio import activity


@activity.defn
async def charge_payment_api(order_id: str) -> str:
    info = activity.info()
    activity.logger.info(
        f"Charging payment API for order {order_id} (attempt {info.attempt})"
    )
    # The payment gateway is down — every attempt fails.
    # Change this to succeed when info.attempt == 1 to watch the happy path.
    raise RuntimeError(f"payment gateway unavailable (attempt {info.attempt})")
