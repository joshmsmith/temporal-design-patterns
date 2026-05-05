from temporalio import activity

from shared import PaymentPayload


@activity.defn
async def process_payment(payload: PaymentPayload) -> str:
    activity.logger.info(f"Processing payment {payload.payment_id} for ${payload.amount:.2f}")
    return f"Payment {payload.payment_id} processed successfully for ${payload.amount:.2f}"
