import json
import urllib.request

from temporalio import activity

from shared import CallbackInput, PaymentPayload


@activity.defn
async def process_payment(payload: PaymentPayload) -> str:
    activity.logger.info(f"Processing payment {payload.payment_id} for ${payload.amount:.2f}")
    return f"Payment {payload.payment_id} processed successfully for ${payload.amount:.2f}"


@activity.defn
async def send_webhook_callback(input: CallbackInput) -> str:
    # In production this would POST to input.callback_url with input.payload as the body.
    # For this demo we skip the actual HTTP call.
    activity.logger.info(
        f"[stub] Would POST to {input.callback_url} with payload: {input.payload}"
    )
    return f"Callback to {input.callback_url} delivered (stubbed)"
