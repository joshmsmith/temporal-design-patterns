from temporalio import activity

from shared import ALERT_THRESHOLD

# Fails the first 5 attempts, then succeeds.
# Emits a metric counter on each attempt past the threshold (attempts 4 and 5).
SUCCEED_ON_ATTEMPT = 6


@activity.defn
async def call_downstream_service(endpoint: str) -> str:
    info = activity.info()
    activity.logger.info(f"Calling downstream service (attempt {info.attempt})")

    if info.attempt > ALERT_THRESHOLD:
        meter = activity.metric_meter()
        meter.create_counter(
            "high_activity_error_count",
            "Activity has exceeded the failure attempt threshold",
        ).add(1)
        activity.logger.warning(
            f"Attempt {info.attempt} exceeds threshold {ALERT_THRESHOLD} — "
            "emitted high_activity_error_count metric. "
            "In production, your alerting system would fire here."
        )

    if info.attempt < SUCCEED_ON_ATTEMPT:
        raise RuntimeError(
            f"downstream service unavailable (attempt {info.attempt})"
        )

    return f"Downstream call succeeded on attempt {info.attempt}"
