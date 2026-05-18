from temporalio import activity


@activity.defn
async def call_downstream(request: str, phase: int) -> str:
    """Call a downstream service.

    phase=1 (fast): always fails — simulates a service that is completely down.
    phase=2 (slow): fails on attempts 1-2, succeeds on attempt 3 — simulates recovery.
    """
    info = activity.info()
    activity.logger.info(
        f"Calling downstream service (phase {phase}, attempt {info.attempt})"
    )

    if phase == 1:
        raise RuntimeError(
            f"service down — fast phase exhausted (attempt {info.attempt})"
        )

    # Phase 2: service is recovering — succeeds on attempt 3
    if info.attempt < 3:
        raise RuntimeError(
            f"still recovering (phase 2, attempt {info.attempt})"
        )

    return f"Success for '{request}' (phase 2, recovered on attempt {info.attempt})"
