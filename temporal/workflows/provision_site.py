"""
ProvisionSiteWorkflow — durable orchestrator for a single site router ZTP run.

Architecture decisions encoded here:

ROLL-FORWARD PHILOSOPHY
    This workflow never pushes a "rollback" config to a device.  Nautobot is
    the sole source of intent — the only correct response to a failed or
    rejected provisioning run is to fix the intent in Nautobot and submit a
    new workflow execution.  Pushing an archived "previous good config" would
    be unsafe because:

      1. The device's live state may have changed since that config was captured
         (BGP sessions, DHCP leases, spanning tree).  The old config is not
         guaranteed to be safe in the current context.
      2. It would put the device in a state that Nautobot does not describe,
         deliberately creating the drift this pipeline exists to eliminate.
      3. In a manually-executed maintenance window an engineer watches for these
         interactions; in continuous automated provisioning at scale there is no
         such window.

    On failure the device is left in whatever state the push reached.  The
    FAILED status in Nautobot flags it for engineer attention.  Recovery is
    always a forward change: update Nautobot intent, re-run provisioning.

HUMAN-IN-THE-LOOP (HITL)
    When post-push validation detects drift, the workflow parks itself waiting
    for an ``approve_escalation`` signal.  The 24-hour timeout accommodates
    enterprise change processes that may span time zones or business-hours-only
    review queues.  The full audit trail (who signalled, when, what the drift
    was) is preserved in Temporal workflow history.

    Signal decisions:
      ``approved`` — the detected drift is acceptable (e.g. a known emergency
                     out-of-band change).  Mark COMPLETE and move on.
      ``rejected`` — the drift is not acceptable.  Mark FAILED.  The engineer
                     must fix Nautobot intent and submit a new provisioning run.

RETRY POLICIES
    Policies are tuned per activity based on real-world failure modes:
    - Nautobot fetches: short backoff, few retries — CMDB is normally reliable.
    - Config push: longer backoff with exponential growth — device reachability
      after a WAN flap needs time to recover.
    - Status writes: many retries, short backoff — PATCH is cheap and must not
      be dropped; we accept at-least-once semantics here.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    # Imports that contain non-deterministic code must be guarded so the
    # Temporal workflow sandbox does not flag them during replay.
    from temporal.activities.ansible_activities import push_config, render_config
    from temporal.activities.nautobot_activities import (
        fetch_device_intent,
        write_provisioning_status,
    )
    from temporal.activities.validation_activities import validate_device_state
    from temporal.metrics import drift_detected, hitl_pending, workflow_completed, workflow_started
    from temporal.models import (
        DeviceIntent,
        ProvisioningStatus,
        ProvisionSiteInput,
        ProvisionSiteResult,
        RenderedConfig,
    )

logger = logging.getLogger(__name__)

_DEFAULT_ACTIVITY_TIMEOUT = timedelta(minutes=5)

_RETRY_FETCH_INTENT = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=1.5,
)

_RETRY_PUSH_CONFIG = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
)

_RETRY_VALIDATE = RetryPolicy(
    maximum_attempts=2,
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=1.5,
)

_RETRY_WRITE_STATUS = RetryPolicy(
    maximum_attempts=5,
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=1.5,
)


@workflow.defn
class ProvisionSiteWorkflow:
    """
    Orchestrate zero-touch provisioning for a single site router.

    Step sequence:
      fetch_intent → render_config → push_config → validate → [HITL?] → done

    The workflow is idempotent: re-running with the same device_id will produce
    the same outcome as long as Nautobot's intent hasn't changed between runs.
    Temporal's workflow ID deduplication prevents accidental concurrent runs
    for the same device.
    """

    def __init__(self) -> None:
        # Signal buffer: the approve_escalation signal writes here; the main
        # coroutine drains it via workflow.wait_condition.
        self._approval_decision: str | None = None

    @workflow.signal
    async def approve_escalation(self, decision: str) -> None:
        """
        Receive a human approval or rejection for a drift-detected escalation.

        Args:
            decision: ``"approved"`` to accept the drift and mark COMPLETE;
                      ``"rejected"`` to mark FAILED and leave recovery to a
                      new provisioning run after Nautobot intent is corrected.
        """
        workflow.logger.info(
            "Received approve_escalation signal: decision=%s workflow_id=%s",
            decision,
            workflow.info().workflow_id,
        )
        self._approval_decision = decision

    @workflow.run
    async def run(self, input: ProvisionSiteInput) -> ProvisionSiteResult:
        """
        Execute the full ZTP provisioning sequence for one device.

        Args:
            input: Trigger record containing the device_id and requester identity.

        Returns:
            ProvisionSiteResult with final success/failure status.
        """
        workflow_id = workflow.info().workflow_id
        device_id = input.device_id
        workflow_started.labels(phase="day1").inc()

        workflow.logger.info(
            "ProvisionSiteWorkflow started: device_id=%s requested_by=%s workflow_id=%s",
            device_id,
            input.requested_by,
            workflow_id,
        )

        try:
            # ----------------------------------------------------------------
            # Step 1 — announce start
            # ----------------------------------------------------------------
            await self._write_status(
                device_id, ProvisioningStatus.PROVISIONING_STARTED, workflow_id
            )

            # ----------------------------------------------------------------
            # Step 2 — fetch intent from Nautobot
            # ----------------------------------------------------------------
            intent: DeviceIntent = await workflow.execute_activity(
                fetch_device_intent,
                device_id,
                start_to_close_timeout=_DEFAULT_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_FETCH_INTENT,
            )
            workflow.logger.info("Device intent fetched: hostname=%s", intent.hostname)

            # ----------------------------------------------------------------
            # Step 3 — render configuration
            # ----------------------------------------------------------------
            rendered: RenderedConfig = await workflow.execute_activity(
                render_config,
                intent,
                start_to_close_timeout=_DEFAULT_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_FETCH_INTENT,
            )
            await self._write_status(device_id, ProvisioningStatus.CONFIG_RENDERED, workflow_id)
            workflow.logger.info(
                "Config rendered: template=%s device_id=%s",
                rendered.template_name,
                device_id,
            )

            # ----------------------------------------------------------------
            # Step 4 — push configuration
            # ----------------------------------------------------------------
            push_result = await workflow.execute_activity(
                push_config,
                rendered,
                start_to_close_timeout=timedelta(minutes=15),
                retry_policy=_RETRY_PUSH_CONFIG,
            )

            if not push_result.success:
                raise ApplicationError(
                    f"Config push failed for device_id={device_id}: {push_result.output}",
                    non_retryable=True,
                )

            await self._write_status(device_id, ProvisioningStatus.CONFIG_PUSHED, workflow_id)
            workflow.logger.info(
                "Config pushed successfully: device_id=%s duration=%.2fs",
                device_id,
                push_result.duration_seconds,
            )

            # ----------------------------------------------------------------
            # Step 5 — post-push validation
            # ----------------------------------------------------------------
            validation = await workflow.execute_activity(
                validate_device_state,
                args=[device_id, intent],
                start_to_close_timeout=_DEFAULT_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_VALIDATE,
            )

            if validation.passed:
                await self._write_status(
                    device_id, ProvisioningStatus.VALIDATION_PASSED, workflow_id
                )
                workflow.logger.info("Validation passed — provisioning complete for %s", device_id)
                await self._write_status(device_id, ProvisioningStatus.COMPLETE, workflow_id)
                workflow_completed.labels(phase="day1", status="success").inc()
                return ProvisionSiteResult(
                    device_id=device_id,
                    success=True,
                    workflow_id=workflow_id,
                )

            # ----------------------------------------------------------------
            # Step 6 — HITL escalation (drift detected)
            # ----------------------------------------------------------------
            workflow.logger.warning(
                "Validation drift detected for device_id=%s: %s — escalating to human",
                device_id,
                validation.drift_detected,
            )
            await self._write_status(
                device_id, ProvisioningStatus.AWAITING_HUMAN_APPROVAL, workflow_id
            )

            drift_detected.labels(site_id=device_id).inc()
            hitl_pending.inc()
            # Park here until a signal arrives or the 24-hour SLA expires.
            # wait_condition returns True if condition met, False on timeout.
            condition_met: bool = await workflow.wait_condition(  # type: ignore[func-returns-value, assignment]
                lambda: self._approval_decision is not None,
                timeout=timedelta(hours=24),
            )
            hitl_pending.dec()

            if not condition_met:
                raise ApplicationError(
                    f"HITL escalation timed out for device_id={device_id} — "
                    "no operator decision received within 24 hours",
                    non_retryable=True,
                )

            decision = self._approval_decision
            workflow.logger.info(
                "HITL decision received: device_id=%s decision=%s", device_id, decision
            )

            if decision == "approved":
                await self._write_status(device_id, ProvisioningStatus.COMPLETE, workflow_id)
                workflow_completed.labels(phase="day1", status="success").inc()
                return ProvisionSiteResult(
                    device_id=device_id,
                    success=True,
                    workflow_id=workflow_id,
                )

            # Operator rejected the drift.  The device stays in its current
            # state.  Recovery is a forward action: fix Nautobot intent and
            # submit a new provisioning run.
            raise ApplicationError(
                f"Operator rejected provisioning for device_id={device_id} "
                f"due to drift: {validation.drift_detected}. "
                "Correct Nautobot intent and submit a new provisioning run.",
                non_retryable=True,
            )

        except (ApplicationError, ActivityError) as exc:
            workflow.logger.error("Provisioning failed for device_id=%s: %s", device_id, exc)
            await self._write_status(device_id, ProvisioningStatus.FAILED, workflow_id)
            workflow_completed.labels(phase="day1", status="failure").inc()
            return ProvisionSiteResult(
                device_id=device_id,
                success=False,
                workflow_id=workflow_id,
                failure_reason=str(exc),
            )

    async def _write_status(
        self,
        device_id: str,
        status: ProvisioningStatus,
        workflow_id: str,
    ) -> None:
        """Write a lifecycle status to Nautobot with a generous retry policy."""
        await workflow.execute_activity(
            write_provisioning_status,
            args=[device_id, status.value, workflow_id],
            start_to_close_timeout=_DEFAULT_ACTIVITY_TIMEOUT,
            retry_policy=_RETRY_WRITE_STATUS,
        )
