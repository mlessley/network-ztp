"""
BootstrapDeviceWorkflow — Day 0 orchestrator.

Handles everything from "device record created in Nautobot" to "device is
SSH-reachable and Day 1 provisioning has completed."

Step sequence:
  1. Register DHCP reservation  — device's MAC gets the right IP + Option 67
                                   before it ever powers on.
  2. Fetch device intent         — pull hostname, mgmt IP, gateway from Nautobot.
  3. Render bootstrap script     — minimal IOS-XE ZTP Python script.
  4. Publish bootstrap script    — write to HTTP file server at the Option 67 URL.
  5. Wait for device reachability — park until SSH responds (up to 8 hours).
  6. Trigger Day 1 as child workflow — hand off to ProvisionSiteWorkflow.

The 8-hour reachability timeout covers the worst-case field scenario: a
device ships to a branch, sits on a loading dock, gets racked at end of
business day, and is cabled by the overnight team.  If the timeout expires
the workflow marks FAILED and alerts operations — the device never came online.

Child workflow coupling:
  BootstrapDeviceWorkflow owns Day 0 and delegates Day 1 to
  ProvisionSiteWorkflow as a child workflow.  This keeps the two phases
  independently observable in the Temporal UI and independently retryable.
  A failed Day 1 does not require re-running Day 0.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from temporal.activities.bootstrap_activities import (
        publish_bootstrap_script,
        register_dhcp_reservation,
        render_bootstrap_script,
        wait_for_device_reachability,
    )
    from temporal.activities.nautobot_activities import (
        fetch_device_intent,
        write_provisioning_status,
    )
    from temporal.metrics import workflow_completed, workflow_started
    from temporal.models import (
        BootstrapDeviceInput,
        BootstrapDeviceResult,
        ProvisioningStatus,
        ProvisionSiteInput,
    )
    from temporal.workflows.provision_site import ProvisionSiteWorkflow

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = timedelta(minutes=5)

_RETRY_STANDARD = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=1.5,
)

_RETRY_WRITE_STATUS = RetryPolicy(
    maximum_attempts=5,
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=1.5,
)

# Reachability polling can wait a long time for a field engineer to rack
# the device — this is the activity-level timeout, not the poll interval.
_REACHABILITY_TIMEOUT = timedelta(hours=8)


@workflow.defn
class BootstrapDeviceWorkflow:
    """
    Orchestrate Day 0 bootstrap for a single device.

    Triggered when a device record is created in Nautobot (via webhook or CLI).
    Completes when Day 1 provisioning has also finished — the caller gets a
    single result covering the full lifecycle from unboxing to operational.
    """

    @workflow.run
    async def run(self, input: BootstrapDeviceInput) -> BootstrapDeviceResult:
        """
        Execute the Day 0 bootstrap sequence and hand off to Day 1.

        Args:
            input: Bootstrap trigger containing device_id, MAC address, and requester.

        Returns:
            BootstrapDeviceResult reflecting the combined Day 0 + Day 1 outcome.
        """
        workflow_id = workflow.info().workflow_id
        device_id = input.device_id
        workflow_started.labels(phase="day0").inc()

        workflow.logger.info(
            "BootstrapDeviceWorkflow started: device_id=%s mac=%s requested_by=%s",
            device_id,
            input.mac_address,
            input.requested_by,
        )

        try:
            # ----------------------------------------------------------------
            # Step 1 — fetch intent (need hostname + mgmt IP for DHCP + script)
            # ----------------------------------------------------------------
            await self._write_status(device_id, ProvisioningStatus.BOOTSTRAP_STARTED, workflow_id)

            intent = await workflow.execute_activity(
                fetch_device_intent,
                device_id,
                start_to_close_timeout=_DEFAULT_TIMEOUT,
                retry_policy=_RETRY_STANDARD,
            )
            workflow.logger.info("Device intent fetched: hostname=%s", intent.hostname)

            # ----------------------------------------------------------------
            # Step 2 — register DHCP reservation
            # ----------------------------------------------------------------
            await workflow.execute_activity(
                register_dhcp_reservation,
                args=[input.mac_address, device_id, intent.hostname],
                start_to_close_timeout=_DEFAULT_TIMEOUT,
                retry_policy=_RETRY_STANDARD,
            )
            await self._write_status(device_id, ProvisioningStatus.DHCP_RESERVED, workflow_id)
            workflow.logger.info(
                "DHCP reservation registered: mac=%s device_id=%s", input.mac_address, device_id
            )

            # ----------------------------------------------------------------
            # Step 3 — render bootstrap script
            # ----------------------------------------------------------------
            script = await workflow.execute_activity(
                render_bootstrap_script,
                intent,
                start_to_close_timeout=_DEFAULT_TIMEOUT,
                retry_policy=_RETRY_STANDARD,
            )

            # ----------------------------------------------------------------
            # Step 4 — publish to HTTP file server
            # ----------------------------------------------------------------
            await workflow.execute_activity(
                publish_bootstrap_script,
                script,
                start_to_close_timeout=_DEFAULT_TIMEOUT,
                retry_policy=_RETRY_STANDARD,
            )
            await self._write_status(
                device_id, ProvisioningStatus.BOOTSTRAP_SCRIPT_READY, workflow_id
            )
            workflow.logger.info(
                "Bootstrap script published: url=%s device_id=%s", script.script_url, device_id
            )

            # ----------------------------------------------------------------
            # Step 5 — wait for device to come online
            # Device can now be physically racked and cabled.  When it powers
            # on it will DHCP, fetch the script, run it, and become reachable.
            # ----------------------------------------------------------------
            await self._write_status(
                device_id, ProvisioningStatus.AWAITING_DEVICE_CHECKIN, workflow_id
            )
            workflow.logger.info(
                "Waiting for device to come online: device_id=%s mgmt_ip=%s (timeout=8h)",
                device_id,
                intent.primary_ip,
            )

            mgmt_ip = intent.primary_ip.split("/")[0]
            await workflow.execute_activity(
                wait_for_device_reachability,
                args=[device_id, mgmt_ip],
                start_to_close_timeout=_REACHABILITY_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=1),  # timeout IS the retry strategy here
            )

            await self._write_status(device_id, ProvisioningStatus.BOOTSTRAP_COMPLETE, workflow_id)
            workflow.logger.info("Device online — handing off to Day 1: device_id=%s", device_id)

            # ----------------------------------------------------------------
            # Step 6 — Day 1 as child workflow
            # Using execute_child_workflow means the Day 1 run is independently
            # visible in the Temporal UI, has its own history, and can be
            # retried separately without re-running Day 0.
            # ----------------------------------------------------------------
            day1_result = await workflow.execute_child_workflow(
                ProvisionSiteWorkflow.run,
                ProvisionSiteInput(
                    device_id=device_id,
                    requested_by=f"bootstrap:{input.requested_by}",
                ),
                id=f"day1-{device_id}-{workflow_id}",
                task_queue=workflow.info().task_queue,
            )

            if not day1_result.success:
                raise ApplicationError(
                    f"Day 1 provisioning failed for device_id={device_id}: "
                    f"{day1_result.failure_reason}",
                    non_retryable=True,
                )

            workflow.logger.info(
                "Day 0+1 complete: device_id=%s day1_workflow=%s",
                device_id,
                day1_result.workflow_id,
            )
            workflow_completed.labels(phase="day0", status="success").inc()
            return BootstrapDeviceResult(
                device_id=device_id,
                success=True,
                workflow_id=workflow_id,
            )

        except (ApplicationError, ActivityError) as exc:
            workflow.logger.error("Bootstrap failed for device_id=%s: %s", device_id, exc)
            await self._write_status(device_id, ProvisioningStatus.FAILED, workflow_id)
            workflow_completed.labels(phase="day0", status="failure").inc()
            return BootstrapDeviceResult(
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
        await workflow.execute_activity(
            write_provisioning_status,
            args=[device_id, status.value, workflow_id],
            start_to_close_timeout=_DEFAULT_TIMEOUT,
            retry_policy=_RETRY_WRITE_STATUS,
        )
