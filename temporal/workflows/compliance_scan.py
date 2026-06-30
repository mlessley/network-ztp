"""
ComplianceScanWorkflow — Day 2 drift detection across a site.

Periodically re-validates every device at a site against its current Nautobot
intent.  Any device whose running state has diverged is reported as drifted.

Design choices:

SCHEDULED EXECUTION
    This workflow is designed to run on a Temporal schedule (cron).  The
    recommended cadence is every 4–6 hours so drift is caught within the same
    business day it occurs.  The schedule is registered separately via the CLI
    or the Temporal UI — see the README for the setup command.

SEQUENTIAL DEVICE VALIDATION
    Devices are validated one at a time within a single workflow execution
    rather than fan-out child workflows.  This keeps the implementation simple
    and avoids flooding devices with concurrent SSH connections.  For sites
    with hundreds of devices, the pattern to adopt is fan-out: one child
    ComplianceScanWorkflow per batch of N devices, orchestrated by a parent
    workflow.  That extension is noted in the README but not implemented here.

REMEDIATION POLICY
    The scan reports drift but does not auto-trigger remediation.  Automatic
    re-provisioning on drift detection is a high-risk default in production —
    a misconfigured Nautobot intent record would reprovision every device in
    the site.  The safer default is: surface the report, let an engineer decide
    whether to trigger ProvisionSiteWorkflow for each drifted device.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from temporal.activities.nautobot_activities import (
        fetch_device_intent,
        fetch_site_devices,
        write_provisioning_status,
    )
    from temporal.activities.validation_activities import validate_device_state
    from temporal.metrics import drift_detected, workflow_completed, workflow_started
    from temporal.models import (
        ComplianceScanInput,
        ComplianceScanResult,
        DeviceComplianceResult,
        ProvisioningStatus,
    )

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


@workflow.defn
class ComplianceScanWorkflow:
    """
    Validate all devices at a site against current Nautobot intent.

    Returns a ComplianceScanResult summarising which devices are converged
    and which have drifted.  Drifted devices should be re-provisioned via
    a separate ProvisionSiteWorkflow execution after the engineer has
    confirmed whether the drift is unexpected or a known out-of-band change.
    """

    @workflow.run
    async def run(self, input: ComplianceScanInput) -> ComplianceScanResult:
        """
        Execute a compliance scan for a site.

        Args:
            input: Scan parameters — site ID and optional explicit device list.

        Returns:
            ComplianceScanResult with per-device pass/drift breakdown.
        """
        workflow_id = workflow.info().workflow_id
        workflow_started.labels(phase="day2").inc()

        workflow.logger.info(
            "ComplianceScanWorkflow started: site_id=%s requested_by=%s workflow_id=%s",
            input.site_id,
            input.requested_by,
            workflow_id,
        )

        # ----------------------------------------------------------------
        # Resolve device list — use explicit list if provided, else fetch
        # all devices for the site from Nautobot.
        # ----------------------------------------------------------------
        if input.device_ids:
            device_ids = input.device_ids
            workflow.logger.info("Using explicit device list: %d devices", len(device_ids))
        else:
            device_ids = await workflow.execute_activity(
                fetch_site_devices,
                input.site_id,
                start_to_close_timeout=_DEFAULT_TIMEOUT,
                retry_policy=_RETRY_STANDARD,
            )
            workflow.logger.info(
                "Fetched %d devices for site_id=%s", len(device_ids), input.site_id
            )

        results: list[DeviceComplianceResult] = []

        # ----------------------------------------------------------------
        # Validate each device in turn.
        # Failures on a single device are caught and recorded so one
        # unreachable device does not abort the scan for the whole site.
        # ----------------------------------------------------------------
        for device_id in device_ids:
            try:
                intent = await workflow.execute_activity(
                    fetch_device_intent,
                    device_id,
                    start_to_close_timeout=_DEFAULT_TIMEOUT,
                    retry_policy=_RETRY_STANDARD,
                )

                validation = await workflow.execute_activity(
                    validate_device_state,
                    args=[device_id, intent],
                    start_to_close_timeout=_DEFAULT_TIMEOUT,
                    retry_policy=_RETRY_STANDARD,
                )

                results.append(
                    DeviceComplianceResult(
                        device_id=device_id,
                        hostname=intent.hostname,
                        passed=validation.passed,
                        drift_detected=validation.drift_detected,
                    )
                )

                status = (
                    ProvisioningStatus.COMPLIANCE_PASSED
                    if validation.passed
                    else ProvisioningStatus.COMPLIANCE_DRIFTED
                )
                await self._write_status(device_id, status, workflow_id)

                if validation.passed:
                    workflow.logger.info("Compliance PASSED: device_id=%s", device_id)
                else:
                    drift_detected.labels(site_id=input.site_id).inc()
                    workflow.logger.warning(
                        "Compliance DRIFTED: device_id=%s drift=%s",
                        device_id,
                        validation.drift_detected,
                    )

            except (ApplicationError, ActivityError) as exc:
                workflow.logger.error(
                    "Compliance scan failed for device_id=%s: %s — skipping", device_id, exc
                )
                results.append(
                    DeviceComplianceResult(
                        device_id=device_id,
                        hostname=device_id,
                        passed=False,
                        drift_detected=[f"scan error: {exc}"],
                    )
                )

        # ----------------------------------------------------------------
        # Aggregate and return
        # ----------------------------------------------------------------
        passed = [r for r in results if r.passed]
        drifted = [r for r in results if not r.passed]

        workflow.logger.info(
            "Compliance scan complete: site_id=%s total=%d passed=%d drifted=%d",
            input.site_id,
            len(results),
            len(passed),
            len(drifted),
        )

        workflow_completed.labels(phase="day2", status="success").inc()
        return ComplianceScanResult(
            site_id=input.site_id,
            workflow_id=workflow_id,
            total_devices=len(results),
            passed_count=len(passed),
            drifted_count=len(drifted),
            drifted_devices=drifted,
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
