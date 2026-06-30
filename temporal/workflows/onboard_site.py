from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporal.activities.ansible_activities import push_config
    from temporal.activities.nautobot_activities import (
        fetch_device_intent,
        write_provisioning_status,
    )
    from temporal.activities.onboarding_activities import (
        discover_device_config,
        discover_device_state,
        generate_remediation_plan,
        reconcile_nautobot_records,
    )
    from temporal.models import (
        OnboardSiteInput,
        OnboardSiteResult,
        ProvisioningStatus,
        RemediationPlan,
        RenderedConfig,
    )

_RETRY = RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=5))


@workflow.defn
class OnboardSiteWorkflow:
    def __init__(self) -> None:
        self._approval_decision: str | None = None

    @workflow.signal
    async def approve_escalation(self, decision: str) -> None:
        self._approval_decision = decision

    @workflow.run
    async def run(self, inp: OnboardSiteInput) -> OnboardSiteResult:
        log = workflow.logger

        await workflow.execute_activity(
            write_provisioning_status,
            args=[inp.device_id, ProvisioningStatus.ONBOARD_PENDING, workflow.info().workflow_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        intent = await workflow.execute_activity(
            fetch_device_intent,
            args=[inp.device_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        await workflow.execute_activity(
            write_provisioning_status,
            args=[
                inp.device_id,
                ProvisioningStatus.ONBOARD_DISCOVERING,
                workflow.info().workflow_id,
            ],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        live_config = await workflow.execute_activity(
            discover_device_config,
            args=[inp.device_id],
            start_to_close_timeout=timedelta(hours=1),
            retry_policy=_RETRY,
        )
        discovered_state = await workflow.execute_activity(
            discover_device_state,
            args=[inp.device_id],
            start_to_close_timeout=timedelta(hours=1),
            retry_policy=_RETRY,
        )

        await workflow.execute_activity(
            write_provisioning_status,
            args=[
                inp.device_id,
                ProvisioningStatus.ONBOARD_DISCOVERED,
                workflow.info().workflow_id,
            ],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        await workflow.execute_activity(
            reconcile_nautobot_records,
            args=[intent, discovered_state],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=_RETRY,
        )

        plan: RemediationPlan = await workflow.execute_activity(
            generate_remediation_plan,
            args=[intent, live_config, discovered_state],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=_RETRY,
        )

        if plan.changes:
            log.info(
                "onboard_site.hitl_required", device_id=inp.device_id, changes=len(plan.changes)
            )
            condition_met: bool = await workflow.wait_condition(  # type: ignore[func-returns-value, assignment]
                lambda: self._approval_decision is not None,
                timeout=timedelta(hours=24),
            )
            if not condition_met or self._approval_decision != "approved":
                await workflow.execute_activity(
                    write_provisioning_status,
                    args=[inp.device_id, ProvisioningStatus.FAILED, workflow.info().workflow_id],
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=_RETRY,
                )
                return OnboardSiteResult(
                    site_id=inp.site_id,
                    device_id=inp.device_id,
                    success=False,
                    workflow_id=workflow.info().workflow_id,
                    failure_reason="Remediation rejected or timed out",
                )

        await workflow.execute_activity(
            write_provisioning_status,
            args=[
                inp.device_id,
                ProvisioningStatus.ONBOARD_RECONCILING,
                workflow.info().workflow_id,
            ],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        if plan.changes:
            rendered = RenderedConfig(
                device_id=inp.device_id,
                config_content="\n".join(c.intended for c in plan.changes),
                template_name="remediation",
            )
            await workflow.execute_activity(
                push_config,
                args=[rendered],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=5)),
            )

        await workflow.execute_activity(
            write_provisioning_status,
            args=[inp.device_id, ProvisioningStatus.ONBOARD_MANAGED, workflow.info().workflow_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )
        return OnboardSiteResult(
            site_id=inp.site_id,
            device_id=inp.device_id,
            success=True,
            workflow_id=workflow.info().workflow_id,
        )
