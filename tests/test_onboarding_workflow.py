from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal.models import (
    OnboardSiteInput,
    OnboardSiteResult,
    RemediationPlan,
)


def _make_plan(changes=None):
    from datetime import UTC, datetime

    return RemediationPlan(
        site_id="SITE-001",
        device_id="DEV001",
        snapshot_id="snap-001",
        changes=changes or [],
        estimated_impact="low",
        created_at=datetime.now(UTC),
    )


class TestOnboardSiteWorkflow:
    async def test_happy_path_no_changes(self):
        from temporal.models import DeviceIntent
        from temporal.workflows.onboard_site import OnboardSiteWorkflow

        @activity.defn(name="fetch_device_intent")
        async def mock_fetch(device_id: str) -> DeviceIntent:
            return DeviceIntent(
                device_id=device_id,
                hostname="rtr01",
                platform="cisco_ios_xe",
                primary_ip="10.0.1.1/30",
            )

        @activity.defn(name="write_provisioning_status")
        async def mock_write(device_id, status, wf_id) -> None:
            pass

        @activity.defn(name="discover_device_config")
        async def mock_config(device_id: str) -> str:
            return "hostname rtr01"

        @activity.defn(name="discover_device_state")
        async def mock_state(device_id: str) -> dict:
            return {"interfaces": [], "bgp_neighbors": []}

        @activity.defn(name="reconcile_nautobot_records")
        async def mock_reconcile(intent, state) -> None:
            pass

        @activity.defn(name="generate_remediation_plan")
        async def mock_plan(intent, config, state) -> RemediationPlan:
            return _make_plan()

        async with (
            await WorkflowEnvironment.start_time_skipping() as env,
            Worker(
                env.client,
                task_queue="test-onboard",
                workflows=[OnboardSiteWorkflow],
                activities=[
                    mock_fetch,
                    mock_write,
                    mock_config,
                    mock_state,
                    mock_reconcile,
                    mock_plan,
                ],
            ),
        ):
            result: OnboardSiteResult = await env.client.execute_workflow(
                OnboardSiteWorkflow.run,
                OnboardSiteInput(site_id="SITE-001", device_id="DEV001", requested_by="test"),
                id="test-onboard-001",
                task_queue="test-onboard",
            )
        assert result.success is True
        assert result.device_id == "DEV001"

    async def test_hitl_rejection_returns_failure(self):
        from temporal.models import ConfigChange, DeviceIntent
        from temporal.workflows.onboard_site import OnboardSiteWorkflow

        @activity.defn(name="fetch_device_intent")
        async def mock_fetch(device_id: str) -> DeviceIntent:
            return DeviceIntent(
                device_id=device_id,
                hostname="rtr01",
                platform="cisco_ios_xe",
                primary_ip="10.0.1.1/30",
            )

        @activity.defn(name="write_provisioning_status")
        async def mock_write(device_id, status, wf_id) -> None:
            pass

        @activity.defn(name="discover_device_config")
        async def mock_config(device_id: str) -> str:
            return "hostname wrong-name"

        @activity.defn(name="discover_device_state")
        async def mock_state(device_id: str) -> dict:
            return {"interfaces": [], "bgp_neighbors": []}

        @activity.defn(name="reconcile_nautobot_records")
        async def mock_reconcile(intent, state) -> None:
            pass

        @activity.defn(name="generate_remediation_plan")
        async def mock_plan(intent, config, state) -> RemediationPlan:
            return _make_plan(
                changes=[
                    ConfigChange(
                        section="hostname",
                        description="mismatch",
                        current="hostname wrong-name",
                        intended="hostname rtr01",
                    )
                ]
            )

        async with (
            await WorkflowEnvironment.start_time_skipping() as env,
            Worker(
                env.client,
                task_queue="test-onboard",
                workflows=[OnboardSiteWorkflow],
                activities=[
                    mock_fetch,
                    mock_write,
                    mock_config,
                    mock_state,
                    mock_reconcile,
                    mock_plan,
                ],
            ),
        ):
            handle = await env.client.start_workflow(
                OnboardSiteWorkflow.run,
                OnboardSiteInput(site_id="SITE-001", device_id="DEV001", requested_by="test"),
                id="test-onboard-002",
                task_queue="test-onboard",
            )
            await handle.signal(OnboardSiteWorkflow.approve_escalation, "rejected")
            result: OnboardSiteResult = await handle.result()
        assert result.success is False
        assert "rejected" in result.failure_reason.lower()
