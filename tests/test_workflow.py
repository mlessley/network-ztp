"""
Workflow-level tests using Temporal's time-skipping test environment.

WorkflowEnvironment.start_time_skipping() runs workflows against a local
in-process Temporal server that auto-advances the clock — so the 24-hour HITL
timeout resolves in milliseconds.  Activities are replaced by mock
implementations that return controlled values, keeping tests deterministic.

Mock activity naming convention:
    @activity.defn(name="original_activity_name")
    async def mock_<activity_name>(...) -> ReturnType: ...

The `name=` parameter is required: Temporal matches activities by name, so the
mock must register under the same name as the real implementation.

Test philosophy:
  - Test workflow *orchestration* (sequencing, signals, child-workflow launch),
    not activity *correctness* (covered in test_activities.py).
  - Inject failures by raising ApplicationError in mock activities to verify
    retry logic and failure propagation.
"""

from __future__ import annotations

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal.models import (
    BootstrapScript,
    ComplianceScanInput,
    DeviceIntent,
    DhcpReservation,
    InterfaceIntent,
    ProvisionSiteInput,
    ProvisionSiteResult,
    PushResult,
    RenderedConfig,
    ValidationResult,
    VlanIntent,
)
from temporal.workflows.compliance_scan import ComplianceScanWorkflow
from temporal.workflows.provision_site import ProvisionSiteWorkflow

TASK_QUEUE = "test-ztp-queue"


# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------


def _make_intent(device_id: str = "DEV001") -> DeviceIntent:

    return DeviceIntent(
        device_id=device_id,
        hostname=f"br-{device_id.lower()}-rtr01",
        platform="cisco_ios_xe",
        primary_ip="10.100.255.1/32",
        interfaces=[
            InterfaceIntent(
                name="GigabitEthernet0/0/0",
                description="WAN",
                ip_address="203.0.113.2/30",
            ),
            InterfaceIntent(name="Loopback0", ip_address="10.100.255.1/32"),
        ],
        vlans=[VlanIntent(vlan_id=100, name="CORP-DATA")],
        bgp_asn=65001,
        bgp_peer_ip="203.0.113.1",
        bgp_peer_asn=64512,
        ntp_servers=["10.0.0.1"],
        syslog_servers=["10.0.1.100"],
        default_gateway="10.100.255.254",
    )


def _make_rendered_config(device_id: str = "DEV001") -> RenderedConfig:
    from datetime import UTC, datetime

    return RenderedConfig(
        device_id=device_id,
        config_content=f"hostname br-{device_id.lower()}-rtr01\n",
        template_name="ios_xe_branch_router.j2",
        rendered_at=datetime.now(tz=UTC),
    )


# ---------------------------------------------------------------------------
# Mock activities — ProvisionSiteWorkflow (Day 1)
# ---------------------------------------------------------------------------


@activity.defn(name="fetch_device_intent")
async def mock_fetch_device_intent(device_id: str) -> DeviceIntent:
    return _make_intent(device_id)


@activity.defn(name="write_provisioning_status")
async def mock_write_provisioning_status(device_id: str, status: str, workflow_id: str) -> None:
    pass


@activity.defn(name="render_config")
async def mock_render_config(intent: DeviceIntent) -> RenderedConfig:
    return _make_rendered_config(intent.device_id)


@activity.defn(name="push_config")
async def mock_push_config(config: RenderedConfig) -> PushResult:
    return PushResult(
        device_id=config.device_id,
        success=True,
        output="PLAY RECAP: ok=2 changed=1 unreachable=0 failed=0",
        duration_seconds=1.0,
    )


@activity.defn(name="validate_device_state")
async def mock_validate_device_state(device_id: str, expected: DeviceIntent) -> ValidationResult:
    return ValidationResult(device_id=device_id, passed=True, drift_detected=[])


# ---------------------------------------------------------------------------
# Mock activities — BootstrapDeviceWorkflow (Day 0)
# ---------------------------------------------------------------------------


@activity.defn(name="register_dhcp_reservation")
async def mock_register_dhcp_reservation(
    mac_address: str, device_id: str, hostname: str
) -> DhcpReservation:
    return DhcpReservation(
        device_id=device_id,
        mac_address=mac_address,
        assigned_ip="10.100.255.1",
        lease_seconds=3600,
    )


@activity.defn(name="render_bootstrap_script")
async def mock_render_bootstrap_script(intent: DeviceIntent) -> BootstrapScript:
    return BootstrapScript(
        device_id=intent.device_id,
        script_content="# mock script\n",
        script_url=f"http://bootstrap.example.com/ztp/{intent.device_id}.py",
    )


@activity.defn(name="publish_bootstrap_script")
async def mock_publish_bootstrap_script(script: BootstrapScript) -> None:
    pass


@activity.defn(name="wait_for_device_reachability")
async def mock_wait_for_device_reachability(device_id: str, management_ip: str) -> bool:
    return True


# ---------------------------------------------------------------------------
# Mock activities — ComplianceScanWorkflow (Day 2)
# ---------------------------------------------------------------------------


@activity.defn(name="fetch_site_devices")
async def mock_fetch_site_devices(site_id: str) -> list[str]:
    return ["DEV001", "DEV002"]


# ---------------------------------------------------------------------------
# Day 1: ProvisionSiteWorkflow tests
# ---------------------------------------------------------------------------


class TestProvisionSiteWorkflow:
    @pytest.fixture
    def day1_activities(self):  # type: ignore[no-untyped-def]
        return [
            mock_fetch_device_intent,
            mock_write_provisioning_status,
            mock_render_config,
            mock_push_config,
            mock_validate_device_state,
        ]

    async def test_happy_path(self, day1_activities: list) -> None:
        """Full Day 1 run: fetch → render → push → validate → COMPLETE."""
        async with (
            await WorkflowEnvironment.start_time_skipping() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[ProvisionSiteWorkflow],
                activities=day1_activities,
            ),
        ):
            result: ProvisionSiteResult = await env.client.execute_workflow(
                ProvisionSiteWorkflow.run,
                ProvisionSiteInput(device_id="DEV001", requested_by="test"),
                id="test-day1-happy",
                task_queue=TASK_QUEUE,
            )

        assert result.device_id == "DEV001"
        assert result.success is True
        assert result.failure_reason == ""

    async def test_validation_drift_triggers_hitl(self) -> None:
        """Drift on validation parks the workflow for human approval."""

        @activity.defn(name="validate_device_state")
        async def _drifting_validation(device_id: str, expected: DeviceIntent) -> ValidationResult:
            return ValidationResult(
                device_id=device_id,
                passed=False,
                drift_detected=["BGP neighbor 203.0.113.1 not established"],
            )

        activities = [
            mock_fetch_device_intent,
            mock_write_provisioning_status,
            mock_render_config,
            mock_push_config,
            _drifting_validation,
        ]

        async with (
            await WorkflowEnvironment.start_time_skipping() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[ProvisionSiteWorkflow],
                activities=activities,
            ),
        ):
            handle = await env.client.start_workflow(
                ProvisionSiteWorkflow.run,
                ProvisionSiteInput(device_id="DEV001", requested_by="test"),
                id="test-day1-hitl",
                task_queue=TASK_QUEUE,
            )

            # Send approval signal — time-skipping env fast-forwards the wait.
            await handle.signal(ProvisionSiteWorkflow.approve_escalation, "approved")
            result: ProvisionSiteResult = await handle.result()

        assert result.device_id == "DEV001"

    async def test_hitl_rejection_fails_workflow(self) -> None:
        """A 'rejected' signal from the engineer marks the workflow as failed."""

        @activity.defn(name="validate_device_state")
        async def _drifting_validation(device_id: str, expected: DeviceIntent) -> ValidationResult:
            return ValidationResult(
                device_id=device_id,
                passed=False,
                drift_detected=["VLAN 100 missing"],
            )

        activities = [
            mock_fetch_device_intent,
            mock_write_provisioning_status,
            mock_render_config,
            mock_push_config,
            _drifting_validation,
        ]

        async with (
            await WorkflowEnvironment.start_time_skipping() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[ProvisionSiteWorkflow],
                activities=activities,
            ),
        ):
            handle = await env.client.start_workflow(
                ProvisionSiteWorkflow.run,
                ProvisionSiteInput(device_id="DEV001", requested_by="test"),
                id="test-day1-rejected",
                task_queue=TASK_QUEUE,
            )

            await handle.signal(ProvisionSiteWorkflow.approve_escalation, "rejected")
            result: ProvisionSiteResult = await handle.result()

        assert result.success is False

    async def test_fetch_failure_returns_failed_result(self) -> None:
        """An activity failure is caught by the workflow's except block → success=False."""

        @activity.defn(name="fetch_device_intent")
        async def _failing_fetch(device_id: str) -> DeviceIntent:
            raise ApplicationError("Device not found in Nautobot", non_retryable=True)

        activities = [
            _failing_fetch,
            mock_write_provisioning_status,
            mock_render_config,
            mock_push_config,
            mock_validate_device_state,
        ]

        async with (
            await WorkflowEnvironment.start_time_skipping() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[ProvisionSiteWorkflow],
                activities=activities,
            ),
        ):
            result: ProvisionSiteResult = await env.client.execute_workflow(
                ProvisionSiteWorkflow.run,
                ProvisionSiteInput(device_id="DEV_MISSING", requested_by="test"),
                id="test-day1-fetch-fail",
                task_queue=TASK_QUEUE,
            )

        assert result.success is False
        assert (
            result.failure_reason != ""
        )  # ActivityError wrapper; inner message is in the cause chain


# ---------------------------------------------------------------------------
# Day 2: ComplianceScanWorkflow tests
# ---------------------------------------------------------------------------


class TestComplianceScanWorkflow:
    @pytest.fixture
    def day2_activities(self):  # type: ignore[no-untyped-def]
        return [
            mock_fetch_site_devices,
            mock_fetch_device_intent,
            mock_validate_device_state,
            mock_write_provisioning_status,
        ]

    async def test_scans_all_site_devices(self, day2_activities: list) -> None:
        """Without explicit device_ids, the workflow scans all devices at the site."""
        async with (
            await WorkflowEnvironment.start_time_skipping() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[ComplianceScanWorkflow],
                activities=day2_activities,
            ),
        ):
            result = await env.client.execute_workflow(
                ComplianceScanWorkflow.run,
                ComplianceScanInput(
                    site_id="SITE001",
                    requested_by="test",
                    device_ids=[],
                ),
                id="test-day2-all-devices",
                task_queue=TASK_QUEUE,
            )

        assert result is not None
        assert result.site_id == "SITE001"

    async def test_explicit_device_ids_skips_site_fetch(self, day2_activities: list) -> None:
        """Explicit device_ids bypass fetch_site_devices."""
        async with (
            await WorkflowEnvironment.start_time_skipping() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[ComplianceScanWorkflow],
                activities=day2_activities,
            ),
        ):
            result = await env.client.execute_workflow(
                ComplianceScanWorkflow.run,
                ComplianceScanInput(
                    site_id="SITE001",
                    requested_by="test",
                    device_ids=["DEV001"],
                ),
                id="test-day2-explicit",
                task_queue=TASK_QUEUE,
            )

        assert result.site_id == "SITE001"
        assert result.total_devices == 1
