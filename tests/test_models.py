"""Unit tests for Pydantic model validation and serialization."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from temporal.models import (
    DeviceIntent,
    InterfaceIntent,
    ProvisioningStatus,
    ProvisionSiteInput,
    ProvisionSiteResult,
    VlanIntent,
)


class TestVlanIntent:
    def test_valid_vlan(self) -> None:
        v = VlanIntent(vlan_id=100, name="CORP-DATA")
        assert v.vlan_id == 100
        assert v.name == "CORP-DATA"

    def test_vlan_id_bounds(self) -> None:
        with pytest.raises(ValidationError):
            VlanIntent(vlan_id=0, name="too-low")
        with pytest.raises(ValidationError):
            VlanIntent(vlan_id=4095, name="too-high")

    def test_boundary_vlans_valid(self) -> None:
        VlanIntent(vlan_id=1, name="min")
        VlanIntent(vlan_id=4094, name="max")


class TestDeviceIntent:
    def test_defaults(self) -> None:
        d = DeviceIntent(
            device_id="DEV001",
            hostname="br-dev001-rtr01",
            platform="cisco_ios_xe",
            primary_ip="10.100.255.1/32",
        )
        assert d.provisioning_status == ProvisioningStatus.QUEUED
        assert d.interfaces == []
        assert d.vlans == []
        assert d.bgp_asn == 65000

    def test_with_interfaces_and_vlans(self) -> None:
        iface = InterfaceIntent(name="GigabitEthernet0/0/0", ip_address="203.0.113.2/30")
        vlan = VlanIntent(vlan_id=100, name="CORP")
        d = DeviceIntent(
            device_id="DEV001",
            hostname="br-dev001-rtr01",
            platform="cisco_ios_xe",
            primary_ip="10.100.255.1/32",
            interfaces=[iface],
            vlans=[vlan],
        )
        assert len(d.interfaces) == 1
        assert len(d.vlans) == 1

    def test_json_roundtrip(self) -> None:
        d = DeviceIntent(
            device_id="DEV001",
            hostname="br-dev001-rtr01",
            platform="cisco_ios_xe",
            primary_ip="10.100.255.1/32",
        )
        serialised = d.model_dump_json()
        restored = DeviceIntent.model_validate_json(serialised)
        assert restored.device_id == d.device_id
        assert restored.hostname == d.hostname


class TestProvisionSiteInput:
    def test_required_fields(self) -> None:
        with pytest.raises(ValidationError):
            ProvisionSiteInput(device_id="DEV001")  # type: ignore[call-arg]

    def test_valid(self) -> None:
        inp = ProvisionSiteInput(device_id="DEV001", requested_by="mark")
        assert inp.device_id == "DEV001"
        assert inp.requested_by == "mark"


class TestProvisionSiteResult:
    def test_completed_at_default(self) -> None:
        r = ProvisionSiteResult(
            device_id="DEV001",
            success=True,
            workflow_id="wf-123",
        )
        assert r.completed_at is not None
        assert r.failure_reason == ""

    def test_failure_result(self) -> None:
        r = ProvisionSiteResult(
            device_id="DEV001",
            success=False,
            workflow_id="wf-123",
            failure_reason="push failed",
        )
        assert not r.success
        assert r.failure_reason == "push failed"


class TestOnboardingModels:
    def test_remediation_plan_round_trip(self) -> None:
        from datetime import UTC, datetime

        from temporal.models import ConfigChange, RemediationPlan

        plan = RemediationPlan(
            site_id="SITE-001",
            device_id="DEV001",
            snapshot_id="snap-abc",
            changes=[
                ConfigChange(
                    section="bgp",
                    description="Add peer 10.0.0.1",
                    current="",
                    intended="neighbor 10.0.0.1 remote-as 64512",
                )
            ],
            estimated_impact="high",
            created_at=datetime.now(UTC),
        )
        assert RemediationPlan.model_validate(plan.model_dump()) == plan

    def test_onboard_site_input_round_trip(self) -> None:
        from temporal.models import OnboardSiteInput

        inp = OnboardSiteInput(site_id="SITE-001", device_id="DEV001", requested_by="eng")
        assert OnboardSiteInput.model_validate(inp.model_dump()) == inp

    def test_onboarding_status_values_are_stable(self) -> None:
        from temporal.models import ProvisioningStatus

        assert ProvisioningStatus.ONBOARD_PENDING == "ONBOARD_PENDING"
        assert ProvisioningStatus.ONBOARD_MANAGED == "ONBOARD_MANAGED"
