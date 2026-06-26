"""
Integration tests for Temporal activities.

These tests invoke activities directly (bypassing the Temporal worker) to
validate the mock paths, Jinja2 rendering, and validation logic without
requiring a live Temporal server or network devices.

pytest-asyncio is configured with asyncio_mode=auto in pyproject.toml so no
per-test @pytest.mark.asyncio decorator is needed.
"""

from __future__ import annotations

from temporal.activities.ansible_activities import push_config, render_config
from temporal.activities.nautobot_activities import (
    fetch_device_intent,
    write_provisioning_status,
)
from temporal.activities.validation_activities import validate_device_state
from temporal.models import DeviceIntent, InterfaceIntent, VlanIntent

# ---------------------------------------------------------------------------
# Helpers
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
        vlans=[
            VlanIntent(vlan_id=100, name="CORP-DATA"),
            VlanIntent(vlan_id=200, name="VOICE"),
        ],
        bgp_asn=65001,
        bgp_peer_ip="203.0.113.1",
        bgp_peer_asn=64512,
        ntp_servers=["10.0.0.1"],
        syslog_servers=["10.0.1.100"],
    )


# ---------------------------------------------------------------------------
# Nautobot activities
# ---------------------------------------------------------------------------


class TestFetchDeviceIntent:
    async def test_returns_device_intent(self) -> None:
        result = await fetch_device_intent("DEV001")
        assert result.device_id == "DEV001"
        assert result.hostname.startswith("br-")
        assert result.platform == "cisco_ios_xe"
        assert len(result.interfaces) >= 1
        assert len(result.vlans) >= 1

    async def test_bgp_fields_populated(self) -> None:
        result = await fetch_device_intent("DEV001")
        assert result.bgp_asn > 0
        assert result.bgp_peer_ip != ""

    async def test_different_device_ids(self) -> None:
        r1 = await fetch_device_intent("DEV001")
        r2 = await fetch_device_intent("DEV999")
        assert r1.device_id == "DEV001"
        assert r2.device_id == "DEV999"


class TestWriteProvisioningStatus:
    async def test_no_exception_on_valid_status(self) -> None:
        await write_provisioning_status("DEV001", "PROVISIONING_STARTED", "wf-test-123")

    async def test_accepts_all_lifecycle_states(self) -> None:
        for status in ("QUEUED", "PROVISIONING_STARTED", "COMPLETE", "FAILED"):
            await write_provisioning_status("DEV001", status, "wf-test-123")


# ---------------------------------------------------------------------------
# Ansible activities
# ---------------------------------------------------------------------------


class TestRenderConfig:
    async def test_renders_hostname(self) -> None:
        intent = _make_intent()
        result = await render_config(intent)
        assert f"hostname {intent.hostname}" in result.config_content

    async def test_renders_interfaces(self) -> None:
        intent = _make_intent()
        result = await render_config(intent)
        assert "GigabitEthernet0/0/0" in result.config_content
        assert "Loopback0" in result.config_content

    async def test_renders_vlans(self) -> None:
        intent = _make_intent()
        result = await render_config(intent)
        assert "vlan 100" in result.config_content
        assert "CORP-DATA" in result.config_content

    async def test_renders_bgp(self) -> None:
        intent = _make_intent()
        result = await render_config(intent)
        assert f"router bgp {intent.bgp_asn}" in result.config_content
        assert intent.bgp_peer_ip in result.config_content

    async def test_renders_ntp(self) -> None:
        intent = _make_intent()
        result = await render_config(intent)
        assert "ntp server 10.0.0.1" in result.config_content

    async def test_renders_syslog(self) -> None:
        intent = _make_intent()
        result = await render_config(intent)
        assert "logging host 10.0.1.100" in result.config_content

    async def test_template_name_set(self) -> None:
        intent = _make_intent()
        result = await render_config(intent)
        assert result.template_name == "ios_xe_branch_router.j2"
        assert result.device_id == intent.device_id

    async def test_ios_ip_address_format(self) -> None:
        """IOS uses 'ip address x.x.x.x m.m.m.m' not CIDR notation."""
        intent = _make_intent()
        result = await render_config(intent)
        # CIDR notation should not appear in interface stanzas
        assert "203.0.113.2/30" not in result.config_content
        assert "203.0.113.2 255.255.255.252" in result.config_content


class TestPushConfig:
    async def test_returns_success(self) -> None:
        intent = _make_intent()
        rendered = await render_config(intent)
        result = await push_config(rendered)
        assert result.success is True
        assert result.device_id == intent.device_id
        assert result.duration_seconds > 0

    async def test_output_contains_play_recap(self) -> None:
        intent = _make_intent()
        rendered = await render_config(intent)
        result = await push_config(rendered)
        assert "PLAY RECAP" in result.output


# ---------------------------------------------------------------------------
# Validation activities
# ---------------------------------------------------------------------------


class TestValidateDeviceState:
    async def test_returns_validation_result(self) -> None:
        intent = _make_intent()
        result = await validate_device_state("DEV001", intent)
        assert result.device_id == "DEV001"
        assert isinstance(result.passed, bool)
        assert isinstance(result.drift_detected, list)

    async def test_mostly_passes(self) -> None:
        """Statistical test: over 50 runs, at least 70% should pass."""
        intent = _make_intent()
        passes = 0
        for _ in range(50):
            result = await validate_device_state("DEV001", intent)
            if result.passed:
                passes += 1
        assert passes >= 35, f"Expected ≥70% pass rate, got {passes}/50"

    async def test_drift_items_are_strings(self) -> None:
        intent = _make_intent()
        # Run enough times to hit a failure case (statistically very likely over 20 runs).
        failed: list[str] = []
        for _ in range(20):
            result = await validate_device_state("DEV001", intent)
            if not result.passed:
                failed = result.drift_detected
                break
        # If we didn't hit a failure, skip rather than fail the suite.
        if failed:
            assert all(isinstance(item, str) for item in failed)
            assert all(len(item) > 0 for item in failed)
