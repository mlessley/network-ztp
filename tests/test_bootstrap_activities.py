"""Unit tests for Day 0 bootstrap activities."""

from __future__ import annotations

from temporal.activities.bootstrap_activities import (
    publish_bootstrap_script,
    register_dhcp_reservation,
    render_bootstrap_script,
    wait_for_device_reachability,
)
from temporal.models import BootstrapScript, DeviceIntent, InterfaceIntent


def _make_intent(device_id: str = "DEV001") -> DeviceIntent:
    return DeviceIntent(
        device_id=device_id,
        hostname=f"br-{device_id.lower()}-rtr01",
        platform="cisco_ios_xe",
        primary_ip="10.100.255.1/32",
        mgmt_interface="GigabitEthernet0",
        default_gateway="10.100.255.254",
        interfaces=[
            InterfaceIntent(name="GigabitEthernet0", ip_address="10.100.255.1/32"),
        ],
        ntp_servers=["10.0.0.1"],
        syslog_servers=["10.0.1.100"],
    )


class TestRegisterDhcpReservation:
    async def test_returns_reservation(self) -> None:
        result = await register_dhcp_reservation("aa:bb:cc:dd:ee:ff", "DEV001", "br-dev001-rtr01")
        assert result.device_id == "DEV001"
        assert result.mac_address == "aa:bb:cc:dd:ee:ff"
        assert result.lease_seconds > 0

    async def test_different_macs(self) -> None:
        r1 = await register_dhcp_reservation("aa:bb:cc:dd:ee:01", "DEV001", "host1")
        r2 = await register_dhcp_reservation("aa:bb:cc:dd:ee:02", "DEV002", "host2")
        assert r1.mac_address != r2.mac_address


class TestRenderBootstrapScript:
    async def test_renders_hostname(self) -> None:
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert intent.hostname in result.script_content

    async def test_renders_mgmt_interface(self) -> None:
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert intent.mgmt_interface in result.script_content

    async def test_renders_default_gateway(self) -> None:
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert intent.default_gateway in result.script_content

    async def test_ios_ip_format_in_script(self) -> None:
        """Management IP should be in IOS format (space-separated mask), not CIDR."""
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert "10.100.255.1/32" not in result.script_content
        assert "10.100.255.1 255.255.255.255" in result.script_content

    async def test_script_url_contains_device_id(self) -> None:
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert intent.device_id in result.script_url

    async def test_missing_gateway_raises(self) -> None:
        import pytest

        intent = _make_intent()
        intent.default_gateway = ""
        with pytest.raises(ValueError, match="default_gateway"):
            await render_bootstrap_script(intent)

    async def test_contains_ssh_config(self) -> None:
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert "ip ssh version 2" in result.script_content

    async def test_contains_write_memory(self) -> None:
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert "write memory" in result.script_content


class TestPublishBootstrapScript:
    async def test_no_exception(self) -> None:
        script = BootstrapScript(
            device_id="DEV001",
            script_content="# test script",
            script_url="http://bootstrap.example.com/ztp/DEV001.py",
        )
        await publish_bootstrap_script(script)


class TestWaitForDeviceReachability:
    async def test_returns_true(self) -> None:
        result = await wait_for_device_reachability("DEV001", "10.100.255.1")
        assert result is True
