"""
Integration tests for Temporal activities.

Test strategy:
  - Mock path (default): _USE_MOCK=True — exercises parsing, templating, and
    business logic without network or device dependencies.
  - Live path: _USE_MOCK=False via monkeypatch + unittest.mock patching of the
    httpx2.AsyncClient class.  httpx2 uses httpcore2 (not httpcore), so respx
    cannot intercept its transport layer.  We patch at the class level instead,
    which lets us assert the correct URL, headers, and payload were sent.

pytest-asyncio is configured with asyncio_mode=auto in pyproject.toml so no
per-test @pytest.mark.asyncio decorator is needed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx2
import pytest

import temporal.activities.nautobot_activities as nautobot_activities
import temporal.activities.validation_activities as validation_activities
from temporal.activities.ansible_activities import push_config, render_config
from temporal.activities.bootstrap_activities import (
    publish_bootstrap_script,
    register_dhcp_reservation,
    render_bootstrap_script,
    wait_for_device_reachability,
)
from temporal.activities.nautobot_activities import (
    fetch_device_intent,
    fetch_site_devices,
    write_provisioning_status,
)
from temporal.activities.validation_activities import validate_device_state
from temporal.models import BootstrapScript, DeviceIntent, InterfaceIntent, VlanIntent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NAUTOBOT_URL = "http://localhost:8080"


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
        default_gateway="10.100.255.254",
    )


def _graphql_device_response(device_id: str = "DEV001") -> dict:  # type: ignore[type-arg]
    """Realistic Nautobot GraphQL payload for use in respx mocks."""
    return {
        "data": {
            "device": {
                "id": device_id,
                "name": f"br-{device_id.lower()}-rtr01",
                "platform": {"slug": "cisco_ios_xe"},
                "primary_ip4": {"address": "10.100.255.1/32"},
                "interfaces": [
                    {
                        "name": "GigabitEthernet0/0/0",
                        "description": "MPLS WAN",
                        "enabled": True,
                        "ip_addresses": [{"address": "203.0.113.2/30"}],
                        "tagged_vlans": [],
                    },
                    {
                        "name": "Loopback0",
                        "description": "Router-ID",
                        "enabled": True,
                        "ip_addresses": [{"address": "10.100.255.1/32"}],
                        "tagged_vlans": [
                            {"vid": 100, "name": "CORP-DATA", "description": ""},
                            {"vid": 200, "name": "VOICE", "description": ""},
                        ],
                    },
                ],
                "config_context": {
                    "bgp_asn": 65001,
                    "bgp_peer_ip": "203.0.113.1",
                    "bgp_peer_asn": 64512,
                    "ntp_servers": ["10.0.0.1"],
                    "syslog_servers": ["10.0.1.100"],
                    "default_gateway": "10.100.255.254",
                },
            }
        }
    }


def _graphql_site_response(site_id: str = "SITE001") -> dict:  # type: ignore[type-arg]
    return {
        "data": {
            "site": {
                "devices": [
                    {"id": "DEV001", "name": "br-dev001-rtr01"},
                    {"id": "DEV002", "name": "br-dev002-rtr01"},
                ]
            }
        }
    }


# ---------------------------------------------------------------------------
# Nautobot activities — mock path
# ---------------------------------------------------------------------------


class TestFetchDeviceIntentMock:
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


# ---------------------------------------------------------------------------
# Nautobot activities — live path (httpx2.AsyncClient patched via unittest.mock)
# ---------------------------------------------------------------------------
# httpx2 uses httpcore2 internally, not httpcore, so respx cannot intercept its
# transport layer.  We patch httpx2.AsyncClient at the class level — this is the
# same object the module references as `httpx.AsyncClient` (via `import httpx2 as httpx`).
# ---------------------------------------------------------------------------


def _make_mock_http_client(json_payload: dict) -> MagicMock:  # type: ignore[type-arg]
    """Build a mock httpx2.AsyncClient whose .post() returns json_payload."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = json_payload

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client


class TestFetchDeviceIntentLive:
    async def test_graphql_query_sent_correctly(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_USE_MOCK=False: the activity issues a real GraphQL POST to Nautobot."""
        monkeypatch.setattr(nautobot_activities, "_USE_MOCK", False)

        mock_client = _make_mock_http_client(_graphql_device_response("DEV001"))
        with patch("httpx2.AsyncClient", return_value=mock_client):
            result = await fetch_device_intent("DEV001")

        mock_client.post.assert_called_once()
        call_url = mock_client.post.call_args[0][0]
        assert call_url.endswith("/graphql/")
        assert result.device_id == "DEV001"
        assert result.hostname == "br-dev001-rtr01"
        assert result.bgp_asn == 65001

    async def test_http_error_propagates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A non-2xx response raises when raise_for_status() is called."""
        monkeypatch.setattr(nautobot_activities, "_USE_MOCK", False)

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx2.HTTPStatusError(
            "401 Unauthorized",
            request=MagicMock(),
            response=MagicMock(),
        )
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("httpx2.AsyncClient", return_value=mock_client),
            pytest.raises(httpx2.HTTPStatusError),
        ):
            await fetch_device_intent("DEV001")

    async def test_fetch_site_devices_live(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """fetch_site_devices issues a GraphQL POST and returns device UUIDs."""
        monkeypatch.setattr(nautobot_activities, "_USE_MOCK", False)

        mock_client = _make_mock_http_client(_graphql_site_response("SITE001"))
        with patch("httpx2.AsyncClient", return_value=mock_client):
            result = await fetch_site_devices("SITE001")

        assert result == ["DEV001", "DEV002"]


class TestWriteProvisioningStatus:
    async def test_no_exception_on_valid_status(self) -> None:
        await write_provisioning_status("DEV001", "PROVISIONING_STARTED", "wf-test-123")

    async def test_accepts_all_lifecycle_states(self) -> None:
        for status in ("QUEUED", "PROVISIONING_STARTED", "COMPLETE", "FAILED"):
            await write_provisioning_status("DEV001", status, "wf-test-123")

    async def test_live_path_calls_pynautobot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_USE_MOCK=False: the activity calls pynautobot.api().dcim.devices.update()."""
        monkeypatch.setattr(nautobot_activities, "_USE_MOCK", False)

        mock_nb = MagicMock()
        mock_nb.dcim.devices.update = MagicMock(return_value=True)

        with patch("temporal.activities.nautobot_activities.pynautobot.api", return_value=mock_nb):
            await write_provisioning_status("DEV001", "COMPLETE", "wf-live-456")

        mock_nb.dcim.devices.update.assert_called_once()
        call_args = mock_nb.dcim.devices.update.call_args[0][0]
        assert call_args[0]["id"] == "DEV001"
        assert call_args[0]["custom_fields"]["ztp_provisioning_status"] == "COMPLETE"
        assert call_args[0]["custom_fields"]["ztp_workflow_id"] == "wf-live-456"


# ---------------------------------------------------------------------------
# Ansible activities — render_config
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
        assert "203.0.113.2/30" not in result.config_content
        assert "203.0.113.2 255.255.255.252" in result.config_content

    async def test_rendered_at_timestamp(self) -> None:
        intent = _make_intent()
        before = datetime.now(tz=UTC)
        result = await render_config(intent)
        assert result.rendered_at >= before


# ---------------------------------------------------------------------------
# Ansible activities — push_config
# ---------------------------------------------------------------------------


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

    async def test_output_shows_device_id(self) -> None:
        intent = _make_intent("DEV042")
        rendered = await render_config(intent)
        result = await push_config(rendered)
        assert "DEV042" in result.output


# ---------------------------------------------------------------------------
# Bootstrap activities
# ---------------------------------------------------------------------------


class TestRegisterDhcpReservation:
    async def test_returns_reservation(self) -> None:
        result = await register_dhcp_reservation("aa:bb:cc:dd:ee:ff", "DEV001", "br-dev001-rtr01")
        assert result.device_id == "DEV001"
        assert result.mac_address == "aa:bb:cc:dd:ee:ff"
        assert result.lease_seconds == 3600

    async def test_assigned_ip_is_set(self) -> None:
        result = await register_dhcp_reservation("aa:bb:cc:dd:ee:ff", "DEV001", "br-dev001-rtr01")
        assert result.assigned_ip != ""


class TestRenderBootstrapScript:
    async def test_renders_hostname(self) -> None:
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert intent.hostname in result.script_content

    async def test_renders_mgmt_interface(self) -> None:
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert (
            "GigabitEthernet" in result.script_content
            or intent.mgmt_interface in result.script_content
        )

    async def test_script_url_set(self) -> None:
        intent = _make_intent("DEV001")
        result = await render_bootstrap_script(intent)
        assert "DEV001" in result.script_url
        assert result.script_url.startswith("http")

    async def test_missing_gateway_raises(self) -> None:
        intent = _make_intent()
        intent.default_gateway = ""
        with pytest.raises(ValueError, match="default_gateway"):
            await render_bootstrap_script(intent)

    async def test_script_contains_configure_management(self) -> None:
        intent = _make_intent()
        result = await render_bootstrap_script(intent)
        assert "configure_management" in result.script_content
        assert "configure_ssh" in result.script_content


class TestPublishBootstrapScript:
    async def test_no_exception_in_mock_mode(self) -> None:
        script = BootstrapScript(
            device_id="DEV001",
            script_content="#!/usr/bin/env python3\nprint('hello')\n",
            script_url="http://bootstrap.example.com/ztp/DEV001.py",
        )
        await publish_bootstrap_script(script)  # should not raise


class TestWaitForDeviceReachability:
    async def test_returns_true_in_mock_mode(self) -> None:
        result = await wait_for_device_reachability("DEV001", "10.100.255.1")
        assert result is True


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
        """Statistical: over 50 runs, at least 70 % should pass (drift rate is 10 %)."""
        intent = _make_intent()
        passes = sum(
            1
            for _ in range(50)
            # we can't await inside a generator but we can use a sync counter
        )
        # rewrite without generator
        passes = 0
        for _ in range(50):
            result = await validate_device_state("DEV001", intent)
            if result.passed:
                passes += 1
        assert passes >= 35, f"Expected ≥70% pass rate, got {passes}/50"

    async def test_drift_items_are_strings(self) -> None:
        """Run until a failure is produced, then assert item types."""
        intent = _make_intent()
        failed: list[str] = []
        for _ in range(30):
            result = await validate_device_state("DEV001", intent)
            if not result.passed:
                failed = result.drift_detected
                break
        if failed:
            assert all(isinstance(item, str) for item in failed)
            assert all(len(item) > 0 for item in failed)

    async def test_forced_failure_returns_drift(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_DRIFT_PROBABILITY=1.0: random() is always < 1.0 so drift always fires."""
        monkeypatch.setattr(validation_activities, "_DRIFT_PROBABILITY", 1.0)
        intent = _make_intent()
        result = await validate_device_state("DEV001", intent)
        assert result.passed is False
        assert len(result.drift_detected) >= 1

    async def test_forced_pass_returns_no_drift(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_DRIFT_PROBABILITY=0.0: random() is always > 0.0 so the no-drift branch always runs."""
        monkeypatch.setattr(validation_activities, "_DRIFT_PROBABILITY", 0.0)
        intent = _make_intent()
        result = await validate_device_state("DEV001", intent)
        assert result.passed is True
        assert result.drift_detected == []
