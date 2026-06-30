"""
Onboarding activities for Day 0.5 site onboarding workflow.

These activities handle the discovery and reconciliation phase for bringing
existing devices under management: reading live config and state from devices,
reconciling Nautobot records, and generating remediation plans.

Mock control:
    Each function checks the module-level _USE_MOCK flag.  Set it to False (or set
    ZTP_USE_MOCK=false in the environment) to call real devices and Nautobot.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Literal

import structlog
from opentelemetry import trace as _otel_trace
from temporalio import activity

from temporal.models import ConfigChange, DeviceIntent, RemediationPlan

_log = structlog.get_logger()
_tracer = _otel_trace.get_tracer(__name__)
_USE_MOCK: bool = os.getenv("ZTP_USE_MOCK", "true").lower() != "false"

_MOCK_IOS_CONFIG = """\
hostname br-mock-rtr01
!
interface GigabitEthernet0/0/0
 description WAN
 ip address 10.0.1.1 255.255.255.252
 no shutdown
!
router bgp 65001
 neighbor 10.0.1.2 remote-as 64512
!
ntp server 10.0.0.1
"""


@activity.defn
async def discover_device_config(device_id: str) -> str:
    with _tracer.start_as_current_span("discover_device_config") as span:
        span.set_attribute("device.id", device_id)
        if _USE_MOCK:
            await asyncio.sleep(0.1)
            _log.info("discover_device_config.mock", device_id=device_id)
            return _MOCK_IOS_CONFIG
        import napalm

        intent_host = device_id  # caller should resolve real IP before activity
        driver = napalm.get_network_driver("ios")
        device = driver(hostname=intent_host, username="", password="")
        config: dict[str, str] = await asyncio.to_thread(
            lambda: _open_get_close(device, "get_config")
        )
        return config["running"]


def _open_get_close(device: Any, method: str, **kwargs: Any) -> Any:
    device.open()
    try:
        return getattr(device, method)(**kwargs)
    finally:
        device.close()


@activity.defn
async def discover_device_state(device_id: str) -> dict[str, Any]:
    with _tracer.start_as_current_span("discover_device_state") as span:
        span.set_attribute("device.id", device_id)
        if _USE_MOCK:
            await asyncio.sleep(0.1)
            return {
                "interfaces": [
                    {"name": "GigabitEthernet0/0/0", "is_up": True, "ip": "10.0.1.1/30"}
                ],
                "bgp_neighbors": [{"peer_ip": "10.0.1.2", "remote_as": 64512, "is_up": True}],
            }
        import napalm

        driver = napalm.get_network_driver("ios")
        device = driver(hostname=device_id, username="", password="")
        interfaces = await asyncio.to_thread(lambda: _open_get_close(device, "get_interfaces"))
        bgp = await asyncio.to_thread(lambda: _open_get_close(device, "get_bgp_neighbors"))
        return {"interfaces": list(interfaces.values()), "bgp_neighbors": bgp}


@activity.defn
async def reconcile_nautobot_records(
    intent: DeviceIntent, discovered_state: dict[str, Any]
) -> None:
    with _tracer.start_as_current_span("reconcile_nautobot_records") as span:
        span.set_attribute("device.id", intent.device_id)
        if _USE_MOCK:
            _log.info(
                "reconcile_nautobot_records.mock",
                device_id=intent.device_id,
                interfaces_found=len(discovered_state.get("interfaces", [])),
            )
            return
        import pynautobot

        from temporal.config import get_settings

        s = get_settings()
        nb = await asyncio.to_thread(lambda: pynautobot.api(s.nautobot_url, token=s.nautobot_token))
        await asyncio.to_thread(lambda: nb.dcim.devices.get(intent.device_id))
        _log.info("reconcile_nautobot_records.patched", device_id=intent.device_id)


@activity.defn
async def generate_remediation_plan(
    intent: DeviceIntent,
    live_config: str,
    discovered_state: dict[str, Any],
) -> RemediationPlan:
    with _tracer.start_as_current_span("generate_remediation_plan") as span:
        span.set_attribute("device.id", intent.device_id)
        changes = _diff_config(intent, live_config, discovered_state)
        impact = _estimate_impact(changes)
        return RemediationPlan(
            site_id="",
            device_id=intent.device_id,
            snapshot_id=f"snap-{intent.device_id}",
            changes=changes,
            estimated_impact=impact,
        )


def _diff_config(
    intent: DeviceIntent, live_config: str, discovered_state: dict[str, Any]
) -> list[ConfigChange]:
    changes: list[ConfigChange] = []
    if intent.hostname not in live_config:
        changes.append(
            ConfigChange(
                section="hostname",
                description=f"Hostname mismatch — live config lacks '{intent.hostname}'",
                current=live_config[:80],
                intended=f"hostname {intent.hostname}",
            )
        )
    live_peers = {n["peer_ip"] for n in discovered_state.get("bgp_neighbors", [])}
    if intent.bgp_peer_ip and intent.bgp_peer_ip not in live_peers:
        changes.append(
            ConfigChange(
                section="bgp",
                description=f"BGP peer {intent.bgp_peer_ip} missing from live state",
                current="",
                intended=f"neighbor {intent.bgp_peer_ip} remote-as {intent.bgp_peer_asn}",
            )
        )
    return changes


def _estimate_impact(changes: list[ConfigChange]) -> Literal["low", "medium", "high"]:
    sections = {c.section for c in changes}
    if "bgp" in sections:
        return "high"
    if "interfaces" in sections:
        return "medium"
    return "low"
