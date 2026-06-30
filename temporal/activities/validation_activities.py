"""
Post-push validation activities.

Production approach — NAPALM:
    NAPALM (Network Automation and Programmability Abstraction Layer with Multivendor
    support) provides a vendor-agnostic interface for reading live device state.
    For Cisco IOS-XE the driver is "ios", which uses Netmiko under the hood for
    the SSH transport.

    The validation pattern:
      1. Open a NAPALM connection to the device's management IP.
      2. Call get_interfaces() and get_bgp_neighbors() for structured state.
      3. Diff each value against the DeviceIntent from Nautobot.
      4. Return drift items as human-readable strings.

    All NAPALM calls are synchronous; they run in asyncio.to_thread() so they
    don't block the Temporal activity event loop.

Mock behaviour:
    _DRIFT_PROBABILITY controls the simulated failure rate (default 10 %).
    Set it to 1.0 in tests to force the HITL escalation path every time.
"""

from __future__ import annotations

import asyncio
import os
import random

import napalm
import structlog
from temporalio import activity

from temporal.models import DeviceIntent, ValidationResult

log = structlog.get_logger()

# Realistic drift scenarios a network engineer would recognise.
_DRIFT_SCENARIOS: list[str] = [
    "interface GigabitEthernet0/0/0 IP mismatch: expected 203.0.113.2/30, got 203.0.113.6/30",
    "VLAN 100 missing from trunk port GigabitEthernet0/0/1",
    "BGP neighbor 203.0.113.1 not established (state=Idle, expected=Established)",
    "NTP server 10.0.0.1 unreachable — clock may be unsynchronised",
    "hostname mismatch: expected br-dev001-rtr01, running config shows OLD-BR-RTR",
    "interface Loopback0 shutdown — expected no shutdown",
    "VLAN 200 name mismatch: expected VOICE, found VOICE-OLD",
    "logging host 10.0.1.100 missing from running config",
    "BGP route-map missing on neighbor — soft-reconfiguration not active",
    "SNMP community string does not match Nautobot config context",
]

_DRIFT_PROBABILITY = 0.10

# Flip to False (or set ZTP_USE_MOCK=false) to call real devices via NAPALM.
_USE_MOCK: bool = os.getenv("ZTP_USE_MOCK", "true").lower() != "false"


@activity.defn
async def validate_device_state(
    device_id: str,
    expected: DeviceIntent,
) -> ValidationResult:
    """
    Compare live device running-state against declared intent from Nautobot.

    Production path (NAPALM):
        Opens an SSH session to the device's management IP and retrieves
        structured state via get_interfaces() and get_bgp_neighbors().
        Diffs each field in `expected` against the live values.

    Mock path:
        Simulates the statistical behaviour of the production path: 90 % pass,
        10 % fail with one or two realistic drift items.  Lets developers
        exercise the HITL escalation path during local testing without a device.

    Args:
        device_id: Device to validate (SSH target in production).
        expected:  The DeviceIntent the device should now reflect.

    Returns:
        ValidationResult with passed=True and empty drift_detected on success,
        or passed=False with drift descriptions on failure.
    """
    log.info(
        "validate_device_state.started",
        device_id=device_id,
        hostname=expected.hostname,
    )

    if not _USE_MOCK:
        drift = await _validate_via_napalm(device_id, expected)
    else:
        await asyncio.sleep(1)  # simulate SSH round-trip + config parse latency
        drift = _simulate_validation()

    passed = len(drift) == 0

    if passed:
        log.info("validate_device_state.passed", device_id=device_id)
    else:
        log.warning(
            "validate_device_state.drift_detected",
            device_id=device_id,
            drift_count=len(drift),
            drift_items=drift,
        )

    return ValidationResult(device_id=device_id, passed=passed, drift_detected=drift)


async def _validate_via_napalm(device_id: str, expected: DeviceIntent) -> list[str]:
    """
    Connect to the device via NAPALM and diff its state against the intent.

    Credentials are sourced from environment variables; in production these
    would be injected via Ansible Vault or a secrets manager.
    """
    mgmt_ip = expected.primary_ip.split("/")[0]
    username = os.getenv("DEVICE_USERNAME", "admin")
    password = os.getenv("DEVICE_PASSWORD", "")

    driver = napalm.get_network_driver("ios")
    device = driver(hostname=mgmt_ip, username=username, password=password)

    # NAPALM is synchronous — run all blocking calls in a thread pool.
    await asyncio.to_thread(device.open)
    try:
        live_interfaces = await asyncio.to_thread(device.get_interfaces)
        live_bgp = await asyncio.to_thread(device.get_bgp_neighbors)
    finally:
        await asyncio.to_thread(device.close)

    return _diff_state(expected, live_interfaces, live_bgp)


def _diff_state(
    expected: DeviceIntent,
    live_interfaces: dict,  # type: ignore[type-arg]
    live_bgp: dict,  # type: ignore[type-arg]
) -> list[str]:
    """
    Diff DeviceIntent fields against NAPALM-retrieved live state.

    Returns a list of human-readable discrepancy strings, empty on full convergence.
    """
    drift: list[str] = []

    # Check each expected interface exists and is in the correct state.
    for iface in expected.interfaces:
        live = live_interfaces.get(iface.name)
        if live is None:
            drift.append(f"interface {iface.name} missing from running config")
            continue
        if iface.enabled and not live.get("is_up", False):
            drift.append(f"interface {iface.name} is down — expected up")

    # Check BGP peer is established.
    if expected.bgp_peer_ip:
        global_table = live_bgp.get("global", {})
        peers = global_table.get("peers", {})
        peer = peers.get(expected.bgp_peer_ip, {})
        if not peer.get("is_up", False):
            state = peer.get("description", "unknown")
            drift.append(f"BGP neighbor {expected.bgp_peer_ip} not established (state={state})")

    return drift


def _simulate_validation() -> list[str]:
    """Return simulated drift items at the configured probability."""
    if random.random() > _DRIFT_PROBABILITY:
        return []

    count = random.randint(1, 2)
    return random.sample(_DRIFT_SCENARIOS, k=min(count, len(_DRIFT_SCENARIOS)))
