"""
Post-push validation activities.

After Ansible pushes the configuration we reconnect to the device (via Netmiko
or NAPALM in production) and compare its running state against the DeviceIntent
that drove the push.  Any discrepancy is surfaced as a ``drift_detected`` item.

If validation fails, the workflow triggers a Human-in-the-Loop (HITL) signal
rather than auto-remediating. On production infrastructure, unexpected drift
requires a human decision: is this a known-good deviation (e.g. emergency
out-of-band change) or a push failure that needs rollback?
"""

from __future__ import annotations

import asyncio
import random

from temporalio import activity

from temporal.models import DeviceIntent, ValidationResult

# Realistic drift scenarios a network engineer would recognize.
_DRIFT_SCENARIOS: list[str] = [
    "interface GigabitEthernet0/0/0 IP mismatch: expected 203.0.113.2/30, got 203.0.113.6/30",
    "VLAN 100 missing from trunk port GigabitEthernet0/0/1",
    "BGP neighbor 203.0.113.1 not established (state=Idle, expected=Established)",
    "NTP server 10.0.0.1 unreachable — clock may be unsynchronized",
    "hostname mismatch: expected br-dev001-rtr01, running config shows OLD-BR-RTR",
    "interface Loopback0 shutdown — expected no shutdown",
    "VLAN 200 name mismatch: expected VOICE, found VOICE-OLD",
    "logging host 10.0.1.100 missing from running config",
]

# Fraction of validations that detect drift — represents realistic transient
# push failures, stale Ansible cached facts, or concurrent out-of-band changes.
_DRIFT_PROBABILITY = 0.10


@activity.defn
async def validate_device_state(
    device_id: str,
    expected: DeviceIntent,
) -> ValidationResult:
    """
    Compare live device running-state against the declared intent.

    In production this activity would:
      1. Open a Netmiko SSH session to the device's management IP.
      2. Run ``show running-config`` and parse it with CiscoConfParse.
      3. Diff each field in ``expected`` against the parsed output.
      4. Return the list of discrepancies as human-readable strings.

    The simulation below matches the expected statistical behaviour: 90 % pass,
    10 % fail with one or two realistic drift items.  This lets developers
    exercise the HITL escalation path during local testing without needing a
    real device.

    Args:
        device_id: Device to validate (used as SSH target in production).
        expected: The DeviceIntent the device should now reflect.

    Returns:
        ValidationResult with passed=True and empty drift_detected on success,
        or passed=False with one or more drift descriptions on failure.
    """
    activity.logger.info(
        "Validating device state for device_id=%s hostname=%s",
        device_id,
        expected.hostname,
    )

    # Simulate the SSH round-trip and config parsing latency.
    await asyncio.sleep(1)

    if random.random() > _DRIFT_PROBABILITY:
        activity.logger.info("Validation passed — no drift detected for device_id=%s", device_id)
        return ValidationResult(device_id=device_id, passed=True, drift_detected=[])

    # Choose 1–2 realistic drift items for the failure case.
    drift_count = random.randint(1, 2)
    drift_items = random.sample(_DRIFT_SCENARIOS, k=min(drift_count, len(_DRIFT_SCENARIOS)))

    activity.logger.warning(
        "Validation FAILED for device_id=%s — drift detected: %s",
        device_id,
        drift_items,
    )
    return ValidationResult(device_id=device_id, passed=False, drift_detected=drift_items)
