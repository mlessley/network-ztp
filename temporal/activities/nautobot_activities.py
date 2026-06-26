"""
Nautobot integration activities.

These activities own the Nautobot API boundary. Each one issues a single,
focused API call so that Temporal can retry them independently. Network calls
to Nautobot are idempotent reads (fetch) or status-flag writes (write_status),
making them safe to retry without side effects on the device itself.

In production these activities would hit a real Nautobot instance. The mock
paths are clearly delimited so swapping them for real HTTP calls requires only
removing the early-return stubs and pointing at the real NAUTOBOT_URL.
"""

from __future__ import annotations

import logging
import os

import httpx
from temporalio import activity

from temporal.models import (
    DeviceIntent,
    InterfaceIntent,
    ProvisioningStatus,
    VlanIntent,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — pulled from environment so nothing is hard-coded in source.
# ---------------------------------------------------------------------------
NAUTOBOT_URL = os.getenv("NAUTOBOT_URL", "http://localhost:8080")
NAUTOBOT_TOKEN = os.getenv("NAUTOBOT_TOKEN", "mock-token")

_GRAPHQL_HEADERS = {
    "Authorization": f"Token {NAUTOBOT_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# GraphQL query — mirrors what a real Nautobot integration would send.
# The query returns everything the provisioning pipeline needs in one round
# trip, avoiding N+1 calls for interfaces, VLANs, and config contexts.
# ---------------------------------------------------------------------------
_DEVICE_INTENT_QUERY = """
query DeviceIntent($device_id: ID!) {
  device(id: $device_id) {
    id
    name
    platform {
      slug
    }
    primary_ip4 {
      address
    }
    interfaces {
      name
      description
      enabled
      ip_addresses {
        address
      }
      tagged_vlans {
        vid
        name
        description
      }
    }
    config_context
  }
}
"""


@activity.defn
async def fetch_device_intent(device_id: str) -> DeviceIntent:
    """
    Fetch the desired device state from Nautobot via GraphQL.

    Issues a single GraphQL query that returns the device record, its
    interfaces, VLAN assignments, and config context in one shot. The config
    context carries site-policy values (NTP, syslog, BGP ASN) that Nautobot
    inherits from region/site/role hierarchies — callers never need to know
    which level a value came from.

    Args:
        device_id: Nautobot device UUID.

    Returns:
        Fully-populated DeviceIntent representing the current desired state.

    Raises:
        httpx.HTTPStatusError: On a non-2xx response from Nautobot.
    """
    activity.logger.info("Fetching device intent from Nautobot for device_id=%s", device_id)

    # ------------------------------------------------------------------
    # MOCK PATH — replace the block below with the real httpx call once
    # a Nautobot instance is available.  The structure is intentionally
    # identical to what the real GraphQL response would look like so the
    # parsing code below is exercised in both modes.
    # ------------------------------------------------------------------
    USE_MOCK = True  # flip to False and set NAUTOBOT_TOKEN to go live

    if not USE_MOCK:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{NAUTOBOT_URL}/graphql/",
                headers=_GRAPHQL_HEADERS,
                json={"query": _DEVICE_INTENT_QUERY, "variables": {"device_id": device_id}},
            )
            response.raise_for_status()
            payload = response.json()
            raw_device = payload["data"]["device"]
    else:
        activity.logger.info("[MOCK] Returning simulated Nautobot GraphQL response")
        raw_device = _mock_graphql_response(device_id)

    # ------------------------------------------------------------------
    # Parse the GraphQL shape into our canonical DeviceIntent model.
    # This translation layer is intentionally explicit — it makes schema
    # mismatches between Nautobot and our models immediately visible.
    # ------------------------------------------------------------------
    interfaces: list[InterfaceIntent] = []
    vlans: list[VlanIntent] = []
    seen_vlans: set[int] = set()

    for iface in raw_device.get("interfaces", []):
        ip_address = ""
        if iface.get("ip_addresses"):
            ip_address = iface["ip_addresses"][0]["address"]

        interfaces.append(
            InterfaceIntent(
                name=iface["name"],
                description=iface.get("description", ""),
                ip_address=ip_address,
                enabled=iface.get("enabled", True),
            )
        )

        for vlan in iface.get("tagged_vlans", []):
            if vlan["vid"] not in seen_vlans:
                seen_vlans.add(vlan["vid"])
                vlans.append(
                    VlanIntent(
                        vlan_id=vlan["vid"],
                        name=vlan["name"],
                        description=vlan.get("description", ""),
                    )
                )

    ctx = raw_device.get("config_context", {})

    intent = DeviceIntent(
        device_id=raw_device["id"],
        hostname=raw_device["name"],
        platform=raw_device["platform"]["slug"],
        primary_ip=raw_device["primary_ip4"]["address"],
        interfaces=interfaces,
        vlans=vlans,
        bgp_asn=ctx.get("bgp_asn", 65000),
        bgp_peer_ip=ctx.get("bgp_peer_ip", ""),
        bgp_peer_asn=ctx.get("bgp_peer_asn", 64512),
        ntp_servers=ctx.get("ntp_servers", []),
        syslog_servers=ctx.get("syslog_servers", []),
        default_gateway=ctx.get("default_gateway", ""),
    )

    activity.logger.info(
        "Device intent fetched: hostname=%s platform=%s interfaces=%d vlans=%d",
        intent.hostname,
        intent.platform,
        len(intent.interfaces),
        len(intent.vlans),
    )
    return intent


@activity.defn
async def write_provisioning_status(
    device_id: str,
    status: str,
    workflow_id: str,
) -> None:
    """
    Write a provisioning lifecycle status back to the Nautobot device record.

    Uses the Nautobot REST API to PATCH two custom fields on the device:
      - ``ztp_provisioning_status``: the current lifecycle stage
      - ``ztp_workflow_id``: the Temporal workflow ID for cross-system tracing

    Storing the workflow ID in Nautobot allows operations engineers to jump from
    a device record directly to the Temporal UI to inspect logs, without needing
    to know the Temporal workflow ID in advance.

    Args:
        device_id: Nautobot device UUID.
        status: One of the ProvisioningStatus enum values.
        workflow_id: Temporal workflow execution ID.
    """
    activity.logger.info(
        "Writing provisioning status to Nautobot: device_id=%s status=%s workflow_id=%s",
        device_id,
        status,
        workflow_id,
    )

    patch_payload = {
        "custom_fields": {
            "ztp_provisioning_status": status,
            "ztp_workflow_id": workflow_id,
        }
    }

    USE_MOCK = True  # flip to False once Nautobot is reachable

    if not USE_MOCK:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.patch(
                f"{NAUTOBOT_URL}/api/dcim/devices/{device_id}/",
                headers={
                    "Authorization": f"Token {NAUTOBOT_TOKEN}",
                    "Content-Type": "application/json",
                },
                json=patch_payload,
            )
            response.raise_for_status()
            activity.logger.info("Nautobot PATCH succeeded for device_id=%s", device_id)
    else:
        activity.logger.info(
            "[MOCK] Would PATCH %s/api/dcim/devices/%s/ with payload: %s",
            NAUTOBOT_URL,
            device_id,
            patch_payload,
        )

    # Validate the status value is a known enum member (catches typos early).
    try:
        ProvisioningStatus(status)
    except ValueError:
        activity.logger.warning("Unknown provisioning status written: %s", status)


# ---------------------------------------------------------------------------
# Mock data helpers — realistic enough to exercise the full pipeline without
# a live Nautobot instance.
# ---------------------------------------------------------------------------


def _mock_graphql_response(device_id: str) -> dict:  # type: ignore[type-arg]
    """Return a GraphQL response shape for a realistic Cisco branch router."""
    return {
        "id": device_id,
        "name": f"br-{device_id.lower()}-rtr01",
        "platform": {"slug": "cisco_ios_xe"},
        "primary_ip4": {"address": "10.100.255.1/32"},
        "interfaces": [
            {
                "name": "GigabitEthernet0/0/0",
                "description": "MPLS WAN — ISP-A",
                "enabled": True,
                "ip_addresses": [{"address": "203.0.113.2/30"}],
                "tagged_vlans": [],
            },
            {
                "name": "GigabitEthernet0/0/1",
                "description": "LAN Trunk to Core Switch",
                "enabled": True,
                "ip_addresses": [],
                "tagged_vlans": [
                    {"vid": 100, "name": "CORP-DATA", "description": "Corporate workstations"},
                    {"vid": 200, "name": "VOICE", "description": "IP telephony"},
                    {"vid": 999, "name": "MGMT", "description": "Out-of-band management"},
                ],
            },
            {
                "name": "Loopback0",
                "description": "Router-ID / BGP source",
                "enabled": True,
                "ip_addresses": [{"address": "10.100.255.1/32"}],
                "tagged_vlans": [],
            },
        ],
        "config_context": {
            "bgp_asn": 65001,
            "bgp_peer_ip": "203.0.113.1",
            "bgp_peer_asn": 64512,
            "ntp_servers": ["10.0.0.1", "10.0.0.2"],
            "syslog_servers": ["10.0.1.100"],
            "default_gateway": "10.100.255.254",
        },
    }


# ---------------------------------------------------------------------------
# Day 2 — site-level device listing
# ---------------------------------------------------------------------------

_SITE_DEVICES_QUERY = """
query SiteDevices($site_id: ID!) {
  site(id: $site_id) {
    devices {
      id
      name
    }
  }
}
"""


@activity.defn
async def fetch_site_devices(site_id: str) -> list[str]:
    """
    Fetch the list of device IDs for a site from Nautobot.

    Used by the Day 2 compliance scan to determine which devices to validate.
    Returns only device UUIDs — the compliance workflow fetches full intent
    for each device individually so retries are scoped per device.

    Args:
        site_id: Nautobot site UUID or slug.

    Returns:
        List of Nautobot device UUIDs for all devices at the site.
    """
    activity.logger.info("Fetching device list for site_id=%s from Nautobot", site_id)

    USE_MOCK = True

    if not USE_MOCK:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{NAUTOBOT_URL}/graphql/",
                headers=_GRAPHQL_HEADERS,
                json={"query": _SITE_DEVICES_QUERY, "variables": {"site_id": site_id}},
            )
            response.raise_for_status()
            payload = response.json()
            devices = payload["data"]["site"]["devices"]
            return [d["id"] for d in devices]

    # Mock: return a handful of realistic device IDs for a medium-sized site.
    mock_ids = [f"DEV{str(i).zfill(3)}" for i in range(1, 6)]
    activity.logger.info(
        "[MOCK] Returning %d mock device IDs for site_id=%s", len(mock_ids), site_id
    )
    return mock_ids
