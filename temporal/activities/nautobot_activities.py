"""
Nautobot integration activities.

Boundary split:
  - GraphQL queries (fetch_device_intent, fetch_site_devices) use httpx2 directly.
    pynautobot does not expose an async GraphQL interface; httpx2 gives us full
    async control over the request lifecycle.
  - REST CRUD writes (write_provisioning_status) use pynautobot, which provides
    a typed client over the Nautobot REST API.  Sync calls are wrapped in
    asyncio.to_thread() so they don't block the event loop.

Mock control:
    Each function checks the module-level _USE_MOCK flag.  Set it to False (or set
    ZTP_USE_MOCK=false in the environment) to hit a real Nautobot instance.  The mock
    data shapes are identical to what the real API returns so the parsing code runs
    in both modes.
"""

from __future__ import annotations

import asyncio
import os

import httpx2 as httpx
import pynautobot
import structlog

from temporal.models import (
    DeviceIntent,
    InterfaceIntent,
    ProvisioningStatus,
    VlanIntent,
)

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NAUTOBOT_URL = os.getenv("NAUTOBOT_URL", "http://localhost:8080")
NAUTOBOT_TOKEN = os.getenv("NAUTOBOT_TOKEN", "mock-token")

# Flip to False (or set ZTP_USE_MOCK=false) to call a real Nautobot instance.
_USE_MOCK: bool = os.getenv("ZTP_USE_MOCK", "true").lower() != "false"

_GRAPHQL_HEADERS = {
    "Authorization": f"Token {NAUTOBOT_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

_DEVICE_INTENT_QUERY = """
query DeviceIntent($device_id: ID!) {
  device(id: $device_id) {
    id
    name
    platform { slug }
    primary_ip4 { address }
    interfaces {
      name
      description
      enabled
      ip_addresses { address }
      tagged_vlans { vid name description }
    }
    config_context
  }
}
"""

_SITE_DEVICES_QUERY = """
query SiteDevices($site_id: ID!) {
  site(id: $site_id) {
    devices { id name }
  }
}
"""

# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

from temporalio import activity  # noqa: E402 — must follow top-level imports


@activity.defn
async def fetch_device_intent(device_id: str) -> DeviceIntent:
    """
    Fetch the desired device state from Nautobot via GraphQL.

    Issues a single query that returns the device record, interfaces, VLAN
    assignments, and config context in one round trip.  Config context carries
    site-policy values (NTP, syslog, BGP ASN) inherited from Nautobot's
    region/site/role hierarchy — callers never need to know which level a
    value came from.

    Args:
        device_id: Nautobot device UUID.

    Returns:
        Fully-populated DeviceIntent representing the current desired state.
    """
    log.info("fetch_device_intent.started", device_id=device_id)

    if _USE_MOCK:
        log.debug("fetch_device_intent.mock", device_id=device_id)
        raw_device = _mock_graphql_response(device_id)
    else:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{NAUTOBOT_URL}/graphql/",
                headers=_GRAPHQL_HEADERS,
                json={"query": _DEVICE_INTENT_QUERY, "variables": {"device_id": device_id}},
            )
            response.raise_for_status()
            raw_device = response.json()["data"]["device"]

    intent = _parse_device_intent(raw_device)

    log.info(
        "fetch_device_intent.complete",
        device_id=device_id,
        hostname=intent.hostname,
        platform=intent.platform,
        interface_count=len(intent.interfaces),
        vlan_count=len(intent.vlans),
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

    PATCHes two custom fields on the device:
      - ``ztp_provisioning_status``: current lifecycle stage
      - ``ztp_workflow_id``: Temporal workflow ID for cross-system tracing

    Storing the Temporal workflow ID in Nautobot lets engineers jump from a
    device record directly to the Temporal UI without knowing the ID in advance.

    Uses pynautobot for REST PATCH (wrapped in asyncio.to_thread for async safety).
    """
    log.info(
        "write_provisioning_status.started",
        device_id=device_id,
        status=status,
        workflow_id=workflow_id,
    )

    # Validate status value early — catches typos before the API call.
    try:
        ProvisioningStatus(status)
    except ValueError:
        log.warning("write_provisioning_status.unknown_status", status=status)

    patch_payload = {
        "custom_fields": {
            "ztp_provisioning_status": status,
            "ztp_workflow_id": workflow_id,
        }
    }

    if _USE_MOCK:
        log.debug(
            "write_provisioning_status.mock",
            url=f"{NAUTOBOT_URL}/api/dcim/devices/{device_id}/",
            payload=patch_payload,
        )
        return

    # pynautobot is sync — wrap in to_thread so we don't block the event loop.
    def _patch() -> None:
        nb = pynautobot.api(url=NAUTOBOT_URL, token=NAUTOBOT_TOKEN)
        nb.dcim.devices.update([{"id": device_id, "custom_fields": patch_payload["custom_fields"]}])

    await asyncio.to_thread(_patch)
    log.info("write_provisioning_status.complete", device_id=device_id, status=status)


@activity.defn
async def fetch_site_devices(site_id: str) -> list[str]:
    """
    Fetch the list of device IDs for a site from Nautobot.

    Returns only device UUIDs — the compliance workflow fetches full intent
    for each device individually so retries are scoped per device.
    """
    log.info("fetch_site_devices.started", site_id=site_id)

    if _USE_MOCK:
        mock_ids = [f"DEV{str(i).zfill(3)}" for i in range(1, 6)]
        log.debug("fetch_site_devices.mock", site_id=site_id, count=len(mock_ids))
        return mock_ids

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{NAUTOBOT_URL}/graphql/",
            headers=_GRAPHQL_HEADERS,
            json={"query": _SITE_DEVICES_QUERY, "variables": {"site_id": site_id}},
        )
        response.raise_for_status()
        devices = response.json()["data"]["site"]["devices"]

    ids = [d["id"] for d in devices]
    log.info("fetch_site_devices.complete", site_id=site_id, count=len(ids))
    return ids


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_device_intent(raw: dict) -> DeviceIntent:  # type: ignore[type-arg]
    """Translate a Nautobot GraphQL device payload into a DeviceIntent."""
    interfaces: list[InterfaceIntent] = []
    vlans: list[VlanIntent] = []
    seen_vlans: set[int] = set()

    for iface in raw.get("interfaces", []):
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

    ctx = raw.get("config_context", {})

    return DeviceIntent(
        device_id=raw["id"],
        hostname=raw["name"],
        platform=raw["platform"]["slug"],
        primary_ip=raw["primary_ip4"]["address"],
        interfaces=interfaces,
        vlans=vlans,
        bgp_asn=ctx.get("bgp_asn", 65000),
        bgp_peer_ip=ctx.get("bgp_peer_ip", ""),
        bgp_peer_asn=ctx.get("bgp_peer_asn", 64512),
        ntp_servers=ctx.get("ntp_servers", []),
        syslog_servers=ctx.get("syslog_servers", []),
        default_gateway=ctx.get("default_gateway", ""),
    )


# ---------------------------------------------------------------------------
# Mock data — realistic enough to exercise the full pipeline without Nautobot
# ---------------------------------------------------------------------------


def _mock_graphql_response(device_id: str) -> dict:  # type: ignore[type-arg]
    """Return a GraphQL response shape for a realistic Cisco router."""
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
            "snmp_community": "public-ro",
            "snmp_location": f"Site-{device_id}",
            "default_gateway": "10.100.255.254",
        },
    }
