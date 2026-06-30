"""
Pydantic models that flow between Temporal activities and workflows.

These are the canonical data contracts for the ZTP pipeline. Using Pydantic
ensures that every value crossing an activity boundary is validated and
serializable — Temporal serializes activity inputs/outputs as JSON, so
correctness of the data shape is checked at the Python level before it ever
hits the wire.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field


class ProvisioningStatus(StrEnum):
    """
    Device lifecycle states written back to Nautobot as a custom field.

    The progression across all three phases:

      Day 0 (bootstrap):
        QUEUED → BOOTSTRAP_STARTED → DHCP_RESERVED → BOOTSTRAP_SCRIPT_READY
        → AWAITING_DEVICE_CHECKIN → BOOTSTRAP_COMPLETE

      Day 1 (provisioning):
        PROVISIONING_STARTED → CONFIG_RENDERED → CONFIG_PUSHED
        → VALIDATION_PASSED | AWAITING_HUMAN_APPROVAL → COMPLETE

      Day 2 (compliance):
        COMPLIANCE_SCAN_STARTED → COMPLIANCE_PASSED | COMPLIANCE_DRIFTED

      Terminal failure state (any phase):
        FAILED
    """

    # Shared / initial
    QUEUED = "QUEUED"
    FAILED = "FAILED"

    # Day 0 — bootstrap
    BOOTSTRAP_STARTED = "BOOTSTRAP_STARTED"
    DHCP_RESERVED = "DHCP_RESERVED"
    BOOTSTRAP_SCRIPT_READY = "BOOTSTRAP_SCRIPT_READY"
    AWAITING_DEVICE_CHECKIN = "AWAITING_DEVICE_CHECKIN"
    BOOTSTRAP_COMPLETE = "BOOTSTRAP_COMPLETE"

    # Day 1 — intent provisioning
    PROVISIONING_STARTED = "PROVISIONING_STARTED"
    CONFIG_RENDERED = "CONFIG_RENDERED"
    CONFIG_PUSHED = "CONFIG_PUSHED"
    VALIDATION_PASSED = "VALIDATION_PASSED"
    AWAITING_HUMAN_APPROVAL = "AWAITING_HUMAN_APPROVAL"
    COMPLETE = "COMPLETE"

    # Day 2 — compliance
    COMPLIANCE_SCAN_STARTED = "COMPLIANCE_SCAN_STARTED"
    COMPLIANCE_PASSED = "COMPLIANCE_PASSED"
    COMPLIANCE_DRIFTED = "COMPLIANCE_DRIFTED"

    # Day 0.5 — site onboarding
    ONBOARD_PENDING = "ONBOARD_PENDING"
    ONBOARD_DISCOVERING = "ONBOARD_DISCOVERING"
    ONBOARD_DISCOVERED = "ONBOARD_DISCOVERED"
    ONBOARD_RECONCILING = "ONBOARD_RECONCILING"
    ONBOARD_MANAGED = "ONBOARD_MANAGED"


# ---------------------------------------------------------------------------
# Shared sub-models
# ---------------------------------------------------------------------------


class InterfaceIntent(BaseModel):
    """Desired state for a single router interface."""

    name: str = Field(..., description="IOS interface name, e.g. GigabitEthernet0/0/0")
    description: str = Field(default="", description="Human-readable circuit or purpose label")
    ip_address: str = Field(
        default="", description="IPv4 address in CIDR notation, e.g. 10.1.1.1/30"
    )
    enabled: bool = Field(default=True)
    vrf: str = Field(default="", description="VRF name; empty string means global routing table")


class VlanIntent(BaseModel):
    """Desired state for a VLAN on the site device."""

    vlan_id: int = Field(..., ge=1, le=4094)
    name: str
    description: str = Field(default="")


class DeviceIntent(BaseModel):
    """
    Complete desired-state record for a single device, sourced from Nautobot.

    This is the canonical 'intent' that flows through the entire provisioning
    pipeline. Every downstream activity (render, push, validate) derives its
    inputs from this object — keeping Nautobot as the single source of truth
    rather than letting individual activities make their own API calls.
    """

    device_id: str = Field(..., description="Nautobot device UUID")
    hostname: str
    platform: str = Field(..., description="Nautobot platform slug, e.g. cisco_ios_xe")
    primary_ip: str = Field(..., description="Management IP in CIDR notation")
    interfaces: list[InterfaceIntent] = Field(default_factory=list)
    vlans: list[VlanIntent] = Field(default_factory=list)
    provisioning_status: ProvisioningStatus = Field(default=ProvisioningStatus.QUEUED)

    # BGP / routing attributes — populated from Nautobot config contexts
    bgp_asn: int = Field(default=65000, description="Site eBGP ASN")
    bgp_peer_ip: str = Field(default="", description="Upstream hub peer IP")
    bgp_peer_asn: int = Field(default=64512, description="Hub ASN")

    # NTP / syslog — global site policy values
    ntp_servers: list[str] = Field(default_factory=list)
    syslog_servers: list[str] = Field(default_factory=list)

    # Day 0 bootstrap fields — used to render the ZTP script
    mgmt_interface: str = Field(
        default="GigabitEthernet0",
        description="Out-of-band management interface used during bootstrap",
    )
    default_gateway: str = Field(
        default="",
        description="Default gateway IP for the management network",
    )


# ---------------------------------------------------------------------------
# Day 1 models
# ---------------------------------------------------------------------------


class RenderedConfig(BaseModel):
    """
    Jinja2-rendered device configuration blob, ready for Ansible to push.

    Storing the rendered text (rather than re-rendering at push time) gives us
    a reproducible artifact: if a push retry fires after Nautobot data changes,
    the retry still pushes the same config that was approved, not a silent
    in-flight mutation.
    """

    device_id: str
    config_content: str = Field(..., description="Full IOS-XE configuration text")
    template_name: str
    rendered_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))


class PushResult(BaseModel):
    """Outcome of an Ansible config-push operation."""

    device_id: str
    success: bool
    output: str = Field(..., description="Ansible stdout/stderr captured from the run")
    duration_seconds: float


class ValidationResult(BaseModel):
    """
    Result of comparing live device state against the declared intent.

    drift_detected contains human-readable descriptions of each discrepancy.
    An empty list means the device is fully converged with Nautobot intent.
    """

    device_id: str
    passed: bool
    drift_detected: list[str] = Field(default_factory=list)


class ProvisionSiteInput(BaseModel):
    """Top-level Day 1 workflow input."""

    device_id: str = Field(..., description="Nautobot device UUID to provision")
    requested_by: str = Field(
        ..., description="Identity of the requester (username or service account)"
    )


class ProvisionSiteResult(BaseModel):
    """Top-level Day 1 workflow result."""

    device_id: str
    success: bool
    workflow_id: str
    completed_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    failure_reason: str = Field(default="", description="Human-readable cause if success=False")


# ---------------------------------------------------------------------------
# Day 0 models
# ---------------------------------------------------------------------------


class BootstrapScript(BaseModel):
    """
    Rendered Cisco IOS-XE ZTP Python script, ready to be served over HTTP.

    The device fetches this script via the URL in DHCP Option 67 and executes
    it using its built-in Python interpreter and ``cli`` module.  The script
    applies the minimal configuration needed to make the device reachable for
    Day 1 provisioning.
    """

    device_id: str
    script_content: str = Field(..., description="Python script text executed by IOS-XE ZTP")
    script_url: str = Field(..., description="HTTP URL the device will fetch this script from")
    rendered_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))


class DhcpReservation(BaseModel):
    """Record of a DHCP reservation created for a bootstrapping device."""

    device_id: str
    mac_address: str
    assigned_ip: str = Field(..., description="IP assigned to this MAC for the bootstrap lease")
    lease_seconds: int = Field(
        default=3600,
        description="Short lease — enough for the device to fetch and run the bootstrap script",
    )


class BootstrapDeviceInput(BaseModel):
    """Top-level Day 0 workflow input."""

    device_id: str = Field(..., description="Nautobot device UUID to bootstrap")
    mac_address: str = Field(..., description="Factory MAC address of the device's management port")
    requested_by: str = Field(
        ..., description="Identity of the requester (username or service account)"
    )


class BootstrapDeviceResult(BaseModel):
    """Top-level Day 0 workflow result."""

    device_id: str
    success: bool
    workflow_id: str
    completed_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    failure_reason: str = Field(default="")


# ---------------------------------------------------------------------------
# Day 2 models
# ---------------------------------------------------------------------------


class DeviceComplianceResult(BaseModel):
    """Per-device result from a compliance scan."""

    device_id: str
    hostname: str
    passed: bool
    drift_detected: list[str] = Field(default_factory=list)


class ComplianceScanInput(BaseModel):
    """
    Top-level Day 2 workflow input.

    If ``device_ids`` is non-empty it is used directly; otherwise the workflow
    fetches all devices for ``site_id`` from Nautobot.
    """

    site_id: str = Field(..., description="Nautobot site UUID or slug to scan")
    requested_by: str
    device_ids: list[str] = Field(
        default_factory=list,
        description="Optional explicit device list — overrides site-based fetch",
    )


class ComplianceScanResult(BaseModel):
    """Aggregate result of a Day 2 compliance scan across a site."""

    site_id: str
    workflow_id: str
    total_devices: int
    passed_count: int
    drifted_count: int
    drifted_devices: list[DeviceComplianceResult] = Field(default_factory=list)
    scanned_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))


# ---------------------------------------------------------------------------
# Day 0.5 models — site onboarding
# ---------------------------------------------------------------------------


class ConfigChange(BaseModel):
    section: str
    description: str
    current: str
    intended: str


class RemediationPlan(BaseModel):
    site_id: str
    device_id: str
    snapshot_id: str
    changes: list[ConfigChange]
    estimated_impact: Literal["low", "medium", "high"]
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))


class OnboardSiteInput(BaseModel):
    site_id: str
    device_id: str
    requested_by: str


class OnboardSiteResult(BaseModel):
    site_id: str
    device_id: str
    success: bool
    workflow_id: str
    completed_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    failure_reason: str = Field(default="")


class BulkOnboardingInput(BaseModel):
    site_ids: list[str]
    sites_per_hour: int = Field(default=50, ge=1, le=500)
    max_concurrent: int = Field(default=10, ge=1, le=50)
    requested_by: str
    region: str = Field(default="SOUTH")


class BulkOnboardingResult(BaseModel):
    total_sites: int
    managed_count: int
    failed_count: int
    completed_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
