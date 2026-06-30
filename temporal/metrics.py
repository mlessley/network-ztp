from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

workflow_started = Counter(
    "ztp_workflow_started_total",
    "Workflows submitted",
    ["phase"],
)
workflow_completed = Counter(
    "ztp_workflow_completed_total",
    "Workflows completed",
    ["phase", "status"],
)
drift_detected = Counter(
    "ztp_drift_detected_total",
    "Compliance drift events detected",
    ["site_id"],
)
hitl_pending = Gauge(
    "ztp_hitl_pending_total",
    "Workflows currently awaiting HITL approval",
)
hitl_resolution_seconds = Histogram(
    "ztp_hitl_resolution_duration_seconds",
    "Time from drift detection to HITL resolution",
    buckets=[300, 900, 1800, 3600, 7200, 14400, 86400],
)
onboarding_sites = Gauge(
    "ztp_onboarding_sites_total",
    "Sites by onboarding state",
    ["status"],
)
