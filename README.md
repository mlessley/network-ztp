# network-ztp

Zero-touch provisioning automation for enterprise network deployments at scale (~5,000 devices).

## Overview

Provisioning devices manually is operationally brittle: human error during config generation, incomplete audit trails, no standard path from factory default to fully operational, and no systematic way to detect post-push configuration drift. This project replaces that process with a fully automated, durable pipeline covering all three lifecycle phases:

**Day 0 — Bootstrap:** Device arrives from factory, boots with no config, fetches and executes a minimal IOS-XE ZTP script via DHCP, and becomes reachable over SSH.

**Day 1 — Provisioning:** Full intent from Nautobot is rendered to a device config via Jinja2 and pushed via Ansible. Post-push validation compares live state to intent, with HITL escalation on drift.

**Day 2 — Compliance:** Periodic re-validation of all devices at a site against current Nautobot intent. Drifted devices are flagged for engineer review and re-provisioning.

All three phases run inside [Temporal](https://temporal.io), which provides durable execution: if the worker crashes mid-run, the workflow resumes from exactly where it left off when the worker restarts, with full audit history preserved.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         network-ztp Pipeline                                     │
│                                                                                  │
│  ┌──────────┐  GraphQL/REST  ┌──────────────────────────────────────────────┐   │
│  │          │───────────────▶│              Temporal Worker                 │   │
│  │ Nautobot │                │                                              │   │
│  │ (intent) │◀──────────────│  DAY 0: BootstrapDeviceWorkflow              │   │
│  │          │  PATCH status  │    DHCP reserve → render script →           │   │
│  └──────────┘                │    publish → wait checkin → [Day 1 child]   │   │
│                              │                                              │   │
│  ┌──────────┐                │  DAY 1: ProvisionSiteWorkflow             │   │
│  │   DHCP   │◀──────────────│    fetch intent → render → push → validate  │   │
│  │  Server  │  reserve MAC   │                               │             │   │
│  └──────────┘                │                    drift?     ▼             │   │
│                              │                         HITL signal         │   │
│  ┌──────────┐                │                         (24h wait)          │   │
│  │ Bootstrap│◀──────────────│    publish ZTP script                       │   │
│  │  Server  │  serve script  │                                              │   │
│  └──────────┘                │  DAY 2: ComplianceScanWorkflow              │   │
│                              │    fetch site devices → validate each →     │   │
│  ┌──────────┐                │    report drifted devices                   │   │
│  │  Human   │───signal──────▶│                                              │   │
│  │ Engineer │                └──────────────────────┬───────────────────────┘   │
│  └──────────┘                                       │                           │
│       ▲                                       Ansible runner                    │
│       │  fix intent &                               │                           │
│       │  re-run to recover                          ▼                           │
│                                           ┌──────────────────┐                  │
│                                           │  Network Device   │                 │
│                                           │  (Cisco IOS-XE)   │                 │
│                                           └──────────────────┘                  │
└──────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│                      Component Roles                           │
│                                                                │
│  Nautobot     ── Source of Intent: what every device should be│
│  Temporal     ── Durable orchestrator: survives crashes        │
│  DHCP Server  ── MAC reservation + Option 67 bootstrap URL    │
│  Bootstrap    ── HTTP server for per-device ZTP scripts        │
│  Jinja2       ── Config renderer: intent → IOS-XE text        │
│  Ansible      ── Config delivery: SSH push + idempotency      │
│  Prometheus   ── Metrics: queue depth, latency, errors        │
└────────────────────────────────────────────────────────────────┘
```

---

## Three-phase lifecycle

### Day 0 — Bootstrap

A device arrives from the factory with no configuration. The `BootstrapDeviceWorkflow` is triggered (by Nautobot webhook or CLI) as soon as the device record is created in Nautobot:

1. **Register DHCP reservation** — pre-create a host reservation for the device's MAC address so its first DHCP Discover returns the right management IP and Option 67 bootstrap URL.
2. **Render ZTP script** — generate a minimal Cisco IOS-XE Python script from Nautobot intent. The script configures only what is needed to reach the management network: hostname, mgmt interface IP, default route, and SSH.
3. **Publish script** — write the rendered script to the HTTP file server at the Option 67 URL.
4. **Wait for check-in** — park the workflow (up to 8 hours) until the device's management IP responds to SSH, signalling the bootstrap script ran successfully.
5. **Trigger Day 1** — hand off to `ProvisionSiteWorkflow` as a child workflow.

The 8-hour wait covers real-world logistics: devices ship to remote sites, sit in receiving, get racked at the end of a shift.

### Day 1 — Intent Provisioning

`ProvisionSiteWorkflow` delivers the full desired state from Nautobot to the device:

1. Fetch complete device intent from Nautobot (GraphQL).
2. Render full IOS-XE configuration via Jinja2.
3. Push configuration via Ansible.
4. Validate post-push state — compare live device config to Nautobot intent.
5. If drift is detected: park and wait for a human `approve_escalation` signal (24-hour timeout).

### Day 2 — Compliance

`ComplianceScanWorkflow` runs on a schedule (recommended: every 4–6 hours) to catch drift that occurs between provisioning runs — out-of-band changes, partial failures, or intent updates not yet applied:

1. Fetch all device IDs for a site from Nautobot.
2. For each device: fetch current intent, validate live state.
3. Return a `ComplianceScanResult` with per-device pass/drift breakdown.
4. Drifted devices are flagged for engineer review. Recovery is a new `ProvisionSiteWorkflow` run after the engineer confirms the drift is unintended.

---

## Why each component exists

### Nautobot as Source of Intent

Nautobot is not just an inventory tool here — it is the **single authoritative description of what every device should look like**. IP addresses, VLANs, BGP ASNs, NTP servers, syslog targets, management gateways — all live in Nautobot's structured data model and config contexts. The ZTP pipeline never asks individual engineers what a device should be configured with; it reads Nautobot.

This matters at scale: inconsistency between what Nautobot says and what devices actually run is the root cause of the majority of outage tickets in large networks. Closing that loop — provisioning *from* Nautobot and *validating back to* Nautobot — eliminates an entire class of drift.

### Temporal for Durable Execution

Config pushes take minutes. Devices go offline mid-push. Workers crash. At scale these failures are not edge cases — they are daily events.

Temporal solves this with **workflow history**: every activity input, output, and side effect is written to a persistent event log before execution proceeds. If the worker process dies after Ansible pushes a config but before the validation result is written back to Nautobot, the workflow replays from the push step when the worker restarts — it does not re-push, it continues from where it left off.

Temporal also provides the 8-hour Day 0 wait for free: the workflow parks without holding a thread, consumes no resources, and resumes the instant the device checks in — even if the worker restarted in between.

### Roll-Forward Always

This system never pushes a "rollback" config to a device. The reasoning:

- **The device's live state is not frozen.** By the time a failure is detected, BGP sessions may have reconverged, DHCP leases may have been handed out, spanning tree may have re-elected. A config snapshot taken before the push has an unknown relationship to that new reality.
- **Nautobot is the only source of truth.** Pushing a config that Nautobot does not currently describe deliberately creates the drift this pipeline exists to eliminate.
- **Scripted undo lists assume a human is watching.** In a manually-executed maintenance window, an engineer validates each rollback step in real time. In continuous automated provisioning at scale there is no such window.

On any failure the device is left in whatever state the push reached, and Nautobot is set to `FAILED`. Recovery is always a forward action: fix the intent in Nautobot, submit a new provisioning run.

### Human-in-the-Loop Escalation

Automated validation cannot make every decision. When post-push state diverges from intent, the workflow parks and waits for an operator signal:

- `approved` — the drift is acceptable (e.g. a known emergency out-of-band change). Mark COMPLETE.
- `rejected` — the drift is not acceptable. Mark FAILED. The engineer fixes Nautobot intent and re-runs.

The 24-hour timeout accommodates enterprise change processes that may span time zones or business-hours-only review queues.

---

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- Temporal server running at `localhost:7233`
  ```bash
  # Quickstart with Docker:
  docker run --rm -p 7233:7233 -p 8080:8080 temporalio/auto-setup:latest
  ```
- (Optional) Nautobot at `localhost:8080` — the pipeline runs in mock mode without it

---

## Local development setup

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd network-ztp

# 2. Install all dependencies (runtime + dev tools)
uv sync

# 3. Copy environment config and edit as needed
cp .env.example .env

# 4. Start the worker (in one terminal)
uv run python temporal/worker.py
```

---

## Running the pipeline end-to-end

### Day 0 — bootstrap a new device from factory

```bash
uv run python temporal/run_workflow.py bootstrap \
  --device-id DEV001 \
  --mac aa:bb:cc:dd:ee:ff \
  --requested-by mark
```

The workflow will:
1. Register the DHCP reservation
2. Render and publish the IOS-XE ZTP script
3. Wait for the device to come online (up to 8 hours)
4. Automatically run Day 1 provisioning once the device checks in

### Day 1 — provision an already-reachable device directly

```bash
uv run python temporal/run_workflow.py start \
  --device-id DEV001 \
  --requested-by mark
```

### Day 2 — run a compliance scan across a site

```bash
# Scan all devices at a site
uv run python temporal/run_workflow.py scan \
  --site-id SITE001 \
  --requested-by mark

# Scan a specific subset of devices
uv run python temporal/run_workflow.py scan \
  --site-id SITE001 \
  --device-ids DEV001,DEV002,DEV003 \
  --requested-by mark
```

### Check workflow status

```bash
uv run python temporal/run_workflow.py status --workflow-id <id>
uv run python temporal/run_workflow.py list
```

---

## Testing the HITL escalation path

Validation fails ~10% of the time by default (configurable via `_DRIFT_PROBABILITY` in `validation_activities.py`). To force a HITL scenario:

```python
# In temporal/activities/validation_activities.py
_DRIFT_PROBABILITY = 1.0  # always drift
```

Then start a Day 1 run and send the approval signal:

```bash
uv run python temporal/run_workflow.py start --device-id DEV001 --requested-by mark
# workflow parks at AWAITING_HUMAN_APPROVAL

uv run python temporal/run_workflow.py approve --workflow-id <id> --decision approved
# or --decision rejected to mark FAILED and leave recovery to a new run
```

---

## Scheduling Day 2 compliance scans

Register a Temporal schedule to run compliance automatically:

```bash
temporal schedule create \
  --schedule-id "compliance-site001-4h" \
  --cron-schedule "0 */4 * * *" \
  --workflow-type ComplianceScanWorkflow \
  --task-queue ztp-queue \
  --input '{"site_id": "SITE001", "requested_by": "scheduled"}'
```

---

## Running the test suite

```bash
uv run pytest tests/ -v
```

---

## Running linters manually

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy temporal/
```

---

## Project structure

```
network-ztp/
├── temporal/
│   ├── models.py                      # Pydantic contracts for all three phases
│   ├── activities/
│   │   ├── bootstrap_activities.py    # Day 0: DHCP, ZTP script render/publish, reachability
│   │   ├── nautobot_activities.py     # Nautobot GraphQL + REST API boundary
│   │   ├── ansible_activities.py      # Day 1: Jinja2 rendering + Ansible push
│   │   └── validation_activities.py   # Day 1/2: post-push state validation
│   ├── workflows/
│   │   ├── bootstrap_device.py        # Day 0 orchestrator (hands off to Day 1 as child)
│   │   ├── provision_site.py        # Day 1 orchestrator + HITL logic
│   │   └── compliance_scan.py         # Day 2 drift detection across a site
│   ├── worker.py                      # Worker process (all workflows + activities)
│   └── run_workflow.py                # CLI: bootstrap / start / scan / status / approve / list
├── tests/
│   ├── test_models.py                 # Pydantic validation + serialization
│   ├── test_activities.py             # Day 1 activity unit tests
│   ├── test_bootstrap_activities.py   # Day 0 activity unit tests
│   └── test_compliance_activities.py  # Day 2 activity unit tests
├── .github/workflows/lint.yml         # CI: ruff + mypy + pytest via uv
├── pyproject.toml                     # uv project config + tool settings
└── .env.example                       # Environment variable reference
```

---

## Prometheus metrics

The worker exposes Temporal SDK metrics on `:9091/metrics`. Key signals to monitor:

| Metric | What it tells you |
|--------|-------------------|
| `temporal_activity_schedule_to_start_latency` | Task queue backpressure — add workers if this grows |
| `temporal_activity_execution_latency` | Activity duration — use to size timeouts |
| `temporal_workflow_failed_total` | Failure rate — alert on unexpected spikes |
| `temporal_worker_task_slots_available` | Capacity — scale workers before this hits zero |

---

## Future work

- **React site survey UI** — web form that creates a Nautobot device record and triggers Day 0 bootstrap in one click, replacing the CLI for field engineers.
- **Terraform production deployment** — ECS task definition for the worker, ALB for the metrics endpoint, CloudWatch alarms wired to the Prometheus metrics.
- **Compliance fan-out** — a parent `ComplianceScanWorkflow` that fans out one child per batch of N devices for parallel validation at very large site counts.
- **Netmiko/NAPALM validation** — replace the simulated validation with real SSH sessions using `CiscoConfParse` for structured diff output.
- **Nautobot webhook trigger** — ingest `dcim.device` create/update webhooks to auto-trigger Day 0 or Day 1 when intent changes, eliminating the manual CLI step.
- **Multi-vendor support** — parameterize the Jinja2 template selection by platform slug so the same pipeline serves Junos, EOS, and NX-OS devices alongside IOS-XE.
