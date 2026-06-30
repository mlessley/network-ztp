# Platform Foundation & Observability Design

**Date:** 2026-06-30
**Status:** Approved — ready for implementation planning
**Scope:** FastAPI API layer, authentication/authorization, configuration management,
OpenTelemetry traces, Prometheus metrics, Grafana dashboards, Loki log aggregation,
docker-compose local stack, and site onboarding workflow.

---

## 1. Context

The existing codebase implements a three-phase ZTP pipeline (Day 0 bootstrap, Day 1
provisioning, Day 2 compliance) as Temporal workflows with a CLI entry point. This
design adds the platform layer that makes it enterprise-grade:

- A versioned REST API in front of Temporal (FastAPI)
- An authentication and authorization model with Apigee at the edge
- Configuration validation that fails fast at startup
- Full observability: traces (OTel/Tempo), logs (structlog/Loki), metrics (Prometheus/Grafana)
- A bulk site onboarding workflow for migrating existing branches into management
- A local development stack that starts with one command

The target scale is 5,000 branch sites across multiple regions. The platform is designed
to run as a single instance per region, with independent regional deployments sharing
the same codebase and config schema.

---

## 2. Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │           Operator / CI / Nautobot            │
                    └──────┬──────────────────────────┬────────────┘
                           │ HTTP                      │ Webhook (device created)
                           ▼                           ▼
                    ┌────────────────────────────────────────────────┐
                    │         Apigee  (GCP — production only)         │
                    │   OAuth validation · rate limiting · routing    │
                    │   Signs JWT, injects X-Authenticated-User       │
                    │   Enforces coarse-grained role checks           │
                    └──────────────────────┬─────────────────────────┘
                                           │ proxies to (mTLS in prod)
                                           ▼
                    ┌────────────────────────────────────────────────┐
                    │              FastAPI service  :8000             │
                    │                                                 │
                    │  /v1/devices/{id}/bootstrap                     │
                    │  /v1/devices/{id}/provision                     │
                    │  /v1/sites/{id}/scan                            │
                    │  /v1/onboarding/bulk                            │
                    │  /v1/onboarding/sites/{id}                      │
                    │  /v1/onboarding/status                          │
                    │  /v1/webhooks/nautobot                          │
                    │  /v1/workflows/{id}  (status / approve / list)  │
                    │  /health  /health/ready                         │
                    │                                                 │
                    │  pydantic-settings config validation            │
                    │  OTel trace origin · structlog correlation      │
                    │  RBAC + region-scoped authorization             │
                    └──────────────────────┬─────────────────────────┘
                                           │ Temporal gRPC :7233
                                           ▼
                    ┌────────────────────────────────────────────────┐
                    │          Temporal  (workflow engine)            │
                    │  BootstrapDeviceWorkflow    (Day 0)             │
                    │  ProvisionSiteWorkflow      (Day 1)             │
                    │  ComplianceScanWorkflow     (Day 2)             │
                    │  BulkOnboardingWorkflow     (Day 0.5 new)       │
                    │  OnboardSiteWorkflow        (Day 0.5 new)       │
                    └──────────┬──────────────────────┬──────────────┘
                               │                      │
               ┌───────────────┘                      └──────────────┐
               ▼                                                      ▼
   ┌──────────────────────┐                         ┌──────────────────────────┐
   │    ZTP Worker(s)     │                         │     Observability        │
   │                      │                         │                          │
   │  10 existing +       │──OTel spans────────────►│  Tempo   (traces)  :3200 │
   │  4 new activities    │──structlog JSON─────────►│  Loki    (logs)    :3100 │
   │  Prometheus  :9091   │──metrics────────────────►│  Prometheus        :9090 │
   └──────────────────────┘                         │  Grafana (unified) :3000 │
                                                    └──────────────────────────┘
```

---

## 3. Project Structure

Changes are additive. `temporal/` is unchanged except for three targeted edits noted
in section 4.6.

```
network-ztp/
├── api/
│   ├── main.py                   app factory, middleware registration, OTel setup
│   ├── config.py                 pydantic-settings Settings model
│   ├── deps.py                   FastAPI dependency providers (Temporal client, user context)
│   ├── errors.py                 RFC 7807 problem details error handler
│   ├── endpoints/
│   │   ├── devices.py            POST /v1/devices/{id}/bootstrap|provision
│   │   ├── sites.py              POST /v1/sites/{id}/scan
│   │   ├── onboarding.py         POST /v1/onboarding/bulk|sites/{id}, GET /v1/onboarding/status
│   │   ├── webhooks.py           POST /v1/webhooks/nautobot
│   │   ├── workflows.py          GET|POST /v1/workflows/...
│   │   └── health.py             GET /health, /health/ready
│   ├── schemas/
│   │   ├── requests.py           HTTP-level Pydantic request models
│   │   └── responses.py          HTTP-level Pydantic response models
│   └── middleware/
│       ├── identity.py           Apigee JWT validation (prod) / header stub (dev)
│       └── observability.py      OTel span + structlog context binding per request
│
├── temporal/
│   ├── config.py                 NEW — pydantic-settings for worker
│   ├── metrics.py                NEW — Prometheus counter/gauge/histogram definitions
│   ├── activities/
│   │   ├── onboarding_activities.py  NEW — discover_device_config, discover_device_state,
│   │   │                                    reconcile_nautobot_records, generate_remediation_plan
│   │   └── ... (existing unchanged)
│   ├── workflows/
│   │   ├── bulk_onboarding.py    NEW — BulkOnboardingWorkflow
│   │   ├── onboard_site.py       NEW — OnboardSiteWorkflow
│   │   └── ... (existing unchanged)
│   ├── worker.py                 UPDATED — uses temporal/config.py, registers new workflows/activities
│   └── run_workflow.py           UPDATED — becomes httpx API client to FastAPI
│
├── tests/
│   ├── api/
│   │   ├── conftest.py           TestClient + Temporal client mock fixtures
│   │   ├── test_devices.py
│   │   ├── test_onboarding.py
│   │   ├── test_webhooks.py
│   │   └── test_workflows.py
│   ├── test_onboarding_activities.py  NEW
│   ├── test_onboarding_workflow.py    NEW
│   └── ... (existing unchanged)
│
├── config/
│   └── observability/
│       ├── grafana/
│       │   ├── provisioning/
│       │   │   ├── datasources.yaml      Prometheus + Tempo + Loki wired automatically
│       │   │   └── dashboards.yaml       auto-load all JSON from dashboards/
│       │   └── dashboards/
│       │       ├── worker-overview.json
│       │       ├── pipeline-latency.json
│       │       ├── compliance-health.json
│       │       └── onboarding-progress.json
│       ├── prometheus/
│       │   ├── prometheus.yml            scrape config
│       │   └── rules/
│       │       ├── recording.yml
│       │       └── alerting.yml
│       └── promtail/
│           └── config.yml                label extraction pipeline
│
├── docker/
│   ├── Dockerfile.api
│   ├── Dockerfile.worker
│   └── entrypoint.sh
│
├── docker-compose.yml
├── docker-compose.override.yml   hot-reload for local dev
├── Makefile
└── .env.example                  all vars documented with descriptions
```

---

## 4. FastAPI API Layer

### 4.1 REST Contract

All routes are versioned under `/v1/`. The API returns `202 Accepted` for all workflow
submissions — the workflow runs asynchronously and the caller polls `/v1/workflows/{id}`.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/devices/{device_id}/bootstrap` | Trigger Day 0 (BootstrapDeviceWorkflow) |
| POST | `/v1/devices/{device_id}/provision` | Trigger Day 1 directly (ProvisionSiteWorkflow) |
| POST | `/v1/sites/{site_id}/scan` | Trigger Day 2 compliance scan |
| POST | `/v1/onboarding/bulk` | Start bulk onboarding with site list and rate config |
| POST | `/v1/onboarding/sites/{site_id}` | Onboard a single existing site |
| GET | `/v1/onboarding/status` | Live counts by onboarding state (from workflow query handler) |
| POST | `/v1/webhooks/nautobot` | Receive Nautobot device-created webhook |
| GET | `/v1/workflows/{workflow_id}` | Status + result for any workflow |
| POST | `/v1/workflows/{workflow_id}/approve` | Send HITL approve/reject signal |
| GET | `/v1/workflows` | List recent workflows (cursor-paginated, filtered by region) |
| GET | `/health` | Liveness probe |
| GET | `/health/ready` | Readiness probe (checks Temporal connectivity) |

**Request models** (in `api/schemas/requests.py`):

```python
class BootstrapRequest(BaseModel):
    requested_by: str

class ProvisionRequest(BaseModel):
    requested_by: str

class ScanRequest(BaseModel):
    requested_by: str
    device_ids: list[str] = []      # empty = all devices at site

class BulkOnboardRequest(BaseModel):
    site_ids: list[str]
    sites_per_hour: int = 50        # rate limit
    max_concurrent: int = 10        # max simultaneous OnboardSiteWorkflows
    requested_by: str

class ApproveRequest(BaseModel):
    decision: Literal["approved", "rejected"]
    note: str = ""
```

**Response models** (in `api/schemas/responses.py`):

```python
class WorkflowSubmitted(BaseModel):     # 202 Accepted
    workflow_id: str
    status_url: str                     # "/v1/workflows/{workflow_id}"

class WorkflowStatus(BaseModel):
    workflow_id: str
    status: str
    device_id: str | None
    site_id: str | None
    started_at: datetime
    completed_at: datetime | None
    failure_reason: str | None
    trace_id: str | None               # OTel trace_id — paste into Grafana/Tempo

class OnboardingStatus(BaseModel):
    pending: int
    discovering: int
    discovered: int
    reconciling: int
    managed: int
    failed: int
    sites_per_hour_actual: float
    estimated_completion: datetime | None
```

**Error responses** follow RFC 7807 problem details. Every error includes `trace_id`
so callers can jump directly to Tempo:

```json
{
  "type": "https://network-ztp/errors/workflow-conflict",
  "title": "Workflow Already Running",
  "status": 409,
  "detail": "A provisioning workflow is already running for device DEV001",
  "instance": "/v1/devices/DEV001/provision",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

Key error types:

| Scenario | Status |
|----------|--------|
| Workflow already running for device/site | 409 Conflict |
| Workflow or device not found | 404 Not Found |
| Temporal unreachable | 503 Service Unavailable |
| Invalid `decision` value in approve | 422 Unprocessable Entity |
| Missing or invalid HMAC on webhook | 401 Unauthorized |
| Insufficient role | 403 Forbidden |
| Device outside user's allowed regions | 403 Forbidden |

### 4.2 Nautobot Webhook

Nautobot fires a webhook when a device record is created or set to Active. The handler:

1. Validates `X-Hook-Signature` HMAC-SHA256 against `NAUTOBOT_WEBHOOK_SECRET`
2. Rejects anything that is not `event=created, model=device`
3. Extracts `device_id` and `mac_address` from the payload
4. Submits `BootstrapDeviceWorkflow`
5. Returns `200` immediately — Nautobot has a short webhook timeout

In mock mode (`ZTP_USE_MOCK=true`) the endpoint accepts a simplified payload so
engineers can trigger the bootstrap flow with a single `curl` command locally.

### 4.3 Middleware Stack

Middleware executes in this order, outermost first:

```
① OTel tracing       generates trace_id, wraps request in a root span
② Request ID         reads or generates X-Request-ID header
③ Structlog binder   binds trace_id + request_id + path + method to log context
④ Identity           validates Apigee JWT (prod) or reads stub header (dev)
⑤ Route handler      sees request.state.user (UserContext), request.state.trace_id
⑥ Error handler      catches all unhandled exceptions → RFC 7807 response
```

**Identity middleware detail:**

In production, Apigee validates the OAuth token upstream and injects a signed JWT
as the `Authorization: Bearer <jwt>` header. FastAPI validates the JWT signature
using Apigee's public key. The JWT carries:

```json
{
  "sub": "mark@corp.com",
  "roles": ["engineer"],
  "regions": ["AMER", "EMEA"]
}
```

In development, the middleware reads `X-Authenticated-User` and falls back to
`settings.auth_dev_user` (`"dev-user"`) if absent. The dev user's roles and regions
are configurable via `AUTH_DEV_ROLES` and `AUTH_DEV_REGIONS` env vars.

**Defense in depth:** The network-level control is that FastAPI is unreachable except
from Apigee's egress (firewall/security group). mTLS between Apigee and FastAPI is
the second layer. The JWT signature is the third. FastAPI's identity middleware enforces
the third layer in code; the first two are deployment topology.

### 4.4 Authentication and Authorization

**Roles:**

```python
class UserRole(StrEnum):
    ADMIN           = "admin"      # full access, all regions
    ENGINEER        = "engineer"   # provision/scan/approve, own regions only
    NOC_OPERATOR    = "noc"        # read-only on all workflows
    SERVICE_ACCOUNT = "service"    # trigger workflows via API, no HITL approval
```

**Authorization matrix:**

| Endpoint | Minimum role | Region-scoped |
|----------|-------------|---------------|
| POST /devices/{id}/bootstrap | engineer | yes |
| POST /devices/{id}/provision | engineer | yes |
| POST /sites/{id}/scan | engineer | yes |
| POST /onboarding/bulk | engineer | yes |
| POST /onboarding/sites/{id} | engineer | yes |
| GET /onboarding/status | noc | no |
| GET /workflows/{id} | noc | no — all statuses readable |
| GET /workflows | noc | list filtered to user's regions |
| POST /workflows/{id}/approve | engineer | yes |
| POST /webhooks/nautobot | service | n/a |

**FastAPI dependency pattern:**

```python
# deps.py

async def get_current_user(request: Request) -> UserContext:
    # parses JWT (prod) or stub header (dev)
    # returns UserContext(user, role, regions)

def require_role(*roles: UserRole):
    async def dep(user: UserContext = Depends(get_current_user)):
        if user.role not in roles:
            raise HTTPException(403)
    return Depends(dep)

async def require_region_access(
    resource_id: str,
    user: UserContext = Depends(get_current_user),
    resource_type: str = "device",     # "device" or "site"
) -> None:
    region = await _get_resource_region(resource_id, resource_type)
    if user.role != UserRole.ADMIN and region not in user.regions:
        raise HTTPException(403, detail=f"Resource is in region {region}")
```

Region membership is authoritative in the JWT (set by the IdP from group membership).
The device's or site's region is resolved from the `DeviceIntent` (Nautobot is the
source of truth). In mock mode, device region resolves to `"AMER"` by default.

### 4.5 Configuration

`api/config.py` and `temporal/config.py` each define a `Settings(BaseSettings)` model.
Both load from the same `.env` file. Neither file contains `os.getenv()` calls.

**Key fields:**

```python
class Settings(BaseSettings):
    # Temporal
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "ztp-queue"

    # API
    api_port: int = 8000

    # Nautobot
    nautobot_url: str = "http://localhost:8080"
    nautobot_token: str = ""
    nautobot_webhook_secret: str = ""

    # Auth
    auth_dev_user: str = "dev-user"
    auth_dev_roles: list[str] = ["engineer"]
    auth_dev_regions: list[str] = ["AMER"]

    # Observability
    otlp_endpoint: str = "http://localhost:4317"
    metrics_port: int = 9090
    ztp_env: Literal["development", "production"] = "development"
    log_level: str = "INFO"

    # Mock control
    ztp_use_mock: bool = True

    # Onboarding
    onboarding_sites_per_hour: int = 50
    onboarding_max_concurrent: int = 10

    # Multi-region
    default_region: str = "AMER"       # used for service-account requests lacking JWT region

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    @model_validator(mode="after")
    def require_live_credentials(self) -> "Settings":
        if not self.ztp_use_mock:
            missing = [v for v in ("nautobot_token", "nautobot_webhook_secret")
                       if not getattr(self, v)]
            if missing:
                raise ValueError(
                    f"Live mode (ZTP_USE_MOCK=false) requires: "
                    f"{', '.join(m.upper() for m in missing)}"
                )
        return self
```

Worker startup calls `Settings()` once. If validation raises, the process exits with
a clear error before registering a single activity or connecting to Temporal.

### 4.6 Targeted Changes to Existing Code

Three surgical edits to existing files — no structural changes:

1. **`httpx2` → `httpx` everywhere.** Drop `import httpx2 as httpx` in
   `nautobot_activities.py`. Update `pyproject.toml`. Respx tests work natively again.

2. **`worker.py`** imports `temporal.config.Settings` instead of calling `os.getenv()`.
   Registers the two new onboarding workflows and four new onboarding activities.

3. **`run_workflow.py`** removes the `temporalio` import. All six commands become
   `httpx.AsyncClient` calls to the FastAPI service. The Temporal client dependency
   leaves this file entirely.

### 4.7 Workflow ID Namespacing

All workflow IDs are prefixed with a region slug to support future multi-instance
deployments without ID collisions:

```
{region}-day0-{device_id}
{region}-day1-{device_id}-{parent_workflow_id}
{region}-day2-{site_id}-{timestamp}
{region}-onboard-bulk-{requested_by}-{timestamp}
{region}-onboard-site-{site_id}
```

Region comes from the JWT claims on the submitted request, defaulting to
`settings.default_region` for service account requests.

---

## 5. Site Onboarding — Day 0.5

This phase migrates existing live branches into management. It is fundamentally
different from ZTP (which targets blank devices) because:

- Devices carry live production configurations
- Discovery is strictly read-only until a human approves remediation
- Simultaneous discovery of 5,000 sites would flood the network
- The process must be pausable, resumable, and fully auditable

### 5.1 Onboarding State Machine

Each site moves through these states independently:

```
PENDING → DISCOVERING → DISCOVERED → [HITL: engineer reviews] → RECONCILING → MANAGED
                                                                       │
                                                                   FAILED (any phase)
```

States are tracked as `ProvisioningStatus` enum values (new entries added at the end
of the enum block per CLAUDE.md rule).

### 5.2 BulkOnboardingWorkflow

Long-running orchestrator. Holds the full site queue in Temporal history. Rate-limits
child workflow spawning via `workflow.sleep()` between submissions.

**Signals:**
- `pause` — stop spawning new child workflows; in-flight children run to completion
- `resume` — resume spawning
- `adjust_rate(sites_per_hour, max_concurrent)` — change rate at runtime

**Query handlers:**
- `get_status()` → `OnboardingStatus` (counts by state, actual rate, ETA)

**Behavior:**
- Spawns up to `max_concurrent` `OnboardSiteWorkflow` children simultaneously
- Sleeps `3600 / sites_per_hour` seconds between spawns
- If the failure rate in the last hour exceeds 5%, pauses automatically and emits
  a `ztp_onboarding_auto_paused` log event at ERROR level (triggers the alert)
- The `GET /v1/onboarding/status` endpoint reads the query handler — no DB query needed

### 5.3 OnboardSiteWorkflow

Per-site child workflow. Each step is a separate activity to enable independent retry.

```
fetch_device_intent           (existing activity — reused)
discover_device_config        (new — NAPALM get_config, read-only)
discover_device_state         (new — NAPALM get_interfaces + get_bgp_neighbors)
reconcile_nautobot_records    (new — compare/create/update Nautobot records)
generate_remediation_plan     (new — structured diff, no config push)
[HITL: engineer reviews plan and approves or rejects]
remediate_drift               (existing push_config — only runs after approval)
write_provisioning_status     (existing activity — marks MANAGED or FAILED)
```

The config snapshot from `discover_device_config` is stored in Temporal workflow
history as an immutable artifact before any remediation is attempted. If remediation
fails, the snapshot is not applied — roll-forward philosophy applies here too.

### 5.4 New Activities

All four new activities live in `temporal/activities/onboarding_activities.py`.
All are read-only except `remediate_drift` (which delegates to the existing `push_config`).

| Activity | External call | Mock path |
|----------|--------------|-----------|
| `discover_device_config` | NAPALM `get_config()` in `asyncio.to_thread()` | Returns realistic Cisco IOS-XE config string |
| `discover_device_state` | NAPALM `get_interfaces()` + `get_bgp_neighbors()` | Returns intent-consistent mock state |
| `reconcile_nautobot_records` | pynautobot REST PATCH via `asyncio.to_thread()` | Logs what would be created/updated |
| `generate_remediation_plan` | Pure computation — no external call | Same code path, no mock needed |

### 5.5 Throttling Rationale

The default of 50 sites/hour (one site every 72 seconds) is conservative by design.
At this rate:

- 5,000 sites complete in ~100 hours (~4 days) — acceptable for a migration
- Peak simultaneous NAPALM connections = 10 (max_concurrent setting)
- Network impact is bounded and predictable

Both values are runtime-adjustable via the `adjust_rate` signal without restarting
the workflow. The Onboarding Progress dashboard makes the current rate and ETA
visible to operators at all times.

---

## 6. Observability

### 6.1 OpenTelemetry — Traces

Both the FastAPI service and the Temporal worker emit traces to Tempo via OTLP gRPC.

**FastAPI** uses `opentelemetry-instrumentation-fastapi` (auto-instrumented — every HTTP
request becomes a root span) and `opentelemetry-instrumentation-httpx` (outbound calls
to Nautobot become child spans automatically).

**Temporal activities** use manual spans:

```python
tracer = trace.get_tracer(__name__)

@activity.defn
async def fetch_device_intent(device_id: str) -> DeviceIntent:
    with tracer.start_as_current_span("fetch_device_intent") as span:
        span.set_attribute("device.id", device_id)
        span.set_attribute("ztp.phase", "day1")
        # ... existing code unchanged ...
```

Every span carries `device.id`, `site.id`, `workflow.id`, and `ztp.phase` as attributes
so Tempo queries can filter by any of these dimensions.

**Structlog ↔ OTel correlation** via a custom processor in the shared processor chain:

```python
def _inject_otel_context(logger, method, event_dict):
    span = trace.get_current_span()
    if span.is_recording():
        ctx = span.get_span_context()
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict
```

Every log line carries `trace_id`. The navigation chain becomes:
**Grafana alert → log line → copy trace_id → paste in Tempo → see exact activity span**.

The `trace_id` field also appears in RFC 7807 error responses so API callers can
jump directly to Tempo from an error without any additional steps.

### 6.2 Prometheus Metrics

The Temporal SDK already exposes workflow and activity metrics. These application-level
metrics are added in `temporal/metrics.py` and incremented at the workflow/activity level:

```python
workflow_started   = Counter("ztp_workflow_started_total", ..., ["phase"])
workflow_completed = Counter("ztp_workflow_completed_total", ..., ["phase", "status"])
drift_detected     = Counter("ztp_drift_detected_total", ..., ["site_id"])
hitl_pending       = Gauge("ztp_hitl_pending_total", ...)
hitl_resolution_s  = Histogram("ztp_hitl_resolution_duration_seconds", ...,
                        buckets=[300, 900, 1800, 3600, 7200, 14400, 86400])
onboarding_sites   = Gauge("ztp_onboarding_sites_total", ..., ["status"])
```

**Recording rules** (`config/observability/prometheus/rules/recording.yml`):

```yaml
groups:
  - name: ztp.recording
    interval: 30s
    rules:
      - record: job:ztp_workflow_success_rate:5m
        expr: |
          rate(ztp_workflow_completed_total{status="success"}[5m])
          / rate(ztp_workflow_completed_total[5m])

      - record: job:ztp_activity_latency_p95:5m
        expr: |
          histogram_quantile(0.95,
            rate(temporal_activity_execution_latency_seconds_bucket[5m]))

      - record: job:ztp_drift_rate:1h
        expr: rate(ztp_drift_detected_total[1h])

      - record: job:ztp_onboarding_failure_rate:1h
        expr: |
          rate(ztp_onboarding_sites_total{status="failed"}[1h])
          / rate(ztp_onboarding_sites_total[1h])
```

**Alert rules** (`config/observability/prometheus/rules/alerting.yml`):

| Alert | Condition | For | Severity |
|-------|-----------|-----|----------|
| `ZTPWorkerDown` | worker scrape gone | 2m | critical |
| `ZTPHighFailureRate` | success rate < 95% | 5m | warning |
| `ZTPHITLStalePending` | `hitl_pending > 0` | 4h | warning |
| `ZTPComplianceDriftSpiking` | drift rate > 10/hour | 15m | critical |
| `ZTPOnboardingFailureRate` | onboarding failure rate > 5% | 15m | critical |
| `ZTPTemporalQueueDepth` | task schedule-to-start > 30s | 10m | warning |

The onboarding failure rate alert is the most operationally important: a >5% failure
rate during bulk discovery indicates a systemic issue (bad credentials, network
segment unreachable) and the batch should pause before affecting thousands more sites.

### 6.3 Grafana Dashboards

Four dashboards, auto-provisioned from JSON on `docker compose up`. No manual
Grafana configuration required.

**Worker Overview** — the on-call dashboard
- Workflow completion rate over time (timeseries)
- Activity latency P50/P95 (timeseries)
- HITL pending count (stat panel, red if > 0 for > 2h)
- Worker up/down state timeline
- Active workflows by phase (gauge row)

**Pipeline Latency** — for performance investigation
- End-to-end Day 0 duration histogram
- End-to-end Day 1 duration histogram
- Per-activity latency breakdown (heatmap)
- Retry rate by activity (surfaces flaky activities)

**Compliance Health** — Day 2 operations
- Drift detection rate over time
- Devices by compliance status (pie)
- HITL resolution time histogram
- Top sites by drift frequency (table, sortable)

**Onboarding Progress** — bulk migration visibility
- Sites by state (bar: pending / discovering / discovered / reconciled / managed / failed)
- Discovery throughput vs. configured limit (timeseries)
- Failure rate with 5% threshold line
- Estimated completion (stat, derived from current rate × remaining count)
- Pause/resume indicator (from `BulkOnboardingWorkflow` query handler)

All three datasources (Prometheus, Tempo, Loki) are pre-configured via
`config/observability/grafana/provisioning/datasources.yaml`. Panels that show
metrics can link directly to Tempo traces and Loki logs for the same time window.

### 6.4 Loki — Log Labels

**Labels are low-cardinality index keys — not arbitrary log fields.** Loki creates
a separate storage stream for every unique label combination. High-cardinality values
(`device_id`, `workflow_id`, `trace_id`) go inside the structlog JSON body where they
are searchable via LogQL but do not create stream explosion.

**Labels defined:**

| Label | Values | Source |
|-------|--------|--------|
| `service` | `ztp-api`, `ztp-worker` | Docker container name (promtail) |
| `env` | `development`, `production` | Container env var (promtail) |
| `level` | `debug`, `info`, `warning`, `error`, `critical` | JSON field, promoted to label |
| `phase` | `day0`, `day1`, `day2`, `onboarding` | JSON field, promoted to label |
| `region` | `AMER`, `EMEA`, `APAC` | JSON field, promoted to label |

**High-cardinality values stay in the log line body** (structlog JSON):
`device_id`, `site_id`, `workflow_id`, `trace_id`, `user`

**Example LogQL queries:**

```logql
# All errors from the worker — labels narrow it fast
{service="ztp-worker", level="error"}

# All logs for a specific device — json filter within the stream
{service="ztp-worker"} | json | device_id="DEV001"

# Jump from a Tempo trace_id directly to its logs
{service=~"ztp-.*"} | json | trace_id="4bf92f3577b34da6a3ce929d0e0e4736"

# All onboarding failures in the last hour
{service="ztp-worker", phase="onboarding", level="error"}

# Regional scope — logs for AMER worker only
{service="ztp-worker", region="AMER", level="error"}
```

Promtail extracts labels from Docker container metadata and JSON log lines:

```yaml
# config/observability/promtail/config.yml (simplified)
pipeline_stages:
  - docker: {}
  - json:
      expressions:
        level: level
        phase: phase
        region: region
  - labels:
      level:
      phase:
      region:
  # device_id, workflow_id, trace_id NOT promoted — left in body
```

---

## 7. Local Development Stack

`make up` starts the complete local environment. `make dev` adds hot-reload.

### 7.1 docker-compose Services

| Service | Image | Port | Notes |
|---------|-------|------|-------|
| `postgresql` | postgres:16 | — | Temporal backend |
| `temporal` | temporalio/auto-setup | 7233 | includes schema migration |
| `temporal-ui` | temporalio/ui | 8080 | workflow history browser |
| `ztp-worker` | docker/Dockerfile.worker | 9091 | depends_on: temporal healthy |
| `ztp-api` | docker/Dockerfile.api | 8000 | depends_on: temporal healthy |
| `prometheus` | prom/prometheus | 9090 | scrapes worker :9091 and api :9090 |
| `tempo` | grafana/tempo | 4317, 3200 | OTLP gRPC receiver + query API |
| `loki` | grafana/loki | 3100 | log aggregation |
| `promtail` | grafana/promtail | — | ships Docker logs to Loki |
| `grafana` | grafana/grafana | 3000 | dashboards + datasources auto-provisioned |

Nautobot is omitted from docker-compose — the `ZTP_USE_MOCK=true` default means
all Nautobot calls return mock data. Engineers can point at a real Nautobot by
setting `ZTP_USE_MOCK=false` and `NAUTOBOT_URL` / `NAUTOBOT_TOKEN` in `.env`.

### 7.2 docker-compose.override.yml

Activated by `make dev`. Adds:
- `api/` and `temporal/` mounted as volumes into their containers
- Uvicorn runs with `--reload` so code changes restart the API immediately
- `ZTP_USE_MOCK=true`, `ZTP_ENV=development`, `LOG_LEVEL=DEBUG`

### 7.3 Makefile

```makefile
up:     docker compose up -d
down:   docker compose down
dev:    docker compose -f docker-compose.yml -f docker-compose.override.yml up
logs:   docker compose logs -f ztp-api ztp-worker
reset:  docker compose down -v && docker compose up -d
test:   uv run pytest tests/ -v
lint:   uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/
build:  docker compose build
```

`make reset` wipes all volumes (Temporal history, Prometheus data, Grafana state)
and starts fresh — useful when testing schema migrations or workflow changes.

---

## 8. Multi-Region Considerations

This design supports a single instance per region. Each regional instance is an
independent deployment of the same codebase with region-specific config:

- `TEMPORAL_NAMESPACE` isolates Temporal history per region
- Workflow IDs are prefixed with `{region}-` (see section 4.7)
- The JWT carries the user's allowed regions; FastAPI enforces region-scoped access
- Prometheus federation (scraping regional Prometheus from a central instance) is
  the path to cross-region dashboards — not implemented here, noted for future work

If organizational or compliance requirements force regional isolation before federation
is built, each region runs its own Grafana. The dashboard JSON files are the same;
only the datasource URLs differ.

---

## 9. What Is Mocked

| Component | Mock behavior | Live path (ZTP_USE_MOCK=false) |
|-----------|--------------|-------------------------------|
| Nautobot GraphQL | `_mock_graphql_response()` in nautobot_activities.py | httpx to `NAUTOBOT_URL` |
| Nautobot REST PATCH | logs the would-be PATCH | pynautobot in `asyncio.to_thread()` |
| DHCP reservation | returns a deterministic IP from `10.0.0.0/24` | httpx to DHCP API |
| ZTP script publish | logs the script path | writes to `ZTP_SCRIPT_PATH` |
| Ansible push | returns `PushResult(success=True)` | ansible-runner in `asyncio.to_thread()` |
| NAPALM validation | random drift at `_DRIFT_PROBABILITY` rate | NAPALM driver in `asyncio.to_thread()` |
| Nautobot webhook (inbound) | simplified payload accepted | full HMAC validation |
| Apigee identity | `X-Authenticated-User` header or `auth_dev_user` | JWT signature validation |
| Device region lookup | returns `"AMER"` | Nautobot GraphQL |
| NAPALM discovery (onboarding) | returns mock config + state | NAPALM `get_config()` + `get_interfaces()` |

---

## 10. Out of Scope

The following are acknowledged but not designed here:

- **Alertmanager routing** — alert rule YAML is defined; where alerts route (PagerDuty,
  Slack, email) depends on the organization's existing alerting infrastructure
- **Prometheus federation** for cross-region dashboards
- **Apigee configuration** (proxy definition, OAuth policies, rate limit quotas) —
  this is managed by the API platform team, not the ZTP team
- **Nautobot webhook configuration** — creating the webhook in Nautobot pointing at
  the FastAPI service URL is a deployment step, not a code change
- **CI/CD pipeline** — the `lint` and `test` Makefile targets are the contract;
  how they run in CI is out of scope
- **Secrets management** — `.env` is used for local dev; production secrets come
  from the organization's vault (AWS Secrets Manager, HashiCorp Vault, etc.)
- **TLS termination** — handled by Apigee (edge) and the service mesh or load
  balancer (internal); FastAPI runs plain HTTP inside the network boundary
