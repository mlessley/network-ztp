# Tutorial 03: Reading the Observability Stack

**Goal:** Find a workflow's metrics in Prometheus, view its Grafana dashboards, and follow a distributed trace end-to-end in Tempo.

**Time:** ~25 minutes

**Prerequisites:** Tutorial 02 complete — at least one workflow has run.

---

## The three pillars

| Pillar | Tool | What it shows |
|--------|------|--------------|
| **Metrics** | Prometheus + Grafana | Counters, gauges, histograms over time |
| **Traces** | Tempo + Grafana | Distributed spans — exactly what code ran and how long each step took |
| **Logs** | Loki + Grafana | Structured JSON logs from all containers, correlated to traces |

All three are wired together in Grafana. The `trace_id` that appears in log lines is the same ID you search for in Tempo.

---

## Part 1: Metrics in Grafana

Open Grafana at `http://localhost:3000` (admin / admin).

Go to **Dashboards → ZTP → Worker Overview**.

### What you're looking at

**Workflow throughput** (top row): `ztp_workflow_started_total` and `ztp_workflow_completed_total` broken down by `phase` label (`day0`, `day0.5`, `day1`, `day2`). After running a few workflows you'll see the bars populate.

**Activity latency** (middle row): A histogram of how long activities took. `fetch_device_intent`, `render_config`, and `push_config` each get their own OTel span, recorded in Tempo (see Part 2). The Prometheus side tracks completion counts.

**HITL pending** (bottom right): `ztp_hitl_pending_total` gauge — shows how many workflows are currently parked waiting for a human decision. If you left any workflows in the HITL state from Tutorial 02, you'll see them here.

### Querying Prometheus directly

Open Prometheus at `http://localhost:9090` and try these queries in the **Graph** tab:

```promql
# Total workflows started, broken down by phase
ztp_workflow_started_total

# Completion rate (success vs failure)
ztp_workflow_completed_total

# Drift detected count by site/device
ztp_drift_detected_total

# How many workflows are currently parked waiting for approval
ztp_hitl_pending_total
```

Click the **Graph** tab (not Table) to see the time series. The counter will only show values after you've run at least one workflow.

---

## Part 2: Distributed Traces in Tempo

Every API request and every activity execution is wrapped in an OTel span. Spans from the API and from the worker (running in a separate process) are correlated into a single trace.

### Finding a trace from a log line

When you ran `make logs`, every log line had a `trace_id` field. Grab one:

```bash
docker compose logs ztp-api 2>/dev/null | grep '"trace_id"' | head -3
```

Copy the `trace_id` value.

In Grafana, go to **Explore** (the compass icon in the left sidebar). Select **Tempo** as the data source from the dropdown.

Paste the trace ID into the **TraceQL** search box:

```
{ trace:id="<your-trace-id>" }
```

Or use the **Search** tab and paste the ID into "Trace ID" directly.

### What you'll see

A trace waterfall showing:

```
POST /v1/devices/DEV001/provision          [API span — FastAPI route handler]
  └── fetch_device_intent                  [activity span — worker]
  └── render_config                        [activity span — worker]
  └── push_config                          [activity span — worker]
  └── validate_device_state                [activity span — worker]
  └── write_provisioning_status            [activity span — worker]
```

Each span shows:
- Start time and duration
- `device.id` attribute set by the activity
- Any error details if the span failed

This is how you diagnose "which step was slow" without reading code.

### Finding traces without a trace ID

In Tempo's **Search** tab, you can filter by:
- **Service name**: `ztp-api` or `ztp-worker`
- **Span name**: e.g., `push_config` or `validate_device_state`
- **Duration**: find slow spans (e.g., > 500ms)

---

## Part 3: Logs in Loki

In Grafana **Explore**, switch the data source to **Loki**.

### Query logs from the API

```logql
{container="sd-branch-ztp-ztp-api-1"} | json
```

> **Note:** The container name prefix depends on your Docker Compose project name. Use `{job="containerlogs"}` as a fallback if needed, or check the **Label browser** to see what labels Promtail is tagging.

### Filter logs for a specific device

```logql
{container=~".*ztp.*"} | json | device_id="DEV001"
```

### Correlate logs to a trace

Click any log line in the Loki results. If the line has a `trace_id` field, Grafana shows a **Tempo** button — click it to jump directly to that trace.

This is the full correlation loop: Prometheus tells you something is wrong → Loki finds the log lines → the log line's `trace_id` takes you to the exact span in Tempo.

---

## Part 4: Alert rules

Prometheus has pre-configured alert rules (in `config/observability/prometheus/rules.yml`):

| Alert | Fires when |
|-------|-----------|
| `ZTPWorkerDown` | Worker scrape fails for 2 minutes |
| `ZTPHighFailureRate` | >20% of workflows completing with `status=failure` over 10 minutes |
| `ZTPHITLStalePending` | A workflow has been waiting for HITL approval for >2 hours |
| `ZTPComplianceDriftSpiking` | Drift detection rate doubles in 30 minutes |
| `ZTPOnboardingFailureRate` | >30% onboarding failure rate over 15 minutes |

To see alert state: `http://localhost:9090/alerts`

To trigger the HITL stale alert artificially: leave a workflow parked in the HITL state (Tutorial 04) and wait. In local dev the 2-hour window is long — but you can see the rule definition and understand what you'd be paged for in production.

---

## What you now know

- Metrics, traces, and logs are all in Grafana — Prometheus, Tempo, and Loki as data sources
- Every log line carries a `trace_id` that links to the OTel trace in Tempo
- `ztp_workflow_started_total`, `ztp_workflow_completed_total`, `ztp_drift_detected_total`, and `ztp_hitl_pending_total` are the four primary business metrics
- Tempo shows the full span waterfall including which activity was slow
- Alert rules are pre-configured for the four most important failure modes

**Next:** [Tutorial 04 — Human-in-the-Loop Escalation](04-human-in-the-loop.md)
