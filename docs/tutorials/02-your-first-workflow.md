# Tutorial 02: Trigger Your First Workflow

**Goal:** Submit a Day 1 provisioning job, watch it execute step-by-step in the Temporal UI, and read the structured logs.

**Time:** ~20 minutes

**Prerequisites:** Tutorial 01 complete — stack running, all services healthy.

---

## What happens in Day 1 provisioning

When you trigger Day 1 for a device, the system:

1. **Fetches device intent** from Nautobot (mock returns a canned `DeviceIntent` object)
2. **Renders configuration** from Jinja2 templates using the intent
3. **Pushes configuration** via Ansible (mock logs the push, returns success)
4. **Validates device state** by comparing live state to intent (10% random drift probability in mock mode)
5. If drift is detected → **parks for human approval** (Tutorial 04)
6. If clean → **marks COMPLETE** in Nautobot and returns

---

## Option A: Use the CLI

The CLI sends HTTP requests to the API. From the project root:

```bash
uv run python temporal/run_workflow.py start \
    --device-id DEV001 \
    --requested-by you@example.com
```

Expected output:

```
╭─────────────────────────────────────────╮
│ Provision submitted                     │
│ Workflow: day1-DEV001                   │
╰─────────────────────────────────────────╯
```

The workflow ID is `day1-DEV001`. You'll use this in a moment.

---

## Option B: Use curl

```bash
curl -s -X POST http://localhost:8000/v1/devices/DEV001/provision \
  -H "Content-Type: application/json" \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" \
  -d '{"requested_by": "you@example.com"}' | python3 -m json.tool
```

The `X-Authenticated-User` header is how dev-mode auth works — format is `username:ROLE:REGION`. Without it you'll get a 401.

Expected response:

```json
{
  "workflow_id": "day1-DEV001",
  "status_url": "/v1/workflows/day1-DEV001"
}
```

---

## Watch it run in Temporal UI

Open `http://localhost:8080` and click **Workflows**.

You'll see `day1-DEV001` appear. Click on it.

The **Event History** panel on the right shows every step the workflow took:

```
WorkflowExecutionStarted
ActivityTaskScheduled    → fetch_device_intent
ActivityTaskStarted
ActivityTaskCompleted    → returned DeviceIntent(device_id="DEV001", ...)

ActivityTaskScheduled    → render_config
ActivityTaskStarted
ActivityTaskCompleted    → returned RenderedConfig(...)

ActivityTaskScheduled    → push_config
ActivityTaskStarted
ActivityTaskCompleted    → returned PushResult(success=True, ...)

ActivityTaskScheduled    → validate_device_state
ActivityTaskStarted
ActivityTaskCompleted    → returned ValidationResult(drift=False, ...)

ActivityTaskScheduled    → write_provisioning_status
ActivityTaskStarted
ActivityTaskCompleted

WorkflowExecutionCompleted
```

Each event is timestamped and includes the full input/output payload. This is Temporal's durable event log — if the worker crashed mid-execution, it would replay from this history to resume exactly where it left off.

---

## Check workflow status via CLI

```bash
uv run python temporal/run_workflow.py status --workflow-id day1-DEV001
```

Output (when complete):

```
┌──────────────┬───────────────────────────────────────────┐
│ Field        │ Value                                     │
├──────────────┼───────────────────────────────────────────┤
│ workflow_id  │ day1-DEV001                               │
│ status       │ COMPLETED                                 │
│ started_at   │ 2026-06-30T14:23:11.123456Z               │
│ closed_at    │ 2026-06-30T14:23:11.987654Z               │
└──────────────┴───────────────────────────────────────────┘
```

---

## Read the logs

In the terminal running `make logs` (or run `make logs` now), you'll see structured output like:

```json
{"event": "activity started", "activity": "fetch_device_intent", "device_id": "DEV001", "request_id": "abc123", "level": "info"}
{"event": "mock: returning device intent", "device_id": "DEV001", "level": "debug"}
{"event": "activity started", "activity": "render_config", "device_id": "DEV001", "level": "info"}
{"event": "activity started", "activity": "push_config", "device_id": "DEV001", "level": "info"}
{"event": "mock: config push successful", "device_id": "DEV001", "level": "info"}
{"event": "activity started", "activity": "validate_device_state", "device_id": "DEV001", "level": "info"}
{"event": "validation passed", "device_id": "DEV001", "drift": false, "level": "info"}
```

Every log line shares a `request_id` that correlates to the originating API call. The `trace_id` field links to an OTel trace in Tempo (Tutorial 03).

---

## List all workflows

```bash
uv run python temporal/run_workflow.py list
```

This shows recent executions on the ZTP task queue:

```
┌─────────────────────────────┬───────────────┐
│ Workflow ID                 │ Status        │
├─────────────────────────────┼───────────────┤
│ day1-DEV001                 │ COMPLETED     │
└─────────────────────────────┴───────────────┘
```

---

## Try triggering drift

The mock validation activity has a 10% drift probability. Run the same device a few times and you'll eventually land in the HITL path:

```bash
for i in 1 2 3 4 5; do
    uv run python temporal/run_workflow.py start --device-id DEV00$i --requested-by you@example.com
    sleep 1
done

uv run python temporal/run_workflow.py list
```

Any workflow showing `RUNNING` has parked waiting for approval — that's the HITL gate. Tutorial 04 covers how to handle it.

---

## Run a Day 2 compliance scan

Scans cover a whole site (a group of devices):

```bash
uv run python temporal/run_workflow.py scan \
    --site-id SITE001 \
    --requested-by you@example.com
```

In the Temporal UI you'll see a `compliance-SITE001-*` workflow loop through each device in the site, calling `validate_device_state` for each.

---

## What you now know

- Workflows are submitted via the API (POST) and identified by a deterministic ID (`day1-<device>`)
- Temporal UI shows the full durable event history for every workflow — this is your primary debugging tool
- Structured logs include `activity`, `device_id`, and `request_id` fields for filtering
- `list` and `status` commands give you CLI-level workflow visibility

**Next:** [Tutorial 03 — Reading the Observability Stack](03-reading-observability.md)
