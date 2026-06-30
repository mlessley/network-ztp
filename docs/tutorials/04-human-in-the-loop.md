# Tutorial 04: Human-in-the-Loop Escalation

**Goal:** Understand the HITL drift escalation path — force a workflow into the waiting state, find it, and send an approval or rejection.

**Time:** ~20 minutes

**Prerequisites:** Tutorial 01 complete — stack running.

---

## What is HITL?

After a Day 1 config push, the `validate_device_state` activity compares the device's live state to the intended state from Nautobot. If they diverge (drift), the workflow does **not** fail automatically. Instead it:

1. Records the drift details
2. Marks status as `HITL_PENDING` in Nautobot
3. **Parks** — suspends itself, holding all state in Temporal
4. Waits up to 24 hours for a human signal

A human engineer reviews the situation, then sends either `approved` or `rejected` via the CLI or API. The workflow resumes from exactly where it parked.

This is the `wait_condition` + signal pattern in Temporal — the workflow consumes zero CPU while parked.

---

## Step 1: Force a drift scenario

The mock `validate_device_state` has a 10% random drift probability. The easiest way to guarantee a drift is to temporarily patch it before triggering. But in a running container the simpler approach is to trigger many runs until one drifts:

```bash
for i in $(seq 1 20); do
    uv run python temporal/run_workflow.py start \
        --device-id "DEV$(printf '%03d' $i)" \
        --requested-by you@example.com
    sleep 0.5
done
```

Then check the list for any that are still `RUNNING`:

```bash
uv run python temporal/run_workflow.py list
```

A workflow that's `RUNNING` after more than a few seconds has parked waiting for HITL approval.

> **Tip:** With 20 devices and 10% drift probability you expect about 2 to land in HITL. If you're impatient, run 50.

---

## Step 2: Find the parked workflow

In the Temporal UI (`http://localhost:8080`), click **Workflows** and filter by **Status: Running**.

You'll see any parked workflows. Click one. The event history ends at `WorkflowExecutionSignaled`... except it hasn't received a signal yet. The last event will be an activity completion (`validate_device_state`) followed by a `TimerStarted` (the 24-hour timeout clock).

The workflow is suspended at:

```python
condition_met = await workflow.wait_condition(
    lambda: self._approval_decision is not None,
    timeout=timedelta(hours=24),
)
```

It will stay here until you send a signal.

---

## Step 3: Check it via CLI

```bash
uv run python temporal/run_workflow.py status --workflow-id day1-DEV007
```

You'll see `status: RUNNING`. The workflow isn't stuck — it's intentionally parked.

---

## Step 4: Send an approval signal

To approve (mark as fixed, continue to COMPLETE):

```bash
uv run python temporal/run_workflow.py approve \
    --workflow-id day1-DEV007 \
    --decision approved
```

Expected output:

```
Decision 'approved' sent to day1-DEV007
```

Immediately in the Temporal UI, you'll see the workflow resume:
- The `WorkflowExecutionSignaled` event appears with `approve_escalation` as the signal name and `{"decision": "approved"}` as the payload
- `write_provisioning_status` fires one more time to mark `COMPLETE`
- `WorkflowExecutionCompleted` appears

---

## Step 5: Send a rejection signal

To reject (engineer needs to fix Nautobot intent and submit a new run):

```bash
uv run python temporal/run_workflow.py approve \
    --workflow-id day1-DEV008 \
    --decision rejected
```

In Temporal UI, the workflow will:
- Receive the signal
- Write `FAILED` status to Nautobot
- Exit with a failure result

The workflow is now `FAILED` in Temporal. Recovery is a forward action: fix the Nautobot intent, then submit a new provisioning run with a different workflow ID.

> **Why no automatic rollback?** The system is designed around a "roll-forward" philosophy — it never pushes a previous config to a device because: (1) the live network state has changed since the snapshot, (2) Nautobot is the only source of truth, and (3) automated rollback runs unattended with no engineer watching for interactions. See `CLAUDE.md` for the full reasoning.

---

## Step 6: Watch the HITL metric clear

After approving or rejecting, check Grafana → **Worker Overview** → **HITL Pending** panel. The gauge should drop by one for each workflow you resolved.

In Prometheus:

```promql
ztp_hitl_pending_total
```

It should return to 0 once all parked workflows are resolved.

---

## What happens if nobody responds?

After 24 hours of no signal, the `wait_condition` timeout fires. The workflow treats this as a rejection — writes `FAILED` to Nautobot and exits. The `ZTPHITLStalePending` alert fires (after 2 hours in the rule config) to page someone before the automatic timeout.

---

## The via-API path

The CLI `approve` command is just wrapping this API call:

```bash
curl -s -X POST \
  http://localhost:8000/v1/workflows/day1-DEV007/approve \
  -H "Content-Type: application/json" \
  -H "X-Authenticated-User: engineer:ENGINEER:SOUTH" \
  -d '{"decision": "approved"}' | python3 -m json.tool
```

Any system that can make an HTTP POST can send the approval signal — a ticketing system webhook, a Slack bot, a human clicking a button in a custom UI.

---

## What you now know

- Drift during validation parks the workflow in Temporal — it holds all state and waits up to 24 hours
- You signal the workflow with `approve` or `reject` via CLI or API
- `approved` → workflow continues to COMPLETE; `rejected` → workflow exits FAILED, forward action required
- The `ztp_hitl_pending_total` gauge tracks how many workflows are currently parked
- The `ZTPHITLStalePending` alert fires if a workflow waits more than 2 hours

**Next:** [Tutorial 05 — Exploring the REST API](05-exploring-the-api.md)
