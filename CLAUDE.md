# CLAUDE.md — network-ztp

## Quick reference

```bash
uv run python temporal/worker.py           # start the worker
uv run python temporal/run_workflow.py     # CLI
uv run pytest tests/ -v                    # test suite (no live server needed)
uv run ruff check . && uv run ruff format . && uv run mypy temporal/   # full lint+type
```

**Package manager is `uv` exclusively.** Never use `pip`, `pip3`, `poetry`, or bare `python`.

---

## Repository layout

```
temporal/
  models.py                    Pydantic contracts — the single canonical data shape
                                for every activity boundary in all three phases.
                                Changes here have blast radius across every workflow.

  activities/
    nautobot_activities.py     Owns the Nautobot API boundary. All GraphQL and REST
                                calls live here. Has a USE_MOCK flag (see below).
    ansible_activities.py      Jinja2 render + Ansible push. Render and push are
                                intentionally TWO separate activities (see below).
    bootstrap_activities.py    Day 0 only: DHCP API, ZTP script render/publish,
                                reachability polling. Has a USE_MOCK flag.
    validation_activities.py   Post-push drift detection. Has _DRIFT_PROBABILITY
                                for controlling test failure rate.

  workflows/
    bootstrap_device.py        Day 0 orchestrator. Runs DHCP→script→publish→wait,
                                then fires Day 1 as a CHILD workflow.
    provision_site.py          Day 1 orchestrator. fetch→render→push→validate→[HITL].
    compliance_scan.py         Day 2 orchestrator. Independent — not a child of Day 1.

  worker.py                    Registers all 3 workflows + 10 activities. Single
                                task queue. Scale by running more replicas.
  run_workflow.py              CLI: bootstrap / start / scan / status / approve / list.

tests/
  test_activities.py           Day 1 activities (render, push, validate, nautobot).
  test_bootstrap_activities.py Day 0 activities (DHCP, ZTP script, reachability).
  test_compliance_activities.py Day 2 activity (fetch_site_devices).
  test_models.py               Pydantic model validation and JSON round-trips.
```

---

## The three-phase chain

```
Nautobot webhook / CLI
        │
        ▼
BootstrapDeviceWorkflow  (Day 0)
  register_dhcp_reservation
  fetch_device_intent
  render_bootstrap_script
  publish_bootstrap_script
  wait_for_device_reachability  ←── parks here up to 8 hours
        │
        │  execute_child_workflow()
        ▼
ProvisionSiteWorkflow  (Day 1)        ← independent child, own Temporal history
  fetch_device_intent
  render_config
  push_config
  validate_device_state
        │ drift?
        ▼
  [HITL: approve_escalation signal]

ComplianceScanWorkflow  (Day 2)       ← NOT a child — triggered separately on schedule
  fetch_site_devices
  for each device:
    fetch_device_intent
    validate_device_state
```

**Why Day 1 is a child workflow, not just more activities in Day 0:**
- Day 1 has its own Temporal history and is independently visible in the UI.
- A failed Day 1 can be retried without re-running Day 0 (re-racking, re-DHCP, etc.).
- The child workflow ID `f"day1-{device_id}-{workflow_id}"` embeds the parent ID for
  traceability; the parent's `workflow_id` is included so it's unique but derivable.

**Why Day 2 is separate, not a child of Day 1:**
- Compliance scans run on a schedule (every 4-6 hours), independent of any provisioning run.
- A compliance scan covers a whole site; Day 1 covers a single device.
- Making it a child would tie its lifecycle to a specific Day 1 run, which is wrong.

---

## Temporal determinism — the most critical rule

Temporal replays the workflow history to rebuild state after a crash. Any code that
produces different output on replay (I/O, time, randomness) causes a **non-deterministic
workflow error** that permanently breaks the execution.

**Inside `temporal/workflows/` the following are banned:**

| Banned | Use instead |
|--------|-------------|
| `datetime.now()` / `time.time()` | `workflow.now()` |
| `random.*` | `workflow.random()` |
| `asyncio.sleep()` | `workflow.sleep()` |
| `os.environ` reads | Read env at worker startup; pass via input models |
| HTTP / filesystem / subprocess | Put in an `@activity.defn` function |
| Any import with module-level side effects | Wrap in `workflow.unsafe.imports_passed_through()` |

**The `workflow.unsafe.imports_passed_through()` block** guards imports that Temporal's
sandbox would flag as non-deterministic. Every workflow file already has one. New imports
inside a workflow class go inside this block, not at the top of the file.

```python
# ✓ correct — inside the guard
with workflow.unsafe.imports_passed_through():
    from temporal.activities.ansible_activities import push_config, render_config
    from temporal.models import ProvisionSiteInput

# ✗ wrong — top-level import of activity modules in a workflow file
from temporal.activities.ansible_activities import push_config
```

**The `wait_condition` type-ignore:** `workflow.wait_condition()` returns `bool | None`
in the Temporal SDK type stubs but the value is always `bool` at runtime. The existing
`# type: ignore[func-returns-value, assignment]` comment is intentional — do not remove it.

---

## Activity design rules

### Render and push are separate activities — intentionally

`render_config` and `push_config` in `ansible_activities.py` are two distinct
`@activity.defn` functions even though they always run back-to-back. This is deliberate:

- The `RenderedConfig` object is stored in Temporal workflow history as an immutable
  artifact. If the push activity fails and retries, it pushes **the same bytes** that
  were approved at render time — not a re-render that might silently pick up a Nautobot
  change that occurred between render and push.
- Temporal can checkpoint between them: a crash after render but before push does not
  require re-rendering.

Never merge these two activities into one.

### The USE_MOCK flag

Two activity files have a `USE_MOCK = True` flag near the top of each function:

- `nautobot_activities.py` — `fetch_device_intent`, `write_provisioning_status`, `fetch_site_devices`
- `bootstrap_activities.py` — `register_dhcp_reservation`, `publish_bootstrap_script`

To go live against a real Nautobot instance:
1. Set `USE_MOCK = False` in the relevant function.
2. Set `NAUTOBOT_TOKEN` and `NAUTOBOT_URL` in `.env`.
3. The real HTTP path (using `httpx.AsyncClient`) is already written; no other code changes.

The mock data in `_mock_graphql_response()` is structured identically to a real
Nautobot GraphQL response — the parsing code runs in both modes.

### Jinja2 StrictUndefined

Both Jinja2 environments use `StrictUndefined`. A missing template variable raises
`jinja2.UndefinedError` immediately rather than silently rendering an empty string
into the device config. Do not change this to `Undefined` or `DebugUndefined`.

### Activity signatures and Pydantic

Every activity input and output must be a Pydantic `BaseModel` from `temporal/models.py`.
Temporal serialises activity boundaries as JSON. Raw `dict` types bypass validation and
will silently corrupt data during replay if the schema evolves.

- ✓ `async def render_config(intent: DeviceIntent) -> RenderedConfig`
- ✗ `async def render_config(intent: dict) -> dict`

### Single-responsibility: one API call per activity

Each Nautobot activity issues exactly one GraphQL or REST call. This keeps retries
scoped to the smallest possible unit of work. Do not batch multiple API calls inside a
single activity function.

---

## Retry policies — values and reasoning

From `provision_site.py` and `bootstrap_device.py`:

| Policy constant | max | initial | backoff | Used for |
|-----------------|-----|---------|---------|---------|
| `_RETRY_FETCH_INTENT` | 3 | 2s | 1.5× | Nautobot GraphQL — normally reliable, short backoff |
| `_RETRY_PUSH_CONFIG` | 3 | 5s | 2.0× (max 60s) | Ansible push — device may be briefly unreachable after WAN flap |
| `_RETRY_VALIDATE` | 2 | 10s | 1.5× | Post-push validation — give device time to converge before retry |
| `_RETRY_WRITE_STATUS` | 5 | 1s | 1.5× | Nautobot PATCH — cheap, must not be dropped, at-least-once OK |
| `_RETRY_STANDARD` (Day 0) | 3 | 2s | 1.5× | General Day 0 activities |
| `RetryPolicy(maximum_attempts=1)` | — | — | — | `wait_for_device_reachability` — the 8h timeout IS the retry strategy |

The `wait_for_device_reachability` pattern is deliberate: `maximum_attempts=1` means
the activity runs once and either completes within `start_to_close_timeout=8h` or fails.
There is no retry because the activity polls internally — retrying it would restart the
clock, not continue polling.

---

## Pydantic models — models.py is the contract layer

`temporal/models.py` owns the data shape for everything crossing an activity boundary.
It is the *only* file that defines these shapes.

**Before adding a field to any model:**
- New required fields (no default) will break existing `ProvisioningStatus` log entries
  in Temporal history on replay. Use `Field(default=...)` for all new fields unless
  you are certain no live workflows exist.
- `DeviceIntent` is read by every phase. Adding a field here requires updating
  `_mock_graphql_response()` in `nautobot_activities.py` and the `_make_intent()`
  helpers in all test files.
- `ProvisioningStatus` is a `StrEnum`. Add new states at the end of their phase block,
  never reorder existing values (Temporal stores the string values, not ordinal positions).

**Phase-specific models:**

| Phase | Input model | Result model |
|-------|-------------|--------------|
| Day 0 | `BootstrapDeviceInput` | `BootstrapDeviceResult` |
| Day 1 | `ProvisionSiteInput` | `ProvisionSiteResult` |
| Day 2 | `ComplianceScanInput` | `ComplianceScanResult` |

`DeviceIntent`, `RenderedConfig`, `PushResult`, `ValidationResult` are shared across phases.

---

## HITL escalation pattern

`ProvisionSiteWorkflow` parks on drift via a signal + `wait_condition` pattern:

```python
# In __init__:
self._approval_decision: str | None = None

# Signal handler writes to the buffer:
@workflow.signal
async def approve_escalation(self, decision: str) -> None:
    self._approval_decision = decision

# Main run() waits on the buffer:
condition_met: bool = await workflow.wait_condition(  # type: ignore[...]
    lambda: self._approval_decision is not None,
    timeout=timedelta(hours=24),
)
```

The signal is sent from `run_workflow.py`:
```bash
uv run python temporal/run_workflow.py approve --workflow-id <id> --decision approved
```

Valid decisions: `"approved"` (mark COMPLETE) or `"rejected"` (mark FAILED — engineer
fixes Nautobot intent and submits a new run). There is no automatic remediation.

---

## Roll-forward philosophy

This codebase never pushes a "rollback" config to a device. This is an architectural
decision, not an omission. The reasoning:

1. **The device's live state is not frozen.** BGP sessions reconverge, DHCP leases are
   handed out, spanning tree re-elects. A config snapshot taken before the push has
   an unknown relationship to that new reality.
2. **Nautobot is the only source of truth.** Pushing a config Nautobot does not currently
   describe deliberately creates the drift this pipeline exists to eliminate.
3. **Automated undo runs unattended.** In a manual maintenance window an engineer
   watches for interactions in real time. Automated rollback at scale does not have
   that safeguard.

On any failure the device is left in its current state. Nautobot is set to `FAILED`.
Recovery is a forward action: fix the intent in Nautobot, submit a new provisioning run.

**Do not add `except` blocks that push configs to devices, restore previous state, or
call Ansible with archived configs.** If you see a pattern that looks like compensation
or undo, question whether it belongs here.

---

## Testing

### No Temporal server needed

Activities are plain `async def` functions decorated with `@activity.defn`. The decorator
is a no-op outside a running worker. Tests call them directly:

```python
result = await fetch_device_intent("DEV001")   # no Worker, no Client, no server
```

Workflow tests use `temporalio.testing.WorkflowEnvironment.start_time_skipping()` — see
`tests/test_workflow.py`.  The `temporalio[testing]` extra must be installed for this
to work; it is already in `pyproject.toml` dev dependencies.

### asyncio_mode = "auto"

`pyproject.toml` sets `asyncio_mode = "auto"`. Every async test function runs
automatically in an event loop. **Do not add `@pytest.mark.asyncio` decorators** —
they are redundant and will trigger a ruff warning.

### The `_make_intent()` helper pattern

Every test file that needs a `DeviceIntent` uses a local `_make_intent()` factory with
sensible defaults. When you add a new required field to `DeviceIntent`, update the
`_make_intent()` in each test file: `test_activities.py`, `test_bootstrap_activities.py`,
`test_compliance_activities.py`, and `test_workflow.py`.

### _DRIFT_PROBABILITY

`validation_activities.py` has `_DRIFT_PROBABILITY = 0.10`. Tests that need a
deterministic failure (HITL path) should set this to `1.0` locally:

```python
import temporal.activities.validation_activities as va
va._DRIFT_PROBABILITY = 1.0
```

Restore it in a `finally` block or use `monkeypatch`.

---

## How to add a new activity

1. **Write the function** in the appropriate `activities/` file (or a new file for a
   new integration). Decorate with `@activity.defn`. Input and output must be
   Pydantic models from `models.py`.

2. **Add a mock path** if the activity calls an external system. Follow the
   `USE_MOCK = True` pattern already used in `nautobot_activities.py`.

3. **Register in `worker.py`** — add the function to `_REGISTERED_ACTIVITIES` in the
   correct phase comment block.

4. **Import in the workflow** that will call it — inside the
   `workflow.unsafe.imports_passed_through()` block.

5. **Write a test** in the appropriate `tests/test_*_activities.py` file. Call the
   activity directly (no Temporal server). Follow the class-per-activity pattern.

6. **Run the full check:** `uv run ruff check . && uv run mypy temporal/ && uv run pytest tests/ -v`

---

## Common mistakes to avoid

**In workflow files:**
- Using `datetime.now()` instead of `workflow.now()` — breaks replay determinism.
- Importing an activity module at the top of the file (outside the
  `imports_passed_through` block) — triggers Temporal sandbox violation.
- Calling `asyncio.sleep()` instead of `workflow.sleep()` — not durable; loses
  the wait on worker restart.
- Adding `try/except` that calls activities to "undo" a previous step — violates
  roll-forward philosophy.

**In activity files:**
- Merging render and push into one activity — breaks the immutable-artifact guarantee.
- Reading `os.environ` inside a workflow function (not an activity) — non-deterministic.
- Returning a raw `dict` instead of a Pydantic model — bypasses validation.
- Forgetting to register a new activity in `worker.py` — silent failure (Temporal
  times out waiting for a task that no worker knows how to execute).

**In test files:**
- Adding `@pytest.mark.asyncio` — redundant with `asyncio_mode = "auto"`, causes warnings.
- Importing `ProvisionSiteWorkflow` to test it without `WorkflowEnvironment` — the
  workflow sandbox will raise errors outside a proper test environment.
