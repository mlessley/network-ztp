I'm building a ZTP (zero-touch provisioning) automation system for 
enterprise SD-Branch deployments at bank scale (~5,000 branches). 
The stack uses Temporal for workflow orchestration, Nautobot as 
source of truth, and Ansible for config push. This is a portfolio 
project for a senior engineering interview so code quality, 
comments, and README must be excellent.

TEMPORAL SERVER: already running at localhost:7233
LANGUAGE: Python 3.11+
PACKAGE MANAGER: uv throughout — no pip, no poetry, no setup.py
DEPENDENCIES: temporalio, httpx, pydantic, python-dotenv, jinja2

CREATE THIS STRUCTURE AND IMPLEMENT FULLY:

sd-branch-ztp/
├── README.md
├── pyproject.toml
├── uv.lock
├── .env.example
├── .python-version
├── temporal/
│   ├── __init__.py
│   ├── models.py
│   ├── activities/
│   │   ├── __init__.py
│   │   ├── nautobot_activities.py
│   │   ├── ansible_activities.py
│   │   └── validation_activities.py
│   ├── workflows/
│   │   ├── __init__.py
│   │   └── provision_branch.py
│   ├── worker.py
│   └── run_workflow.py
└── .github/
    └── workflows/
        └── lint.yml

PACKAGE MANAGER RULES:
- Use uv for ALL Python tooling
- Initialize with: uv init --no-workspace
- Add deps with: uv add temporalio httpx pydantic python-dotenv jinja2
- Add dev deps with: uv add --dev ruff mypy pytest pytest-asyncio
- Never reference pip, poetry, or setup.py anywhere
- README must show uv commands for setup
- pyproject.toml must be uv-compatible (no tool.poetry section)
- .python-version file should contain: 3.11
- GitHub Actions must use uv:
    - Install uv with: curl -LsSf https://astral.sh/uv/install.sh | sh
    - Run tools with: uv run ruff check .
    - Run tests with: uv run pytest
    - No pip install steps anywhere in CI

MODELS (temporal/models.py):
Define these Pydantic models used across activities and workflows:
- DeviceIntent: device_id, hostname, platform, primary_ip, 
  interfaces list, vlans list, provisioning_status
- RenderedConfig: device_id, config_content, template_name, 
  rendered_at datetime
- PushResult: device_id, success bool, output str, 
  duration_seconds float
- ValidationResult: device_id, passed bool, 
  drift_detected list[str]
- ProvisionBranchInput: device_id str, requested_by str
- ProvisionBranchResult: device_id, success bool, 
  workflow_id str, completed_at datetime

ACTIVITIES (nautobot_activities.py):
- fetch_device_intent(device_id: str) -> DeviceIntent
  Mock an async httpx call to Nautobot GraphQL endpoint.
  Structure it as a real GraphQL query against 
  http://localhost:8080/graphql/ with proper query string.
  Return realistic mock data for a Cisco branch router.
  
- write_provisioning_status(device_id: str, status: str, 
  workflow_id: str) -> None
  Mock async httpx PATCH to Nautobot REST API custom fields.
  Log what would be written. Structure as real API call.

ACTIVITIES (ansible_activities.py):
- render_config(intent: DeviceIntent) -> RenderedConfig
  Use Jinja2 to render a realistic Cisco IOS-XE branch router 
  config template from the DeviceIntent. Include: hostname, 
  interfaces with IPs, VLANs, basic BGP stub, NTP, logging.
  
- push_config(config: RenderedConfig) -> PushResult
  Simulate ansible-runner execution. Log the command that 
  would run. Sleep 2 seconds to simulate work. Return 
  realistic PushResult.

ACTIVITIES (validation_activities.py):
- validate_device_state(device_id: str, 
  expected: DeviceIntent) -> ValidationResult
  Simulate connecting to device and comparing state to intent.
  90% of the time return passed=True.
  10% of the time return passed=False with realistic 
  drift_detected items like ["interface GigE0/1 IP mismatch",
  "VLAN 100 missing from trunk"].

WORKFLOW (provision_branch.py):
Implement ProvisionBranchWorkflow with:

1. SAGA/COMPENSATION PATTERN
   Build compensations list as steps succeed.
   On any failure run compensations in reverse order.
   Compensations: release_device_lock, restore_previous_config

2. STEP SEQUENCE
   - write_status(PROVISIONING_STARTED)
   - fetch_device_intent → register compensation
   - render_config
   - push_config → register compensation (restore config)
   - validate_device_state
   - If validation fails with drift: emit HITL signal, 
     wait for human approval (timeout 24hrs)
   - write_status(COMPLETE or FAILED)

3. RETRY POLICIES
   - fetch_device_intent: 3 retries, 2s initial backoff
   - push_config: 3 retries, 5s initial backoff, 
     exponential backoff coefficient 2.0
   - validate_device_state: 2 retries, 10s backoff
   - write_status: 5 retries, 1s backoff

4. HITL SIGNAL
   Signal name: approve_escalation
   Payload: decision str ("approved" or "rejected")
   If approved: proceed to write COMPLETE
   If rejected: run compensations, write FAILED
   If timeout (24hrs): escalate to ops, write FAILED

5. ACTIVITY TIMEOUTS
   All activities: start_to_close_timeout=timedelta(minutes=5)
   push_config: start_to_close_timeout=timedelta(minutes=15)

WORKER (worker.py):
- Connect to localhost:7233
- Namespace: default
- Task queue: ztp-queue
- Register workflow and all activities
- Expose Prometheus metrics on port 9091
- Graceful shutdown on SIGINT/SIGTERM
- Log worker startup with registered workflows/activities

RUN SCRIPT (run_workflow.py):
CLI with these modes:
  uv run python run_workflow.py start --device-id DEV001 --requested-by mark
  uv run python run_workflow.py status --workflow-id <id>
  uv run python run_workflow.py approve --workflow-id <id> --decision approved
  uv run python run_workflow.py list

pyproject.toml:
- Project name: sd-branch-ztp
- Python requires: >=3.11
- All deps with version pins
- Ruff config: line-length 100, target py311
- Mypy config: strict mode
- Pytest config: asyncio_mode = auto
- NO tool.poetry section — uv native format only

.github/workflows/lint.yml:
- Trigger on push and PR to main
- Install uv via curl installer
- Cache uv's cache directory
- Steps:
    uv sync --frozen
    uv run ruff check .
    uv run ruff format --check .
    uv run mypy temporal/
    uv run pytest tests/ -v
- No pip install steps anywhere

README.md - write as a genuine architecture document:
- Project purpose: ZTP for enterprise SD-Branch at scale
- Architecture diagram in ASCII showing data flow:
  Nautobot → Temporal → Ansible → Device → status back to Nautobot
- Why each component exists (not just what it does)
- Nautobot as Source of Intent explanation
- Temporal durable execution and why it matters for ZTP
- HITL escalation pattern explanation
- Local dev setup using uv:
    uv sync
    uv run python temporal/worker.py
    uv run python temporal/run_workflow.py start --device-id DEV001 --requested-by mark
- Assumes Temporal at localhost:7233, Nautobot at localhost:8080
- How to run end to end
- How to test the HITL escalation path
- Future: React site survey UI, Terraform production deployment

Use proper async/await throughout. All Temporal decorators 
correct. Type hints everywhere. Docstrings on all classes 
and public methods. This code will be reviewed by a 
senior engineering hiring manager.