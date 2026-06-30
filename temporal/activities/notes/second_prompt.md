ADDENDUM — apply these changes on top of the previous prompt:

PACKAGE CHANGES:
Replace httpx with httpx2 throughout — same API, drop-in replacement.
Import as: import httpx2 as httpx

Add these production dependencies:
  napalm          # vendor-agnostic network device state validation
  netmiko         # SSH to network devices, used by napalm under the hood
  ansible-runner  # programmatic Ansible execution from Python activities
  pynautobot      # official Nautobot Python client for REST API calls
  tenacity        # fine-grained retry logic inside activities
  structlog       # structured JSON logging throughout worker and activities
  rich            # formatted CLI output in run_workflow.py

Add these dev dependencies:
  respx           # mock httpx2 calls in tests
  freezegun       # freeze time for testing workflow timeouts

Full uv command:
  uv add httpx2 napalm netmiko ansible-runner pynautobot tenacity structlog rich
  uv add --dev respx freezegun

ACTIVITY CHANGES:

nautobot_activities.py:
- Replace raw httpx2 calls with pynautobot client where it makes sense
  (pynautobot for REST CRUD, httpx2 for GraphQL queries)
- Use structlog for all logging, not print() or stdlib logging
- Example structlog usage:
    import structlog
    log = structlog.get_logger()
    log.info("fetching_device_intent", device_id=device_id)

ansible_activities.py:
- render_config: structure the Jinja2 template to produce realistic
  Cisco IOS-XE config including hostname, interfaces, VLANs,
  BGP stub, NTP, logging, SNMP
- push_config: use ansible_runner.run() instead of subprocess
  Mock the actual execution but import and call ansible_runner
  so the real pattern is clear
- Use structlog for logging

validation_activities.py:
- Structure validate_device_state to show NAPALM would be used:
    import napalm
    driver = napalm.get_network_driver("ios")
    device = driver(hostname=..., username=..., password=...)
  Mock the actual connection but show the real NAPALM pattern
- Include get_interfaces() and get_vlans() as the methods
  that would be called for validation
- Use structlog for logging

worker.py:
- Use structlog for all worker lifecycle logging
- Configure structlog at startup with JSON renderer for production,
  ConsoleRenderer for dev (check ENV var)

run_workflow.py:
- Use rich for all output:
  - rich.table.Table for workflow list output
  - rich.console.Console for status output
  - rich.progress for polling workflow status

TESTING:
Add a tests/ directory with:
  tests/
  ├── __init__.py
  ├── test_models.py        # pydantic model validation tests
  ├── test_activities.py    # activity unit tests with respx mocks
  └── test_workflow.py      # temporal workflow tests using
                              temporalio.testing.WorkflowEnvironment

test_activities.py should use respx to mock httpx2 calls:
  import respx
  import httpx2
  with respx.mock:
      respx.post("http://localhost:8080/graphql/").mock(
          return_value=httpx2.Response(200, json={...})
      )

test_workflow.py should use Temporal's test environment:
  from temporalio.testing import WorkflowEnvironment
  from temporalio.worker import Worker
  async def test_provision_branch_happy_path():
      async with await WorkflowEnvironment.start_time_skipping() as env:
          async with Worker(env.client, ...):
              result = await env.client.execute_workflow(...)
              assert result.success is True

STACK SUMMARY (update README to reflect):
Runtime:
  Python 3.11, uv, Temporal, Nautobot, Ansible
  httpx2, pynautobot, napalm, netmiko, ansible-runner
  jinja2, pydantic, python-dotenv
  tenacity, structlog, rich

Observability:
  Prometheus (scraped from worker on :9091)
  Grafana (dashboards for workflow metrics)

Dev tooling:
  ruff, mypy, pytest, pytest-asyncio, respx, freezegun

Infrastructure (local dev via docker-compose / DevX):
  Temporal (localhost:7233, UI at localhost:8233)
  Nautobot (localhost:8080)
  PostgreSQL (Nautobot backend)
  Redis (Nautobot task queue)
  Prometheus + Grafana (standalone, shared across projects)

Future V2:
  React + TypeScript — site survey / provisioning intake UI
  FastAPI — API layer between React and Temporal
  Terraform — production environment provisioning