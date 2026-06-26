"""
CLI for triggering and inspecting ZTP workflow executions.

Commands:
    bootstrap  — Day 0: trigger device bootstrap from factory default
    start      — Day 1: trigger full intent provisioning for a device
    scan       — Day 2: run a compliance scan across a site
    status     — query the current state of a running or completed workflow
    approve    — send the approve_escalation signal to a HITL-parked workflow
    list       — show recent workflow executions on the ZTP task queue

Usage examples:
    uv run python temporal/run_workflow.py bootstrap --device-id DEV001 --mac aa:bb:cc:dd:ee:ff --requested-by mark
    uv run python temporal/run_workflow.py start --device-id DEV001 --requested-by mark
    uv run python temporal/run_workflow.py scan --site-id SITE001 --requested-by mark
    uv run python temporal/run_workflow.py status --workflow-id <id>
    uv run python temporal/run_workflow.py approve --workflow-id <id> --decision approved
    uv run python temporal/run_workflow.py list
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid

from dotenv import load_dotenv
from temporalio.client import Client, WorkflowExecutionStatus

from temporal.models import BootstrapDeviceInput, ComplianceScanInput, ProvisionSiteInput
from temporal.workflows.bootstrap_device import BootstrapDeviceWorkflow
from temporal.workflows.compliance_scan import ComplianceScanWorkflow
from temporal.workflows.provision_site import ProvisionSiteWorkflow

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "ztp-queue")


async def _connect() -> Client:
    return await Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)


# ---------------------------------------------------------------------------
# Command: bootstrap (Day 0)
# ---------------------------------------------------------------------------


async def cmd_bootstrap(device_id: str, mac_address: str, requested_by: str) -> None:
    """Trigger Day 0 bootstrap for a device arriving from factory."""
    client = await _connect()

    workflow_id = f"bootstrap-{device_id}-{uuid.uuid4().hex[:8]}"

    handle = await client.start_workflow(
        BootstrapDeviceWorkflow.run,
        BootstrapDeviceInput(
            device_id=device_id,
            mac_address=mac_address,
            requested_by=requested_by,
        ),
        id=workflow_id,
        task_queue=TEMPORAL_TASK_QUEUE,
    )

    print("Bootstrap workflow started")
    print(f"  Workflow ID : {handle.id}")
    print(f"  Device      : {device_id}")
    print(f"  MAC address : {mac_address}")
    print(f"  Requested by: {requested_by}")
    print("\nThe workflow will:")
    print("  1. Register a DHCP reservation for the device MAC")
    print("  2. Render and publish the IOS-XE ZTP bootstrap script")
    print("  3. Wait up to 8 hours for the device to come online")
    print("  4. Automatically trigger Day 1 provisioning when reachable")
    print(
        f"\nCheck status:\n  uv run python temporal/run_workflow.py status --workflow-id {handle.id}"
    )


# ---------------------------------------------------------------------------
# Command: start (Day 1)
# ---------------------------------------------------------------------------


async def cmd_start(device_id: str, requested_by: str) -> None:
    """Submit a Day 1 ProvisionSiteWorkflow execution directly."""
    client = await _connect()

    workflow_id = f"day1-{device_id}-{uuid.uuid4().hex[:8]}"

    handle = await client.start_workflow(
        ProvisionSiteWorkflow.run,
        ProvisionSiteInput(device_id=device_id, requested_by=requested_by),
        id=workflow_id,
        task_queue=TEMPORAL_TASK_QUEUE,
    )

    print("Provisioning workflow started")
    print(f"  Workflow ID : {handle.id}")
    print(f"  Device      : {device_id}")
    print(f"  Requested by: {requested_by}")
    print(
        f"\nTrack in Temporal UI: http://localhost:8080/namespaces/{TEMPORAL_NAMESPACE}/workflows/{handle.id}"
    )
    print(
        f"\nCheck status:\n  uv run python temporal/run_workflow.py status --workflow-id {handle.id}"
    )


# ---------------------------------------------------------------------------
# Command: scan (Day 2)
# ---------------------------------------------------------------------------


async def cmd_scan(site_id: str, requested_by: str, device_ids: list[str]) -> None:
    """Run a Day 2 compliance scan across a site."""
    client = await _connect()

    workflow_id = f"scan-{site_id}-{uuid.uuid4().hex[:8]}"

    handle = await client.start_workflow(
        ComplianceScanWorkflow.run,
        ComplianceScanInput(
            site_id=site_id,
            requested_by=requested_by,
            device_ids=device_ids,
        ),
        id=workflow_id,
        task_queue=TEMPORAL_TASK_QUEUE,
    )

    scope = (
        f"{len(device_ids)} explicit devices" if device_ids else f"all devices at site {site_id}"
    )
    print("Compliance scan started")
    print(f"  Workflow ID : {handle.id}")
    print(f"  Site        : {site_id}")
    print(f"  Scope       : {scope}")
    print(f"  Requested by: {requested_by}")
    print(
        f"\nCheck status:\n  uv run python temporal/run_workflow.py status --workflow-id {handle.id}"
    )


# ---------------------------------------------------------------------------
# Command: status
# ---------------------------------------------------------------------------


async def cmd_status(workflow_id: str) -> None:
    """Print the execution status and result of a workflow."""
    client = await _connect()
    handle = client.get_workflow_handle(workflow_id)

    desc = await handle.describe()
    status = desc.status

    _status_labels: dict[WorkflowExecutionStatus, str] = {
        WorkflowExecutionStatus.RUNNING: "RUNNING",
        WorkflowExecutionStatus.COMPLETED: "COMPLETED",
        WorkflowExecutionStatus.FAILED: "FAILED",
        WorkflowExecutionStatus.CANCELED: "CANCELED",
        WorkflowExecutionStatus.TERMINATED: "TERMINATED",
        WorkflowExecutionStatus.TIMED_OUT: "TIMED_OUT",
        WorkflowExecutionStatus.CONTINUED_AS_NEW: "CONTINUED_AS_NEW",
    }
    status_label = _status_labels.get(status) if status is not None else "UNKNOWN"
    status_label = status_label or str(status)

    print(f"Workflow: {workflow_id}")
    print(f"  Status    : {status_label}")
    print(f"  Started   : {desc.start_time}")
    print(f"  Task queue: {desc.task_queue}")

    if status == WorkflowExecutionStatus.COMPLETED:
        try:
            result = await handle.result()
            print(f"  Result    : {json.dumps(result, indent=4, default=str)}")
        except Exception as exc:
            print(f"  Error     : {exc}")


# ---------------------------------------------------------------------------
# Command: approve
# ---------------------------------------------------------------------------


async def cmd_approve(workflow_id: str, decision: str) -> None:
    """Send the approve_escalation signal to a HITL-parked workflow."""
    if decision not in ("approved", "rejected"):
        print(f"Error: --decision must be 'approved' or 'rejected', got '{decision}'")
        sys.exit(1)

    client = await _connect()
    handle = client.get_workflow_handle(workflow_id)

    await handle.signal(ProvisionSiteWorkflow.approve_escalation, decision)

    print("Signal sent successfully")
    print(f"  Workflow ID: {workflow_id}")
    print(f"  Decision   : {decision}")
    print("\nThe workflow will now proceed based on your decision.")


# ---------------------------------------------------------------------------
# Command: list
# ---------------------------------------------------------------------------


async def cmd_list() -> None:
    """List recent workflow executions on the ZTP task queue."""
    client = await _connect()

    print(f"Recent ZTP workflow executions (namespace={TEMPORAL_NAMESPACE}):\n")

    count = 0
    async for execution in client.list_workflows(
        f'TaskQueue="{TEMPORAL_TASK_QUEUE}" order by StartTime desc',
        page_size=20,
    ):
        status = execution.status.name if execution.status else "UNKNOWN"
        print(
            f"  {execution.id:<50} {status:<15} started={execution.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        count += 1

    if count == 0:
        print("  No executions found.")
    else:
        print(f"\n  Total: {count} execution(s)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_workflow",
        description="network-ztp workflow CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # bootstrap (Day 0)
    p_bootstrap = sub.add_parser("bootstrap", help="Day 0: bootstrap a device from factory default")
    p_bootstrap.add_argument("--device-id", required=True, help="Nautobot device UUID")
    p_bootstrap.add_argument(
        "--mac", required=True, dest="mac_address", help="Factory MAC address of management port"
    )
    p_bootstrap.add_argument("--requested-by", required=True, help="Requester identity")

    # start (Day 1)
    p_start = sub.add_parser("start", help="Day 1: trigger full intent provisioning")
    p_start.add_argument("--device-id", required=True, help="Nautobot device UUID")
    p_start.add_argument("--requested-by", required=True, help="Requester identity")

    # scan (Day 2)
    p_scan = sub.add_parser("scan", help="Day 2: compliance scan across a site")
    p_scan.add_argument("--site-id", required=True, help="Nautobot site UUID or slug")
    p_scan.add_argument("--requested-by", required=True, help="Requester identity")
    p_scan.add_argument(
        "--device-ids",
        default="",
        help="Comma-separated device IDs to scan (default: all devices at site)",
    )

    # status
    p_status = sub.add_parser("status", help="Query workflow status")
    p_status.add_argument("--workflow-id", required=True, help="Temporal workflow ID")

    # approve
    p_approve = sub.add_parser("approve", help="Send HITL approval signal")
    p_approve.add_argument("--workflow-id", required=True, help="Temporal workflow ID")
    p_approve.add_argument(
        "--decision",
        required=True,
        choices=["approved", "rejected"],
        help="Operator decision",
    )

    # list
    sub.add_parser("list", help="List recent workflow executions")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "bootstrap":
        asyncio.run(cmd_bootstrap(args.device_id, args.mac_address, args.requested_by))
    elif args.command == "start":
        asyncio.run(cmd_start(args.device_id, args.requested_by))
    elif args.command == "scan":
        device_ids = [d.strip() for d in args.device_ids.split(",") if d.strip()]
        asyncio.run(cmd_scan(args.site_id, args.requested_by, device_ids))
    elif args.command == "status":
        asyncio.run(cmd_status(args.workflow_id))
    elif args.command == "approve":
        asyncio.run(cmd_approve(args.workflow_id, args.decision))
    elif args.command == "list":
        asyncio.run(cmd_list())
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
