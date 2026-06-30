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
    uv run python temporal/run_workflow.py bootstrap \\
        --device-id DEV001 --mac aa:bb:cc:dd:ee:ff --requested-by mark
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
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text
from temporalio.client import Client, WorkflowExecutionStatus

from temporal.models import BootstrapDeviceInput, ComplianceScanInput, ProvisionSiteInput
from temporal.workflows.bootstrap_device import BootstrapDeviceWorkflow
from temporal.workflows.compliance_scan import ComplianceScanWorkflow
from temporal.workflows.provision_site import ProvisionSiteWorkflow

load_dotenv()

logging.disable(logging.CRITICAL)  # silence SDK noise; this CLI uses rich for output

console = Console()

TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "ztp-queue")

_STATUS_STYLES: dict[WorkflowExecutionStatus, str] = {
    WorkflowExecutionStatus.RUNNING: "bold cyan",
    WorkflowExecutionStatus.COMPLETED: "bold green",
    WorkflowExecutionStatus.FAILED: "bold red",
    WorkflowExecutionStatus.CANCELED: "yellow",
    WorkflowExecutionStatus.TERMINATED: "red",
    WorkflowExecutionStatus.TIMED_OUT: "bold red",
    WorkflowExecutionStatus.CONTINUED_AS_NEW: "blue",
}

_STATUS_LABELS: dict[WorkflowExecutionStatus, str] = {
    WorkflowExecutionStatus.RUNNING: "RUNNING",
    WorkflowExecutionStatus.COMPLETED: "COMPLETED",
    WorkflowExecutionStatus.FAILED: "FAILED",
    WorkflowExecutionStatus.CANCELED: "CANCELED",
    WorkflowExecutionStatus.TERMINATED: "TERMINATED",
    WorkflowExecutionStatus.TIMED_OUT: "TIMED_OUT",
    WorkflowExecutionStatus.CONTINUED_AS_NEW: "CONTINUED_AS_NEW",
}


async def _connect() -> Client:
    return await Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)


# ---------------------------------------------------------------------------
# Command: bootstrap (Day 0)
# ---------------------------------------------------------------------------


async def cmd_bootstrap(device_id: str, mac_address: str, requested_by: str) -> None:
    """Trigger Day 0 bootstrap for a device arriving from factory."""
    client = await _connect()
    workflow_id = f"bootstrap-{device_id}-{uuid.uuid4().hex[:8]}"

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        progress.add_task("Submitting bootstrap workflow…", total=None)
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

    console.print(
        Panel(
            "\n".join(
                [
                    f"[bold]Workflow ID :[/bold]  {handle.id}",
                    f"[bold]Device      :[/bold]  {device_id}",
                    f"[bold]MAC address :[/bold]  {mac_address}",
                    f"[bold]Requested by:[/bold]  {requested_by}",
                    "",
                    "[dim]The workflow will:[/dim]",
                    "  1. Register a DHCP reservation for the device MAC",
                    "  2. Render and publish the IOS-XE ZTP bootstrap script",
                    "  3. Wait up to 8 hours for the device to come online",
                    "  4. Automatically trigger Day 1 provisioning when reachable",
                    "",
                    f"[dim]uv run python temporal/run_workflow.py status"
                    f" --workflow-id {handle.id}[/dim]",
                ]
            ),
            title="[bold green]Bootstrap workflow started[/bold green]",
            border_style="green",
        )
    )


# ---------------------------------------------------------------------------
# Command: start (Day 1)
# ---------------------------------------------------------------------------


async def cmd_start(device_id: str, requested_by: str) -> None:
    """Submit a Day 1 ProvisionSiteWorkflow execution directly."""
    client = await _connect()
    workflow_id = f"day1-{device_id}-{uuid.uuid4().hex[:8]}"

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        progress.add_task("Submitting provisioning workflow…", total=None)
        handle = await client.start_workflow(
            ProvisionSiteWorkflow.run,
            ProvisionSiteInput(device_id=device_id, requested_by=requested_by),
            id=workflow_id,
            task_queue=TEMPORAL_TASK_QUEUE,
        )

    console.print(
        Panel(
            "\n".join(
                [
                    f"[bold]Workflow ID :[/bold]  {handle.id}",
                    f"[bold]Device      :[/bold]  {device_id}",
                    f"[bold]Requested by:[/bold]  {requested_by}",
                    "",
                    f"[dim]Temporal UI: http://localhost:8233/namespaces/{TEMPORAL_NAMESPACE}"
                    f"/workflows/{handle.id}[/dim]",
                    f"[dim]uv run python temporal/run_workflow.py status"
                    f" --workflow-id {handle.id}[/dim]",
                ]
            ),
            title="[bold green]Provisioning workflow started[/bold green]",
            border_style="green",
        )
    )


# ---------------------------------------------------------------------------
# Command: scan (Day 2)
# ---------------------------------------------------------------------------


async def cmd_scan(site_id: str, requested_by: str, device_ids: list[str]) -> None:
    """Run a Day 2 compliance scan across a site."""
    client = await _connect()
    workflow_id = f"scan-{site_id}-{uuid.uuid4().hex[:8]}"

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        progress.add_task("Submitting compliance scan…", total=None)
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
        f"{len(device_ids)} explicit devices"
        if device_ids
        else f"all devices at site [bold]{site_id}[/bold]"
    )
    console.print(
        Panel(
            "\n".join(
                [
                    f"[bold]Workflow ID :[/bold]  {handle.id}",
                    f"[bold]Site        :[/bold]  {site_id}",
                    f"[bold]Scope       :[/bold]  {scope}",
                    f"[bold]Requested by:[/bold]  {requested_by}",
                    "",
                    f"[dim]uv run python temporal/run_workflow.py status"
                    f" --workflow-id {handle.id}[/dim]",
                ]
            ),
            title="[bold green]Compliance scan started[/bold green]",
            border_style="green",
        )
    )


# ---------------------------------------------------------------------------
# Command: status
# ---------------------------------------------------------------------------


async def cmd_status(workflow_id: str) -> None:
    """Print the execution status and result of a workflow."""
    client = await _connect()
    handle = client.get_workflow_handle(workflow_id)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        progress.add_task("Fetching workflow status…", total=None)
        desc = await handle.describe()

    status = desc.status
    label = _STATUS_LABELS.get(status, "UNKNOWN") if status is not None else "UNKNOWN"
    style = _STATUS_STYLES.get(status, "white") if status is not None else "white"

    lines = [
        f"[bold]Workflow ID:[/bold]  {workflow_id}",
        f"[bold]Status     :[/bold]  [{style}]{label}[/{style}]",
        f"[bold]Started    :[/bold]  {desc.start_time}",
        f"[bold]Task queue :[/bold]  {desc.task_queue or ''}",
    ]

    if status == WorkflowExecutionStatus.COMPLETED:
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True,
            ) as p2:
                p2.add_task("Fetching result…", total=None)
                result = await handle.result()
            lines += ["", f"[dim]{json.dumps(result, indent=2, default=str)}[/dim]"]
        except Exception as exc:
            lines.append(f"[bold red]Error:[/bold red]  {exc}")

    console.print(
        Panel(
            "\n".join(lines),
            title="[bold]Workflow Status[/bold]",
            border_style=style,
        )
    )


# ---------------------------------------------------------------------------
# Command: approve
# ---------------------------------------------------------------------------


async def cmd_approve(workflow_id: str, decision: str) -> None:
    """Send the approve_escalation signal to a HITL-parked workflow."""
    if decision not in ("approved", "rejected"):
        console.print(
            f"[bold red]Error:[/bold red] --decision must be 'approved' or 'rejected',"
            f" got '{decision}'"
        )
        sys.exit(1)

    client = await _connect()
    handle = client.get_workflow_handle(workflow_id)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        progress.add_task("Sending signal…", total=None)
        await handle.signal(ProvisionSiteWorkflow.approve_escalation, decision)

    decision_style = "green" if decision == "approved" else "yellow"
    console.print(
        Panel(
            "\n".join(
                [
                    f"[bold]Workflow ID:[/bold]  {workflow_id}",
                    f"[bold]Decision   :[/bold]  [{decision_style}]{decision}[/{decision_style}]",
                    "",
                    "The workflow will now proceed based on your decision.",
                ]
            ),
            title="[bold green]Signal sent[/bold green]",
            border_style="green",
        )
    )


# ---------------------------------------------------------------------------
# Command: list
# ---------------------------------------------------------------------------


async def cmd_list() -> None:
    """List recent workflow executions on the ZTP task queue."""
    client = await _connect()

    table = Table(
        title=f"ZTP Workflows  [dim](namespace={TEMPORAL_NAMESPACE})[/dim]",
        show_header=True,
        header_style="bold",
        border_style="dim",
        show_lines=False,
    )
    table.add_column("Workflow ID", style="cyan", no_wrap=True, max_width=52)
    table.add_column("Status", no_wrap=True, width=14)
    table.add_column("Type", style="dim", no_wrap=True, width=28)
    table.add_column("Started", style="dim", no_wrap=True)

    count = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        progress.add_task("Fetching executions…", total=None)
        async for execution in client.list_workflows(
            f'TaskQueue="{TEMPORAL_TASK_QUEUE}" order by StartTime desc',
            page_size=25,
        ):
            status = execution.status
            label = _STATUS_LABELS.get(status, "UNKNOWN") if status else "UNKNOWN"
            style = _STATUS_STYLES.get(status, "white") if status else "white"
            table.add_row(
                execution.id,
                Text(label, style=style),
                execution.workflow_type or "",
                (
                    execution.start_time.strftime("%Y-%m-%d %H:%M:%S")
                    if execution.start_time
                    else ""
                ),
            )
            count += 1

    console.print(table)
    if count == 0:
        console.print("[dim]No executions found.[/dim]")
    else:
        console.print(f"[dim]{count} execution(s)[/dim]")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_workflow",
        description="network-ztp workflow CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_bootstrap = sub.add_parser("bootstrap", help="Day 0: bootstrap a device from factory")
    p_bootstrap.add_argument("--device-id", required=True, help="Nautobot device UUID")
    p_bootstrap.add_argument(
        "--mac", required=True, dest="mac_address", help="Factory MAC of management port"
    )
    p_bootstrap.add_argument("--requested-by", required=True, help="Requester identity")

    p_start = sub.add_parser("start", help="Day 1: trigger full intent provisioning")
    p_start.add_argument("--device-id", required=True, help="Nautobot device UUID")
    p_start.add_argument("--requested-by", required=True, help="Requester identity")

    p_scan = sub.add_parser("scan", help="Day 2: compliance scan across a site")
    p_scan.add_argument("--site-id", required=True, help="Nautobot site UUID or slug")
    p_scan.add_argument("--requested-by", required=True, help="Requester identity")
    p_scan.add_argument(
        "--device-ids",
        default="",
        help="Comma-separated device IDs (default: all devices at site)",
    )

    p_status = sub.add_parser("status", help="Query workflow status")
    p_status.add_argument("--workflow-id", required=True, help="Temporal workflow ID")

    p_approve = sub.add_parser("approve", help="Send HITL approval signal")
    p_approve.add_argument("--workflow-id", required=True, help="Temporal workflow ID")
    p_approve.add_argument(
        "--decision",
        required=True,
        choices=["approved", "rejected"],
        help="Engineer decision on detected drift",
    )

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
