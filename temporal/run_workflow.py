"""
CLI client for the network-ztp API.

All commands submit requests to the FastAPI service. The API_BASE_URL
environment variable controls the target (default: http://localhost:8000).

Commands:
    bootstrap         — Day 0: trigger device bootstrap from factory default
    start             — Day 1: trigger full intent provisioning for a device
    scan              — Day 2: run a compliance scan across a site
    status            — query the current state of a running or completed workflow
    approve           — send the approve_escalation signal to a HITL-parked workflow
    list              — show recent workflow executions on the ZTP task queue
    onboard           — Day 0.5: onboard a single site
    bulk-onboard      — Day 0.5: onboard multiple sites in bulk
    onboard-status    — Day 0.5: check the status of the bulk onboarding workflow
"""

from __future__ import annotations

import os

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

_console = Console()
_API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=_API_BASE, timeout=30)


# ---------------------------------------------------------------------------
# Command: bootstrap (Day 0)
# ---------------------------------------------------------------------------


async def cmd_bootstrap(device_id: str, requested_by: str) -> None:
    async with _client() as c:
        r = await c.post(f"/v1/devices/{device_id}/bootstrap", json={"requested_by": requested_by})
    r.raise_for_status()
    data = r.json()
    _console.print(
        Panel(
            f"[green]Bootstrap submitted[/green]\nWorkflow: {data['workflow_id']}\nStatus: {data['status_url']}"
        )
    )


# ---------------------------------------------------------------------------
# Command: start (Day 1)
# ---------------------------------------------------------------------------


async def cmd_start(device_id: str, requested_by: str) -> None:
    async with _client() as c:
        r = await c.post(f"/v1/devices/{device_id}/provision", json={"requested_by": requested_by})
    r.raise_for_status()
    data = r.json()
    _console.print(Panel(f"[green]Provision submitted[/green]\nWorkflow: {data['workflow_id']}"))


# ---------------------------------------------------------------------------
# Command: scan (Day 2)
# ---------------------------------------------------------------------------


async def cmd_scan(site_id: str, requested_by: str) -> None:
    async with _client() as c:
        r = await c.post(
            f"/v1/sites/{site_id}/scan",
            json={"requested_by": requested_by, "device_ids": []},
        )
    r.raise_for_status()
    data = r.json()
    _console.print(Panel(f"[green]Scan submitted[/green]\nWorkflow: {data['workflow_id']}"))


# ---------------------------------------------------------------------------
# Command: status
# ---------------------------------------------------------------------------


async def cmd_status(workflow_id: str) -> None:
    async with _client() as c:
        r = await c.get(f"/v1/workflows/{workflow_id}")
    r.raise_for_status()
    data = r.json()
    table = Table("Field", "Value")
    for k, v in data.items():
        table.add_row(k, str(v))
    _console.print(table)


# ---------------------------------------------------------------------------
# Command: approve
# ---------------------------------------------------------------------------


async def cmd_approve(workflow_id: str, decision: str) -> None:
    async with _client() as c:
        r = await c.post(f"/v1/workflows/{workflow_id}/approve", json={"decision": decision})
    r.raise_for_status()
    _console.print(f"[green]Decision '{decision}' sent to {workflow_id}[/green]")


# ---------------------------------------------------------------------------
# Command: list
# ---------------------------------------------------------------------------


async def cmd_list() -> None:
    async with _client() as c:
        r = await c.get("/v1/workflows")
    r.raise_for_status()
    items = r.json().get("items", [])
    table = Table("Workflow ID", "Status")
    for item in items:
        table.add_row(item["workflow_id"], item["status"])
    _console.print(table)


# ---------------------------------------------------------------------------
# Command: onboard (Day 0.5 — single site)
# ---------------------------------------------------------------------------


async def cmd_onboard(site_id: str) -> None:
    async with _client() as c:
        r = await c.post(f"/v1/onboarding/sites/{site_id}")
    r.raise_for_status()
    data = r.json()
    _console.print(
        Panel(
            f"[green]Onboarding submitted[/green]\nWorkflow: {data['workflow_id']}\nStatus: {data['status_url']}"
        )
    )


# ---------------------------------------------------------------------------
# Command: bulk-onboard (Day 0.5 — bulk sites)
# ---------------------------------------------------------------------------


async def cmd_bulk_onboard(
    site_ids: list[str],
    requested_by: str,
    sites_per_hour: int,
    max_concurrent: int,
) -> None:
    async with _client() as c:
        r = await c.post(
            "/v1/onboarding/bulk",
            json={
                "site_ids": site_ids,
                "requested_by": requested_by,
                "sites_per_hour": sites_per_hour,
                "max_concurrent": max_concurrent,
            },
        )
    r.raise_for_status()
    data = r.json()
    _console.print(
        Panel(
            f"[green]Bulk onboarding submitted[/green]\nWorkflow: {data['workflow_id']}\n"
            f"Sites: {len(site_ids)}\nStatus: {data['status_url']}"
        )
    )


# ---------------------------------------------------------------------------
# Command: onboard-status
# ---------------------------------------------------------------------------


async def cmd_onboard_status() -> None:
    async with _client() as c:
        r = await c.get("/v1/onboarding/status")
    r.raise_for_status()
    data = r.json()
    table = Table("State", "Count")
    for k, v in data.items():
        table.add_row(k, str(v))
    _console.print(table)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="network-ztp CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("bootstrap", help="Day 0: bootstrap a device from factory")
    p.add_argument("--device-id", required=True)
    p.add_argument("--requested-by", default="cli")

    p = sub.add_parser("start", help="Day 1: trigger full intent provisioning")
    p.add_argument("--device-id", required=True)
    p.add_argument("--requested-by", default="cli")

    p = sub.add_parser("scan", help="Day 2: compliance scan across a site")
    p.add_argument("--site-id", required=True)
    p.add_argument("--requested-by", default="cli")

    p = sub.add_parser("status", help="Query workflow status")
    p.add_argument("--workflow-id", required=True)

    p = sub.add_parser("approve", help="Send HITL approval signal")
    p.add_argument("--workflow-id", required=True)
    p.add_argument("--decision", required=True, choices=["approved", "rejected"])

    sub.add_parser("list", help="List recent workflow executions")

    p = sub.add_parser("onboard", help="Day 0.5: onboard a single site")
    p.add_argument("--site-id", required=True)

    p = sub.add_parser("bulk-onboard", help="Day 0.5: onboard multiple sites in bulk")
    p.add_argument(
        "--site-ids",
        required=True,
        help="Comma-separated site IDs",
    )
    p.add_argument("--requested-by", default="cli")
    p.add_argument("--sites-per-hour", type=int, default=50)
    p.add_argument("--max-concurrent", type=int, default=10)

    sub.add_parser("onboard-status", help="Check bulk onboarding progress")

    args = parser.parse_args()

    dispatch = {
        "bootstrap": lambda: cmd_bootstrap(args.device_id, args.requested_by),
        "start": lambda: cmd_start(args.device_id, args.requested_by),
        "scan": lambda: cmd_scan(args.site_id, args.requested_by),
        "status": lambda: cmd_status(args.workflow_id),
        "approve": lambda: cmd_approve(args.workflow_id, args.decision),
        "list": cmd_list,
        "onboard": lambda: cmd_onboard(args.site_id),
        "bulk-onboard": lambda: cmd_bulk_onboard(
            [s.strip() for s in args.site_ids.split(",") if s.strip()],
            args.requested_by,
            args.sites_per_hour,
            args.max_concurrent,
        ),
        "onboard-status": cmd_onboard_status,
    }
    asyncio.run(dispatch[args.command]())


if __name__ == "__main__":
    main()
