"""
Temporal worker process for the ZTP task queue.

Hosts all three pipeline phases on a single task queue:
  - Day 0 BootstrapDeviceWorkflow + bootstrap activities
  - Day 1 ProvisionSiteWorkflow + provisioning activities
  - Day 2 ComplianceScanWorkflow  + compliance activities

All workflow and activity types are stateless — any worker replica can execute
any task.  Scale horizontally by running additional replicas pointing at the
same task queue; Temporal distributes work automatically.

Usage:
    uv run python temporal/worker.py
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys
from collections.abc import Callable
from typing import Any

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from temporal.activities.ansible_activities import push_config, render_config
from temporal.activities.bootstrap_activities import (
    publish_bootstrap_script,
    register_dhcp_reservation,
    render_bootstrap_script,
    wait_for_device_reachability,
)
from temporal.activities.nautobot_activities import (
    fetch_device_intent,
    fetch_site_devices,
    write_provisioning_status,
)
from temporal.activities.validation_activities import validate_device_state
from temporal.workflows.bootstrap_device import BootstrapDeviceWorkflow
from temporal.workflows.compliance_scan import ComplianceScanWorkflow
from temporal.workflows.provision_site import ProvisionSiteWorkflow

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "ztp-queue")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9091"))

_REGISTERED_WORKFLOWS = [
    BootstrapDeviceWorkflow,  # Day 0
    ProvisionSiteWorkflow,  # Day 1
    ComplianceScanWorkflow,  # Day 2
]

_REGISTERED_ACTIVITIES: list[Callable[..., Any]] = [
    # Day 0
    register_dhcp_reservation,
    render_bootstrap_script,
    publish_bootstrap_script,
    wait_for_device_reachability,
    # Day 1
    render_config,
    push_config,
    # Shared (Nautobot + validation)
    fetch_device_intent,
    fetch_site_devices,
    write_provisioning_status,
    validate_device_state,
]


async def run_worker() -> None:
    """Start the Temporal worker and block until shutdown."""
    runtime = Runtime(
        telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address=f"0.0.0.0:{METRICS_PORT}"))
    )

    client = await Client.connect(
        TEMPORAL_HOST,
        namespace=TEMPORAL_NAMESPACE,
        runtime=runtime,
    )

    worker = Worker(
        client,
        task_queue=TEMPORAL_TASK_QUEUE,
        workflows=_REGISTERED_WORKFLOWS,
        activities=_REGISTERED_ACTIVITIES,
    )

    logger.info("=" * 60)
    logger.info("network-ztp Worker starting")
    logger.info("  Temporal: %s  namespace=%s", TEMPORAL_HOST, TEMPORAL_NAMESPACE)
    logger.info("  Task queue: %s", TEMPORAL_TASK_QUEUE)
    logger.info("  Prometheus metrics: http://0.0.0.0:%d/metrics", METRICS_PORT)
    logger.info("  Workflows registered:")
    for wf in _REGISTERED_WORKFLOWS:
        logger.info("    - %s", wf.__name__)
    logger.info("  Activities registered:")
    for act in _REGISTERED_ACTIVITIES:
        logger.info("    - %s", act.__name__)
    logger.info("=" * 60)

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig: signal.Signals) -> None:
        logger.info("Received %s — initiating graceful shutdown", sig.name)
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _request_shutdown, sig)

    async with worker:
        logger.info("Worker is running. Press Ctrl-C to stop.")
        await shutdown_event.wait()
        logger.info("Shutting down worker — waiting for in-flight activities to complete...")

    logger.info("Worker stopped cleanly.")


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run_worker())
    sys.exit(0)


if __name__ == "__main__":
    main()
