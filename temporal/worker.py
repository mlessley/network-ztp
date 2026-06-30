"""
Temporal worker process for the ZTP task queue.

Hosts all three pipeline phases on a single task queue:
  - Day 0  BootstrapDeviceWorkflow + bootstrap activities
  - Day 1  ProvisionSiteWorkflow   + provisioning activities
  - Day 2  ComplianceScanWorkflow  + compliance activities

All workflow and activity types are stateless — any worker replica can handle
any task.  Scale horizontally by running additional replicas pointed at the
same task queue; Temporal distributes work automatically.

Logging:
    Configured via ZTP_ENV environment variable:
      ZTP_ENV=development (default) → structlog ConsoleRenderer (human-readable)
      ZTP_ENV=production            → structlog JSONRenderer (machine-parseable)
    Log level is controlled by LOG_LEVEL (default: INFO).

Metrics:
    Prometheus metrics are exposed on METRICS_PORT (default: 9091).
    Scrape path: http://<worker-host>:9091/metrics

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

import structlog
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "ztp-queue")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9091"))
ZTP_ENV = os.getenv("ZTP_ENV", "development")

# ---------------------------------------------------------------------------
# Structlog configuration
# ---------------------------------------------------------------------------


def configure_logging() -> None:
    """
    Configure structlog for the worker process.

    Development: ConsoleRenderer with coloured level indicators.
    Production:  JSONRenderer for log aggregation (Loki, Splunk, CloudWatch).
    Level is read from LOG_LEVEL env var (default INFO).
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if ZTP_ENV == "production":
        processors: list[structlog.types.Processor] = [
            *shared_processors,
            structlog.processors.ExceptionRenderer(),
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = [
            *shared_processors,
            structlog.dev.ConsoleRenderer(colors=True),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Keep stdlib logging quiet so Temporal SDK logs don't double-print.
    logging.basicConfig(level=level, format="%(message)s")
    for noisy in ("temporalio", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Workflow and activity registration
# ---------------------------------------------------------------------------

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
    # Shared (Nautobot + validation — used by Day 1 and Day 2)
    fetch_device_intent,
    fetch_site_devices,
    write_provisioning_status,
    validate_device_state,
]


# ---------------------------------------------------------------------------
# Worker lifecycle
# ---------------------------------------------------------------------------


async def run_worker() -> None:
    """Start the Temporal worker and block until SIGINT/SIGTERM."""
    configure_logging()
    log = structlog.get_logger()

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

    log.info(
        "worker.starting",
        env=ZTP_ENV,
        temporal_host=TEMPORAL_HOST,
        namespace=TEMPORAL_NAMESPACE,
        task_queue=TEMPORAL_TASK_QUEUE,
        metrics_port=METRICS_PORT,
        workflows=[wf.__name__ for wf in _REGISTERED_WORKFLOWS],
        activities=[fn.__name__ for fn in _REGISTERED_ACTIVITIES],
    )

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig: signal.Signals) -> None:
        log.info("worker.shutdown_requested", signal=sig.name)
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _request_shutdown, sig)

    async with worker:
        log.info("worker.ready", task_queue=TEMPORAL_TASK_QUEUE)
        await shutdown_event.wait()
        log.info("worker.draining", note="waiting for in-flight activities to complete")

    log.info("worker.stopped")


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run_worker())
    sys.exit(0)


if __name__ == "__main__":
    main()
