from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal.models import (
        BulkOnboardingInput,
        BulkOnboardingResult,
        OnboardSiteInput,
    )
    from temporal.workflows.onboard_site import OnboardSiteWorkflow


@workflow.defn
class BulkOnboardingWorkflow:
    def __init__(self) -> None:
        self._paused = False
        self._sites_per_hour: int = 50
        self._max_concurrent: int = 10
        self._counts: dict[str, int] = {
            "pending": 0,
            "in_flight": 0,
            "managed": 0,
            "failed": 0,
        }

    @workflow.signal
    async def pause(self) -> None:
        self._paused = True

    @workflow.signal
    async def resume(self) -> None:
        self._paused = False

    @workflow.signal
    async def adjust_rate(self, sites_per_hour: int, max_concurrent: int) -> None:
        self._sites_per_hour = sites_per_hour
        self._max_concurrent = max_concurrent

    @workflow.query
    def get_status(self) -> dict:  # type: ignore[type-arg]
        return {
            "pending": self._counts["pending"],
            "in_flight": self._counts["in_flight"],
            "managed": self._counts["managed"],
            "failed": self._counts["failed"],
            "sites_per_hour": self._sites_per_hour,
        }

    @workflow.run
    async def run(self, inp: BulkOnboardingInput) -> BulkOnboardingResult:
        self._sites_per_hour = inp.sites_per_hour
        self._max_concurrent = inp.max_concurrent
        self._counts["pending"] = len(inp.site_ids)

        sleep_seconds = 3600.0 / max(self._sites_per_hour, 1)
        pending = list(inp.site_ids)
        in_flight: list[Any] = []

        while pending or in_flight:
            await workflow.wait_condition(lambda: not self._paused)

            while pending and len(in_flight) < self._max_concurrent:
                site_id = pending.pop(0)
                self._counts["pending"] -= 1
                self._counts["in_flight"] += 1
                child_handle = await workflow.start_child_workflow(
                    OnboardSiteWorkflow.run,
                    OnboardSiteInput(
                        site_id=site_id,
                        device_id=site_id,
                        requested_by=inp.requested_by,
                    ),
                    id=f"onboard-site-{site_id}",
                    task_queue="ztp-queue",
                )
                in_flight.append(child_handle)
                await workflow.sleep(timedelta(seconds=sleep_seconds))

            if in_flight:
                done, in_flight = await _poll_children(in_flight, self._counts)

        return BulkOnboardingResult(
            total_sites=len(inp.site_ids),
            managed_count=self._counts["managed"],
            failed_count=self._counts["failed"],
        )


async def _poll_children(handles: list[Any], counts: dict[str, int]) -> tuple[list[Any], list[Any]]:
    still_running: list[Any] = []
    done: list[Any] = []
    for h in handles:
        try:
            result = await h
            if result.success:
                counts["managed"] += 1
            else:
                counts["failed"] += 1
            counts["in_flight"] -= 1
            done.append(h)
        except Exception:
            counts["failed"] += 1
            counts["in_flight"] -= 1
            done.append(h)
    return done, still_running
