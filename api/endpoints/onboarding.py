from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Depends
from temporalio.client import Client

from api.deps import get_temporal_client, require_role
from api.schemas.auth import UserContext, UserRole
from api.schemas.requests import AdjustRateRequest, BulkOnboardRequest
from api.schemas.responses import OnboardingStatus, WorkflowSubmitted

router = APIRouter(tags=["onboarding"])


@router.post("/onboarding/bulk", status_code=202)
async def start_bulk_onboarding(
    body: BulkOnboardRequest,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> WorkflowSubmitted:
    from api.config import get_settings
    from temporal.models import BulkOnboardingInput
    from temporal.workflows.bulk_onboarding import BulkOnboardingWorkflow

    s = get_settings()
    wf_id = f"onboard-bulk-{body.requested_by}-{int(time.time())}"
    handle = await temporal.start_workflow(
        BulkOnboardingWorkflow.run,
        BulkOnboardingInput(
            site_ids=body.site_ids,
            sites_per_hour=body.sites_per_hour,
            max_concurrent=body.max_concurrent,
            requested_by=body.requested_by,
            region=s.default_region,
        ),
        id=wf_id,
        task_queue="ztp-queue",
    )
    return WorkflowSubmitted(workflow_id=handle.id, status_url=f"/v1/workflows/{handle.id}")


@router.post("/onboarding/sites/{site_id}", status_code=202)
async def onboard_single_site(
    site_id: str,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> WorkflowSubmitted:
    from temporal.models import OnboardSiteInput
    from temporal.workflows.onboard_site import OnboardSiteWorkflow

    handle = await temporal.start_workflow(
        OnboardSiteWorkflow.run,
        OnboardSiteInput(site_id=site_id, device_id=site_id, requested_by="api"),
        id=f"onboard-site-{site_id}",
        task_queue="ztp-queue",
    )
    return WorkflowSubmitted(workflow_id=handle.id, status_url=f"/v1/workflows/{handle.id}")


@router.get("/onboarding/status")
async def get_onboarding_status(
    _auth: UserContext = require_role(  # noqa: B008
        UserRole.NOC_OPERATOR, UserRole.ENGINEER, UserRole.ADMIN
    ),
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> OnboardingStatus:
    try:
        handle = temporal.get_workflow_handle("onboard-bulk-latest")
        status: dict[str, Any] = await handle.query(BulkOnboardingWorkflow.get_status)
        return OnboardingStatus(
            pending=int(status.get("pending", 0)),
            discovering=int(status.get("in_flight", 0)),
            managed=int(status.get("managed", 0)),
            failed=int(status.get("failed", 0)),
        )
    except Exception:
        return OnboardingStatus()


@router.post("/onboarding/bulk/{workflow_id}/pause", status_code=200)
async def pause_bulk_onboarding(
    workflow_id: str,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> dict[str, str]:
    handle = temporal.get_workflow_handle(workflow_id)
    await handle.signal("pause")
    return {"workflow_id": workflow_id, "status": "paused"}


@router.post("/onboarding/bulk/{workflow_id}/resume", status_code=200)
async def resume_bulk_onboarding(
    workflow_id: str,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> dict[str, str]:
    handle = temporal.get_workflow_handle(workflow_id)
    await handle.signal("resume")
    return {"workflow_id": workflow_id, "status": "resumed"}


@router.post("/onboarding/bulk/{workflow_id}/adjust-rate", status_code=200)
async def adjust_bulk_onboarding_rate(
    workflow_id: str,
    body: AdjustRateRequest,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> dict[str, object]:
    handle = temporal.get_workflow_handle(workflow_id)
    await handle.signal("adjust_rate", args=[body.sites_per_hour, body.max_concurrent])
    return {
        "workflow_id": workflow_id,
        "sites_per_hour": body.sites_per_hour,
        "max_concurrent": body.max_concurrent,
    }


# needed for the query call type annotation
from temporal.workflows.bulk_onboarding import BulkOnboardingWorkflow  # noqa: E402
