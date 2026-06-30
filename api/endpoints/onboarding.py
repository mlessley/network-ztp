from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from grpc import StatusCode as GrpcStatusCode
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError

from api.deps import get_temporal_client, require_role
from api.schemas.auth import UserContext, UserRole
from api.schemas.requests import BulkOnboardRequest
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
    wf_id = f"onboard-bulk-{body.requested_by}"
    try:
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
    except WorkflowAlreadyStartedError as exc:
        raise HTTPException(
            status_code=409, detail=f"Bulk onboarding already running for {body.requested_by}"
        ) from exc
    return WorkflowSubmitted(workflow_id=handle.id, status_url=f"/v1/workflows/{handle.id}")


@router.post("/onboarding/sites/{site_id}", status_code=202)
async def onboard_single_site(
    site_id: str,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> WorkflowSubmitted:
    from temporal.models import OnboardSiteInput
    from temporal.workflows.onboard_site import OnboardSiteWorkflow

    try:
        handle = await temporal.start_workflow(
            OnboardSiteWorkflow.run,
            OnboardSiteInput(site_id=site_id, device_id=site_id, requested_by="api"),
            id=f"onboard-site-{site_id}",
            task_queue="ztp-queue",
        )
    except WorkflowAlreadyStartedError as exc:
        raise HTTPException(
            status_code=409, detail=f"Onboarding already running for site {site_id}"
        ) from exc
    return WorkflowSubmitted(workflow_id=handle.id, status_url=f"/v1/workflows/{handle.id}")


@router.get("/onboarding/status")
async def get_onboarding_status(
    requested_by: str,
    _auth: UserContext = require_role(  # noqa: B008
        UserRole.NOC_OPERATOR, UserRole.ENGINEER, UserRole.ADMIN
    ),
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> OnboardingStatus:
    try:
        handle = temporal.get_workflow_handle(f"onboard-bulk-{requested_by}")
        status: dict[str, Any] = await handle.query(BulkOnboardingWorkflow.get_status)
        return OnboardingStatus(
            pending=int(status.get("pending", 0)),
            discovering=int(status.get("in_flight", 0)),
            managed=int(status.get("managed", 0)),
            failed=int(status.get("failed", 0)),
        )
    except RPCError as e:
        if e.status == GrpcStatusCode.NOT_FOUND:
            return OnboardingStatus()
        raise


# needed for the query call type annotation
from temporal.workflows.bulk_onboarding import BulkOnboardingWorkflow  # noqa: E402
