from __future__ import annotations

from fastapi import APIRouter, Depends
from temporalio.client import Client

from api.deps import get_temporal_client, require_role
from api.schemas.auth import UserContext, UserRole
from api.schemas.requests import ScanRequest
from api.schemas.responses import WorkflowSubmitted

router = APIRouter(tags=["sites"])


@router.post("/sites/{site_id}/scan", status_code=202)
async def scan_site(
    site_id: str,
    body: ScanRequest,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> WorkflowSubmitted:
    from temporal.models import ComplianceScanInput
    from temporal.workflows.compliance_scan import ComplianceScanWorkflow

    wf_id = f"day2-{site_id}"
    handle = await temporal.start_workflow(
        ComplianceScanWorkflow.run,
        ComplianceScanInput(
            site_id=site_id,
            requested_by=body.requested_by,
            device_ids=body.device_ids,
        ),
        id=wf_id,
        task_queue="ztp-queue",
    )
    return WorkflowSubmitted(workflow_id=handle.id, status_url=f"/v1/workflows/{handle.id}")
