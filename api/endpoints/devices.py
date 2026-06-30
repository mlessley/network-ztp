from __future__ import annotations

from fastapi import APIRouter, Depends
from temporalio.client import Client

from api.deps import get_temporal_client, require_role
from api.schemas.auth import UserContext, UserRole
from api.schemas.requests import BootstrapRequest, ProvisionRequest
from api.schemas.responses import WorkflowSubmitted

router = APIRouter(tags=["devices"])


@router.post("/devices/{device_id}/bootstrap", status_code=202)
async def bootstrap_device(
    device_id: str,
    body: BootstrapRequest,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> WorkflowSubmitted:
    from temporal.models import BootstrapDeviceInput
    from temporal.workflows.bootstrap_device import BootstrapDeviceWorkflow

    wf_id = f"day0-{device_id}"
    handle = await temporal.start_workflow(
        BootstrapDeviceWorkflow.run,
        BootstrapDeviceInput(
            device_id=device_id,
            mac_address="",
            requested_by=body.requested_by,
        ),
        id=wf_id,
        task_queue="ztp-queue",
    )
    return WorkflowSubmitted(workflow_id=handle.id, status_url=f"/v1/workflows/{handle.id}")


@router.post("/devices/{device_id}/provision", status_code=202)
async def provision_device(
    device_id: str,
    body: ProvisionRequest,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> WorkflowSubmitted:
    from temporal.models import ProvisionSiteInput
    from temporal.workflows.provision_site import ProvisionSiteWorkflow

    wf_id = f"day1-{device_id}"
    handle = await temporal.start_workflow(
        ProvisionSiteWorkflow.run,
        ProvisionSiteInput(device_id=device_id, requested_by=body.requested_by),
        id=wf_id,
        task_queue="ztp-queue",
    )
    return WorkflowSubmitted(workflow_id=handle.id, status_url=f"/v1/workflows/{handle.id}")
