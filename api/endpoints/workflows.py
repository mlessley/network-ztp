from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from temporalio.client import Client

from api.deps import get_temporal_client, require_role
from api.schemas.auth import UserContext, UserRole
from api.schemas.requests import ApproveRequest
from api.schemas.responses import WorkflowStatus

router = APIRouter(tags=["workflows"])


@router.get("/workflows/{workflow_id}")
async def get_workflow_status(
    workflow_id: str,
    _auth: UserContext = require_role(UserRole.NOC_OPERATOR, UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> WorkflowStatus:
    try:
        handle = temporal.get_workflow_handle(workflow_id)
        desc = await handle.describe()
        return WorkflowStatus(
            workflow_id=workflow_id,
            status=str(desc.status),
            started_at=desc.start_time,
            completed_at=desc.close_time,
        )
    except Exception as exc:
        raise HTTPException(404, detail=f"Workflow not found: {exc}") from exc


@router.post("/workflows/{workflow_id}/approve", status_code=200)
async def approve_workflow(
    workflow_id: str,
    body: ApproveRequest,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> dict[str, object]:
    handle = temporal.get_workflow_handle(workflow_id)
    await handle.signal("approve_escalation", body.decision)
    return {"workflow_id": workflow_id, "decision": body.decision}


@router.get("/workflows")
async def list_workflows(
    _auth: UserContext = require_role(UserRole.NOC_OPERATOR, UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> dict[str, object]:
    results = []
    async for wf in temporal.list_workflows(query="", page_size=50):
        results.append({"workflow_id": wf.id, "status": str(wf.status)})
    return {"items": results}
