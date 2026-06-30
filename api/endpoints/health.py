from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from temporalio.client import Client

from api.deps import get_temporal_client

router = APIRouter(tags=["health"])


@router.get("/health")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(temporal: Client = Depends(get_temporal_client)) -> dict[str, str]:  # noqa: B008
    try:
        from temporalio.api.workflowservice.v1 import GetSystemInfoRequest

        await temporal.service_client.workflow_service.get_system_info(GetSystemInfoRequest())
        return {"status": "ready"}
    except Exception as exc:
        raise HTTPException(503, detail=f"Temporal unreachable: {exc}") from exc
