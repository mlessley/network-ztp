from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from api.deps import require_role
from api.schemas.auth import UserContext, UserRole

router = APIRouter()


@router.post("/devices/{device_id}/provision", status_code=202)
async def _stub_provision(
    device_id: str,
    _auth: UserContext = require_role(UserRole.ENGINEER, UserRole.ADMIN),  # noqa: B008
) -> dict[str, Any]:
    return {"status": "stub"}
