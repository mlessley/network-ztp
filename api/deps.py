from __future__ import annotations

from typing import Any

from fastapi import Depends, HTTPException, Request
from temporalio.client import Client

from api.schemas.auth import UserContext, UserRole


def get_temporal_client(request: Request) -> Client:
    return request.app.state.temporal_client  # type: ignore[no-any-return]


def get_current_user(request: Request) -> UserContext:
    return request.state.user  # type: ignore[no-any-return]


def require_role(*roles: UserRole) -> Any:
    def _check(user: UserContext = Depends(get_current_user)) -> UserContext:  # noqa: B008
        if user.role not in roles:
            raise HTTPException(403, detail="Insufficient permissions")
        return user

    return Depends(_check)


async def require_region_access(
    resource_id: str,
    resource_type: str = "device",
    user: UserContext = Depends(get_current_user),  # noqa: B008
) -> None:
    from api.config import get_settings

    settings = get_settings()
    if user.role == UserRole.ADMIN:
        return
    # Mock mode: all resources resolve to default_region
    # Live mode: query Nautobot for device/site region
    resource_region = settings.default_region
    if resource_region not in user.regions:
        raise HTTPException(
            403,
            detail=f"Resource {resource_id} is in region {resource_region}; "
            f"your access covers {user.regions}",
        )
