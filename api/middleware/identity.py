from __future__ import annotations

from collections.abc import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from api.config import Settings
from api.schemas.auth import UserContext, UserRole


class IdentityMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        super().__init__(app)
        self._settings = settings

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request.state.user = self._resolve_user(request)
        response: Response = await call_next(request)
        return response

    def _resolve_user(self, request: Request) -> UserContext:
        if self._settings.ztp_env == "production":
            return self._validate_jwt(request.headers.get("Authorization", ""))
        username = request.headers.get("X-Authenticated-User", self._settings.auth_dev_user)
        roles = self._settings.auth_dev_roles
        role = UserRole(roles[0]) if roles else UserRole.NOC_OPERATOR
        return UserContext(
            username=username,
            role=role,
            regions=self._settings.auth_dev_regions,
        )

    def _validate_jwt(self, authorization: str) -> UserContext:
        # Production: Apigee validates OAuth and signs a JWT before proxying here.
        # FastAPI verifies the signature and extracts claims. Not yet implemented —
        # the mTLS + network boundary enforces this for now.
        from fastapi import HTTPException

        if not authorization.startswith("Bearer "):
            raise HTTPException(401, detail="Missing Authorization header")
        raise NotImplementedError("JWT signature validation pending Apigee integration")
