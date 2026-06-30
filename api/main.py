from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from temporalio.client import Client

from api.config import Settings
from api.errors import register_error_handlers
from api.middleware.observability import ObservabilityMiddleware


def create_app(
    settings: Settings | None = None,
    temporal_client: Any = None,
) -> FastAPI:
    _settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        if temporal_client is None:
            app.state.temporal_client = await Client.connect(
                _settings.temporal_host,
                namespace=_settings.temporal_namespace,
            )
        app.state.settings = _settings
        yield

    app = FastAPI(
        title="network-ztp API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )

    # When a client is injected (e.g. in tests), httpx ASGITransport does not
    # fire the lifespan scope, so set state eagerly here.
    if temporal_client is not None:
        app.state.temporal_client = temporal_client
    app.state.settings = _settings

    app.add_middleware(ObservabilityMiddleware)

    from api.endpoints import health

    app.include_router(health.router)

    register_error_handlers(app)
    return app
