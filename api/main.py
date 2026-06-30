from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from temporalio.client import Client

from api.config import Settings
from api.errors import register_error_handlers
from api.middleware.identity import IdentityMiddleware
from api.middleware.observability import ObservabilityMiddleware


def _setup_tracing(service_name: str, otlp_endpoint: str) -> None:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint)))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()


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
    # IdentityMiddleware runs inner (added after ObservabilityMiddleware) so
    # request.state.user is available to all route handlers.
    app.add_middleware(IdentityMiddleware, settings=_settings)

    from api.endpoints import devices, health, onboarding, sites, webhooks, workflows

    app.include_router(health.router)
    app.include_router(devices.router, prefix="/v1")
    app.include_router(sites.router, prefix="/v1")
    app.include_router(webhooks.router, prefix="/v1")
    app.include_router(workflows.router, prefix="/v1")
    app.include_router(onboarding.router, prefix="/v1")

    register_error_handlers(app)

    if _settings.otlp_endpoint:
        _setup_tracing("ztp-api", _settings.otlp_endpoint)

    return app
