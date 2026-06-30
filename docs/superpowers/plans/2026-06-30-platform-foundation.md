# Platform Foundation & Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a versioned FastAPI layer with RBAC auth, OTel traces, Prometheus metrics, Loki log aggregation, a bulk site-onboarding workflow, and a one-command docker-compose local dev stack to the existing network-ztp Temporal pipeline.

**Architecture:** FastAPI sits in front of Temporal — all workflow submissions go through the API, never directly from the CLI. Apigee handles edge auth in production; a dev-mode identity stub runs locally. OTel spans flow from every HTTP request through every Temporal activity into Tempo; trace_id is injected into every structlog line so Grafana can navigate Prometheus alert → log → trace in one click.

**Tech Stack:** FastAPI, uvicorn, pydantic-settings, httpx, respx, opentelemetry-sdk + instrumentation packages, temporalio, structlog, prometheus-client, docker compose v2, Grafana, Tempo, Loki, promtail.

**Spec:** `docs/superpowers/specs/2026-06-30-platform-foundation-design.md`

## Global Constraints

- Package manager `uv` only — never pip, pip3, poetry, or bare python.
- `asyncio_mode = "auto"` in pyproject.toml — never add `@pytest.mark.asyncio`.
- Pydantic v2 — use `model_validate` / `model_dump`, never `parse_obj` / `.dict()`.
- Workflow files (`temporal/workflows/`) — never `datetime.now()`, `asyncio.sleep()`, `os.environ`, or top-level imports of activity modules. All must go inside `workflow.unsafe.imports_passed_through()`.
- Roll-forward only — no `except` blocks that push configs to devices or restore prior state.
- No comments unless the WHY is non-obvious to a future reader.
- After every task: `uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v`
- Region sample values throughout are `SOUTH`, `WEST`, `EAST`, `NORTH` (US domestic).

---

## File Map

**New files:**
```
api/__init__.py
api/main.py                         FastAPI app factory + lifespan
api/config.py                       pydantic-settings Settings model
api/deps.py                         FastAPI dependency providers
api/errors.py                       RFC 7807 error handler
api/schemas/__init__.py
api/schemas/auth.py                 UserContext dataclass + UserRole enum
api/schemas/requests.py             HTTP-level request models
api/schemas/responses.py            HTTP-level response models
api/middleware/__init__.py
api/middleware/identity.py          Apigee JWT stub / dev header passthrough
api/middleware/observability.py     OTel span + structlog context binder
api/endpoints/__init__.py
api/endpoints/health.py             GET /health  GET /health/ready
api/endpoints/devices.py            POST /v1/devices/{id}/bootstrap|provision
api/endpoints/sites.py              POST /v1/sites/{id}/scan
api/endpoints/webhooks.py           POST /v1/webhooks/nautobot
api/endpoints/workflows.py          GET|POST /v1/workflows/...
api/endpoints/onboarding.py         POST /v1/onboarding/...  GET /v1/onboarding/status
tests/api/__init__.py
tests/api/conftest.py               shared fixtures: app, mock_temporal_client
tests/api/test_devices.py
tests/api/test_webhooks.py
tests/api/test_workflows.py
tests/api/test_onboarding.py
temporal/config.py                  pydantic-settings for worker
temporal/metrics.py                 Prometheus counter/gauge/histogram definitions
temporal/activities/onboarding_activities.py
temporal/workflows/onboard_site.py
temporal/workflows/bulk_onboarding.py
tests/test_onboarding_activities.py
tests/test_onboarding_workflow.py
docker/Dockerfile.api
docker/Dockerfile.worker
docker/entrypoint.sh
docker-compose.yml
docker-compose.override.yml
Makefile
.env.example
config/observability/prometheus/prometheus.yml
config/observability/prometheus/rules/recording.yml
config/observability/prometheus/rules/alerting.yml
config/observability/grafana/provisioning/datasources.yaml
config/observability/grafana/provisioning/dashboards.yaml
config/observability/grafana/dashboards/worker-overview.json
config/observability/grafana/dashboards/pipeline-latency.json
config/observability/grafana/dashboards/compliance-health.json
config/observability/grafana/dashboards/onboarding-progress.json
config/observability/promtail/config.yml
```

**Modified files:**
```
pyproject.toml                      remove httpx2; add httpx, fastapi, uvicorn, OTel, pydantic-settings
temporal/models.py                  add ConfigChange, RemediationPlan, onboarding Status values,
                                    OnboardSiteInput/Result, BulkOnboardingInput/Result
temporal/activities/nautobot_activities.py   import httpx (not httpx2); add OTel spans
temporal/activities/bootstrap_activities.py  import httpx (not httpx2); add OTel spans
temporal/activities/ansible_activities.py    add OTel spans
temporal/activities/validation_activities.py add OTel spans
temporal/worker.py                  use temporal/config.py; register new workflows + activities
temporal/run_workflow.py            rewrite as httpx API client to FastAPI
tests/test_activities.py            update live-path tests to use respx (not unittest.mock.patch)
```

---

## Task 1: Dependency Migration + New Packages

**Files:**
- Modify: `pyproject.toml`

**Interfaces:**
- Produces: `httpx` available in all activity files; `fastapi`, `uvicorn`, OTel packages, `pydantic-settings` available for Tasks 2+.

- [ ] **Step 1: Update pyproject.toml**

Replace the `httpx2` line and add new packages:

```toml
dependencies = [
    "ansible-runner>=2.4.3",
    "fastapi>=0.115.0",
    "httpx>=0.28.0",
    "jinja2>=3.1.6",
    "napalm>=5.1.0",
    "netmiko>=4.7.0",
    "opentelemetry-api>=1.29.0",
    "opentelemetry-exporter-otlp-proto-grpc>=1.29.0",
    "opentelemetry-instrumentation-fastapi>=0.50b0",
    "opentelemetry-instrumentation-httpx>=0.50b0",
    "opentelemetry-sdk>=1.29.0",
    "pydantic>=2.13.4",
    "pydantic-settings>=2.7.0",
    "pynautobot>=3.1.0",
    "python-dotenv>=1.2.2",
    "python-multipart>=0.0.20",
    "rich>=15.0.0",
    "structlog>=26.1.0",
    "temporalio>=1.29.0",
    "tenacity>=9.1.4",
    "uvicorn[standard]>=0.34.0",
]

[dependency-groups]
dev = [
    "freezegun>=1.5.5",
    "httpx>=0.28.0",
    "mypy>=2.1.0",
    "pytest>=9.1.1",
    "pytest-asyncio>=1.4.0",
    "respx>=0.23.1",
    "ruff>=0.15.20",
    "temporalio[testing]>=1.29.0",
]
```

Also update `[tool.ruff.lint.isort]` to include `api` as first-party:
```toml
[tool.ruff.lint.isort]
known-first-party = ["temporal", "api"]
```

And add `api` to mypy paths:
```toml
[tool.mypy]
strict = true
python_version = "3.11"
ignore_missing_imports = true
warn_return_any = true
warn_unused_configs = true
```

- [ ] **Step 2: Install updated dependencies**

```bash
uv sync
```

Expected: lock file updated, no errors.

- [ ] **Step 3: Replace httpx2 imports in activity files**

In `temporal/activities/nautobot_activities.py`, find:
```python
import httpx2 as httpx
```
Replace with:
```python
import httpx
```

In `temporal/activities/bootstrap_activities.py`, make the same replacement if present (check with `grep -r "httpx2" temporal/`).

- [ ] **Step 4: Update live-path tests to use respx**

In `tests/test_activities.py`, find the `TestFetchDeviceIntentLive` class. It currently uses `unittest.mock.patch("httpx2.AsyncClient", ...)`. Replace the entire class with respx:

```python
import respx
import httpx as _httpx

class TestFetchDeviceIntentLive:
    @respx.mock
    async def test_live_path_calls_nautobot_graphql(self, monkeypatch):
        monkeypatch.setattr(nautobot_activities, "_USE_MOCK", False)
        payload = {
            "data": {
                "devices": [nautobot_activities._mock_graphql_response("DEV001")["data"]["devices"][0]]
            }
        }
        respx.post("http://localhost:8080/graphql/").mock(
            return_value=_httpx.Response(200, json=payload)
        )
        result = await fetch_device_intent("DEV001")
        assert result.device_id == "DEV001"
        assert result.hostname != ""
```

- [ ] **Step 5: Run full check**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ && uv run pytest tests/ -v
```

Expected: 66 tests pass (no new tests in this task).

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock temporal/activities/nautobot_activities.py \
    temporal/activities/bootstrap_activities.py tests/test_activities.py
git commit -m "chore: migrate httpx2→httpx, add fastapi/uvicorn/OTel/pydantic-settings"
```

---

## Task 2: Config Foundation

**Files:**
- Create: `temporal/config.py`
- Create: `api/config.py`

**Interfaces:**
- Produces: `Settings` class importable from both `temporal.config` and `api.config`. `get_settings()` cached factory in each. `temporal/worker.py` will import from `temporal.config` in Task 9.

- [ ] **Step 1: Write failing test for temporal config**

Create `tests/test_config.py`:

```python
import pytest
from pydantic import ValidationError


class TestTemporalConfig:
    def test_defaults_are_valid(self):
        from temporal.config import Settings
        s = Settings()
        assert s.temporal_host == "localhost:7233"
        assert s.ztp_use_mock is True

    def test_live_mode_requires_nautobot_token(self):
        from temporal.config import Settings
        with pytest.raises(ValidationError, match="NAUTOBOT_TOKEN"):
            Settings(ztp_use_mock=False, nautobot_token="", nautobot_webhook_secret="")

    def test_live_mode_passes_with_credentials(self):
        from temporal.config import Settings
        s = Settings(
            ztp_use_mock=False,
            nautobot_token="tok",
            nautobot_webhook_secret="sec",
        )
        assert s.ztp_use_mock is False
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
uv run pytest tests/test_config.py -v
```

Expected: `ModuleNotFoundError: No module named 'temporal.config'`

- [ ] **Step 3: Create temporal/config.py**

```python
from __future__ import annotations

from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "ztp-queue"

    nautobot_url: str = "http://localhost:8080"
    nautobot_token: str = ""
    nautobot_webhook_secret: str = ""

    otlp_endpoint: str = ""
    metrics_port: int = 9091
    ztp_env: Literal["development", "production"] = "development"
    log_level: str = "INFO"

    ztp_use_mock: bool = True

    onboarding_sites_per_hour: int = 50
    onboarding_max_concurrent: int = 10
    default_region: str = "SOUTH"

    model_config = SettingsConfigDict(
        env_file=".env", case_sensitive=False, extra="ignore"
    )

    @model_validator(mode="after")
    def require_live_credentials(self) -> Settings:
        if not self.ztp_use_mock:
            missing = [
                f for f in ("nautobot_token", "nautobot_webhook_secret")
                if not getattr(self, f)
            ]
            if missing:
                raise ValueError(
                    f"Live mode requires: {', '.join(m.upper() for m in missing)}"
                )
        return self


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
```

- [ ] **Step 4: Create api/config.py**

```python
from __future__ import annotations

from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"

    api_port: int = 8000

    nautobot_url: str = "http://localhost:8080"
    nautobot_token: str = ""
    nautobot_webhook_secret: str = ""

    auth_dev_user: str = "dev-user"
    auth_dev_roles: list[str] = ["engineer"]
    auth_dev_regions: list[str] = ["SOUTH"]

    otlp_endpoint: str = ""
    ztp_env: Literal["development", "production"] = "development"
    log_level: str = "INFO"

    ztp_use_mock: bool = True
    default_region: str = "SOUTH"

    model_config = SettingsConfigDict(
        env_file=".env", case_sensitive=False, extra="ignore"
    )

    @model_validator(mode="after")
    def require_live_credentials(self) -> Settings:
        if not self.ztp_use_mock:
            missing = [
                f for f in ("nautobot_token", "nautobot_webhook_secret")
                if not getattr(self, f)
            ]
            if missing:
                raise ValueError(
                    f"Live mode requires: {', '.join(m.upper() for m in missing)}"
                )
        return self


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
```

- [ ] **Step 5: Create empty api/__init__.py**

```python
```

- [ ] **Step 6: Run tests**

```bash
uv run pytest tests/test_config.py -v
```

Expected: 3 tests pass.

- [ ] **Step 7: Run full check**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v
```

Expected: all 69 tests pass.

- [ ] **Step 8: Commit**

```bash
git add api/__init__.py api/config.py temporal/config.py tests/test_config.py pyproject.toml
git commit -m "feat: add pydantic-settings config for api and worker"
```

---

## Task 3: models.py Additions

**Files:**
- Modify: `temporal/models.py`

**Interfaces:**
- Produces: `ConfigChange`, `RemediationPlan`, `OnboardSiteInput`, `OnboardSiteResult`, `BulkOnboardingInput`, `BulkOnboardingResult` importable from `temporal.models`. New `ProvisioningStatus` values: `ONBOARD_PENDING`, `ONBOARD_DISCOVERING`, `ONBOARD_DISCOVERED`, `ONBOARD_RECONCILING`, `ONBOARD_MANAGED`.

- [ ] **Step 1: Write failing tests**

Add to `tests/test_models.py` (or create it):

```python
class TestOnboardingModels:
    def test_remediation_plan_round_trip(self):
        from datetime import UTC, datetime
        from temporal.models import ConfigChange, RemediationPlan
        plan = RemediationPlan(
            site_id="SITE-001",
            device_id="DEV001",
            snapshot_id="snap-abc",
            changes=[
                ConfigChange(
                    section="bgp",
                    description="Add peer 10.0.0.1",
                    current="",
                    intended="neighbor 10.0.0.1 remote-as 64512",
                )
            ],
            estimated_impact="high",
            created_at=datetime.now(UTC),
        )
        assert RemediationPlan.model_validate(plan.model_dump()) == plan

    def test_onboard_site_input_round_trip(self):
        from temporal.models import OnboardSiteInput
        inp = OnboardSiteInput(site_id="SITE-001", device_id="DEV001", requested_by="eng")
        assert OnboardSiteInput.model_validate(inp.model_dump()) == inp

    def test_onboarding_status_values_are_stable(self):
        from temporal.models import ProvisioningStatus
        assert ProvisioningStatus.ONBOARD_PENDING == "ONBOARD_PENDING"
        assert ProvisioningStatus.ONBOARD_MANAGED == "ONBOARD_MANAGED"
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_models.py -v -k "TestOnboarding"
```

Expected: `ImportError` — `ConfigChange` does not exist.

- [ ] **Step 3: Add new ProvisioningStatus values**

In `temporal/models.py`, after the `COMPLIANCE_DRIFTED` line (keep this order — Temporal stores string values, never reorder existing entries):

```python
    # Day 0.5 — site onboarding
    ONBOARD_PENDING = "ONBOARD_PENDING"
    ONBOARD_DISCOVERING = "ONBOARD_DISCOVERING"
    ONBOARD_DISCOVERED = "ONBOARD_DISCOVERED"
    ONBOARD_RECONCILING = "ONBOARD_RECONCILING"
    ONBOARD_MANAGED = "ONBOARD_MANAGED"
```

- [ ] **Step 4: Add onboarding models**

Append to the bottom of `temporal/models.py`:

```python
# ---------------------------------------------------------------------------
# Day 0.5 models — site onboarding
# ---------------------------------------------------------------------------


class ConfigChange(BaseModel):
    section: str
    description: str
    current: str
    intended: str


class RemediationPlan(BaseModel):
    site_id: str
    device_id: str
    snapshot_id: str
    changes: list[ConfigChange]
    estimated_impact: Literal["low", "medium", "high"]
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))


class OnboardSiteInput(BaseModel):
    site_id: str
    device_id: str
    requested_by: str


class OnboardSiteResult(BaseModel):
    site_id: str
    device_id: str
    success: bool
    workflow_id: str
    completed_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    failure_reason: str = Field(default="")


class BulkOnboardingInput(BaseModel):
    site_ids: list[str]
    sites_per_hour: int = Field(default=50, ge=1, le=500)
    max_concurrent: int = Field(default=10, ge=1, le=50)
    requested_by: str
    region: str = Field(default="SOUTH")


class BulkOnboardingResult(BaseModel):
    total_sites: int
    managed_count: int
    failed_count: int
    completed_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
```

Add `Literal` to the imports at the top of the file:

```python
from typing import Literal
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_models.py -v
```

Expected: all model tests pass.

- [ ] **Step 6: Full check and commit**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v
git add temporal/models.py tests/test_models.py
git commit -m "feat: add onboarding models and ProvisioningStatus states"
```

---

## Task 4: FastAPI Skeleton + Health Endpoint

**Files:**
- Create: `api/main.py`, `api/deps.py`, `api/errors.py`
- Create: `api/middleware/__init__.py`, `api/middleware/observability.py`
- Create: `api/endpoints/__init__.py`, `api/endpoints/health.py`
- Create: `api/schemas/__init__.py`
- Create: `tests/api/__init__.py`, `tests/api/conftest.py`, `tests/api/test_health.py`

**Interfaces:**
- Produces: `create_app(settings, temporal_client) -> FastAPI`. `get_temporal_client(request) -> Client`. All subsequent API tasks import from `api.main.create_app` and `api.deps`.

- [ ] **Step 1: Write failing health test**

Create `tests/api/test_health.py`:

```python
import pytest
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, MagicMock


async def test_liveness(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_readiness_ok(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/health/ready")
    assert r.status_code == 200


async def test_readiness_temporal_down(settings):
    from api.main import create_app
    bad_client = MagicMock()
    bad_client.service_client.workflow_service.get_system_info = AsyncMock(
        side_effect=RuntimeError("connection refused")
    )
    app = create_app(settings=settings, temporal_client=bad_client)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/health/ready")
    assert r.status_code == 503

async def test_unknown_route_returns_rfc7807(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/v1/does-not-exist")
    assert r.status_code == 404
    body = r.json()
    assert "type" in body
    assert "title" in body
    assert body["status"] == 404
```

- [ ] **Step 2: Create tests/api/conftest.py**

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from api.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        ztp_use_mock=True,
        ztp_env="development",
        auth_dev_user="test-engineer",
        auth_dev_roles=["engineer"],
        auth_dev_regions=["SOUTH"],
    )


@pytest.fixture
def mock_temporal_client() -> MagicMock:
    handle = MagicMock()
    handle.id = "test-workflow-id-001"
    handle.query = AsyncMock(return_value="COMPLETED")
    handle.signal = AsyncMock()

    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=handle)
    client.get_workflow_handle = MagicMock(return_value=handle)
    client.service_client.workflow_service.get_system_info = AsyncMock(return_value=MagicMock())
    client.list_workflows = MagicMock(return_value=_async_iter([]))
    return client


def _async_iter(items):
    async def _gen():
        for item in items:
            yield item
    return _gen()


@pytest.fixture
def app(settings, mock_temporal_client):
    from api.main import create_app
    return create_app(settings=settings, temporal_client=mock_temporal_client)
```

- [ ] **Step 3: Create api/errors.py**

```python
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


def _problem(status: int, title: str, detail: str, instance: str, trace_id: str = "") -> dict:
    body: dict = {
        "type": f"https://network-ztp/errors/{title.lower().replace(' ', '-')}",
        "title": title,
        "status": status,
        "detail": detail,
        "instance": instance,
    }
    if trace_id:
        body["trace_id"] = trace_id
    return body


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        trace_id = getattr(request.state, "trace_id", "")
        titles = {
            400: "Bad Request", 401: "Unauthorized", 403: "Forbidden",
            404: "Not Found", 409: "Conflict", 422: "Unprocessable Entity",
            503: "Service Unavailable",
        }
        title = titles.get(exc.status_code, "Error")
        return JSONResponse(
            status_code=exc.status_code,
            content=_problem(
                exc.status_code, title,
                str(exc.detail), str(request.url.path), trace_id,
            ),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        trace_id = getattr(request.state, "trace_id", "")
        return JSONResponse(
            status_code=500,
            content=_problem(500, "Internal Server Error", str(exc), str(request.url.path), trace_id),
        )
```

- [ ] **Step 4: Create api/middleware/__init__.py and api/middleware/observability.py**

`api/middleware/__init__.py`: empty file.

`api/middleware/observability.py`:

```python
from __future__ import annotations

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class ObservabilityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        import uuid
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        request.state.request_id = request_id

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            path=request.url.path,
            method=request.method,
        )

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response
```

- [ ] **Step 5: Create api/deps.py**

```python
from __future__ import annotations

from fastapi import HTTPException, Request
from temporalio.client import Client

from api.schemas.auth import UserContext, UserRole


def get_temporal_client(request: Request) -> Client:
    return request.app.state.temporal_client


def get_current_user(request: Request) -> UserContext:
    return request.state.user


def require_role(*roles: UserRole):
    from fastapi import Depends

    def _check(user: UserContext = Depends(get_current_user)) -> UserContext:
        if user.role not in roles:
            raise HTTPException(403, detail="Insufficient permissions")
        return user

    return Depends(_check)
```

- [ ] **Step 6: Create api/schemas/__init__.py, api/schemas/auth.py**

`api/schemas/__init__.py`: empty file.

`api/schemas/auth.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class UserRole(StrEnum):
    ADMIN = "admin"
    ENGINEER = "engineer"
    NOC_OPERATOR = "noc"
    SERVICE_ACCOUNT = "service"


@dataclass
class UserContext:
    username: str
    role: UserRole
    regions: list[str] = field(default_factory=list)
```

- [ ] **Step 7: Create api/endpoints/__init__.py and api/endpoints/health.py**

`api/endpoints/__init__.py`: empty file.

`api/endpoints/health.py`:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from temporalio.client import Client

from api.deps import get_temporal_client

router = APIRouter(tags=["health"])


@router.get("/health")
async def liveness() -> dict:
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(temporal: Client = Depends(get_temporal_client)) -> dict:
    try:
        from temporalio.api.workflowservice.v1 import GetSystemInfoRequest
        await temporal.service_client.workflow_service.get_system_info(GetSystemInfoRequest())
        return {"status": "ready"}
    except Exception as exc:
        raise HTTPException(503, detail=f"Temporal unreachable: {exc}")
```

- [ ] **Step 8: Create api/main.py**

```python
from __future__ import annotations

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
    async def lifespan(app: FastAPI):
        if temporal_client is not None:
            app.state.temporal_client = temporal_client
        else:
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

    app.add_middleware(ObservabilityMiddleware)

    from api.endpoints import health
    app.include_router(health.router)

    register_error_handlers(app)
    return app
```

- [ ] **Step 9: Run tests**

```bash
uv run pytest tests/api/test_health.py -v
```

Expected: 4 tests pass.

- [ ] **Step 10: Full check and commit**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v
git add api/ tests/api/
git commit -m "feat: add FastAPI skeleton with health endpoints and RFC 7807 errors"
```

---

## Task 5: Identity Middleware + Auth

**Files:**
- Create: `api/middleware/identity.py`
- Modify: `api/main.py` (register middleware + identity)
- Create: `tests/api/test_auth.py`

**Interfaces:**
- Consumes: `UserContext`, `UserRole` from `api.schemas.auth`. `Settings.ztp_env`, `Settings.auth_dev_*` from `api.config`.
- Produces: `request.state.user: UserContext` available in all route handlers. `require_role()` enforces minimum role. `require_region_access()` checks device/site region against user.regions.

- [ ] **Step 1: Write failing auth tests**

Create `tests/api/test_auth.py`:

```python
import pytest
from httpx import ASGITransport, AsyncClient
from api.config import Settings
from api.main import create_app
from unittest.mock import MagicMock, AsyncMock


@pytest.fixture
def noc_app(mock_temporal_client):
    s = Settings(
        ztp_use_mock=True,
        auth_dev_user="noc-user",
        auth_dev_roles=["noc"],
        auth_dev_regions=["SOUTH"],
    )
    return create_app(settings=s, temporal_client=mock_temporal_client)


async def test_dev_user_injected_from_header(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/health/me", headers={"X-Authenticated-User": "alice@test.com"})
    assert r.status_code == 200
    assert r.json()["username"] == "alice@test.com"


async def test_dev_user_falls_back_to_default(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/health/me")
    assert r.status_code == 200
    assert r.json()["username"] == "test-engineer"


async def test_noc_cannot_access_engineer_route(noc_app):
    async with AsyncClient(transport=ASGITransport(app=noc_app), base_url="http://test") as ac:
        r = await ac.post(
            "/v1/devices/DEV001/provision",
            json={"requested_by": "noc-user"},
        )
    assert r.status_code == 403
```

- [ ] **Step 2: Create api/middleware/identity.py**

```python
from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from api.config import Settings
from api.schemas.auth import UserContext, UserRole


class IdentityMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, settings: Settings) -> None:
        super().__init__(app)
        self._settings = settings

    async def dispatch(self, request: Request, call_next) -> Response:
        request.state.user = self._resolve_user(request)
        return await call_next(request)

    def _resolve_user(self, request: Request) -> UserContext:
        if self._settings.ztp_env == "production":
            return self._validate_jwt(request.headers.get("Authorization", ""))
        username = request.headers.get(
            "X-Authenticated-User", self._settings.auth_dev_user
        )
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
```

- [ ] **Step 3: Add /health/me debug endpoint and wire middleware in api/main.py**

Add to `api/endpoints/health.py`:

```python
from fastapi import Depends
from api.deps import get_current_user
from api.schemas.auth import UserContext

@router.get("/health/me")
async def whoami(user: UserContext = Depends(get_current_user)) -> dict:
    return {"username": user.username, "role": user.role, "regions": user.regions}
```

Update `api/main.py` — add identity middleware registration (must be added AFTER ObservabilityMiddleware so it runs inner):

```python
from api.middleware.identity import IdentityMiddleware

# Inside create_app(), after ObservabilityMiddleware:
app.add_middleware(IdentityMiddleware, settings=_settings)
```

Also register the device and site routers now (they'll be implemented in Task 6 but we need them so 403 tests work):

```python
from api.endpoints import health, devices, sites, webhooks, workflows, onboarding

app.include_router(health.router)
app.include_router(devices.router, prefix="/v1")
app.include_router(sites.router, prefix="/v1")
app.include_router(webhooks.router, prefix="/v1")
app.include_router(workflows.router, prefix="/v1")
app.include_router(onboarding.router, prefix="/v1")
```

Create stub files for the endpoints not yet implemented so the imports don't fail. Each stub:

`api/endpoints/devices.py`:
```python
from fastapi import APIRouter
router = APIRouter()
```

`api/endpoints/sites.py`, `api/endpoints/webhooks.py`, `api/endpoints/workflows.py`, `api/endpoints/onboarding.py`: same stub content.

- [ ] **Step 4: Add require_region_access to api/deps.py**

```python
async def require_region_access(
    resource_id: str,
    resource_type: str = "device",
    user: UserContext = Depends(get_current_user),
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
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/api/test_auth.py tests/api/test_health.py -v
```

Expected: all 7 tests pass.

- [ ] **Step 6: Full check and commit**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v
git add api/ tests/api/test_auth.py
git commit -m "feat: add identity middleware, UserContext, require_role dep"
```

---

## Task 6: Device, Site, Webhook + Workflow Endpoints

**Files:**
- Create: `api/schemas/requests.py`, `api/schemas/responses.py`
- Modify: `api/endpoints/devices.py`, `api/endpoints/sites.py`, `api/endpoints/webhooks.py`, `api/endpoints/workflows.py`
- Create: `tests/api/test_devices.py`, `tests/api/test_webhooks.py`, `tests/api/test_workflows.py`

**Interfaces:**
- Consumes: `require_role`, `require_region_access`, `get_temporal_client` from `api.deps`. `BootstrapDeviceWorkflow`, `ProvisionSiteWorkflow`, `ComplianceScanWorkflow` workflow names.
- Produces: REST endpoints per spec section 4.1. `WorkflowSubmitted`, `WorkflowStatus` response models.

- [ ] **Step 1: Create api/schemas/requests.py**

```python
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class BootstrapRequest(BaseModel):
    requested_by: str


class ProvisionRequest(BaseModel):
    requested_by: str


class ScanRequest(BaseModel):
    requested_by: str
    device_ids: list[str] = []


class ApproveRequest(BaseModel):
    decision: Literal["approved", "rejected"]
    note: str = ""
```

- [ ] **Step 2: Create api/schemas/responses.py**

```python
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class WorkflowSubmitted(BaseModel):
    workflow_id: str
    status_url: str


class WorkflowStatus(BaseModel):
    workflow_id: str
    status: str
    device_id: str | None = None
    site_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    failure_reason: str | None = None
    trace_id: str | None = None


class OnboardingStatus(BaseModel):
    pending: int = 0
    discovering: int = 0
    discovered: int = 0
    reconciling: int = 0
    managed: int = 0
    failed: int = 0
    sites_per_hour_actual: float = 0.0
    estimated_completion: datetime | None = None
```

- [ ] **Step 3: Write failing device tests**

Create `tests/api/test_devices.py`:

```python
import pytest
from httpx import ASGITransport, AsyncClient


async def test_provision_device_returns_202(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post(
            "/v1/devices/DEV001/provision",
            json={"requested_by": "test-engineer"},
        )
    assert r.status_code == 202
    body = r.json()
    assert "workflow_id" in body
    assert body["status_url"] == "/v1/workflows/test-workflow-id-001"


async def test_bootstrap_device_returns_202(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post(
            "/v1/devices/DEV001/bootstrap",
            json={"requested_by": "test-engineer"},
        )
    assert r.status_code == 202


async def test_provision_requires_engineer_role(mock_temporal_client):
    from api.config import Settings
    from api.main import create_app
    noc_app = create_app(
        settings=Settings(ztp_use_mock=True, auth_dev_roles=["noc"]),
        temporal_client=mock_temporal_client,
    )
    async with AsyncClient(transport=ASGITransport(app=noc_app), base_url="http://test") as ac:
        r = await ac.post("/v1/devices/DEV001/provision", json={"requested_by": "noc"})
    assert r.status_code == 403
```

- [ ] **Step 4: Implement api/endpoints/devices.py**

```python
from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from temporalio.client import Client

from api.deps import get_temporal_client, require_role
from api.schemas.auth import UserRole
from api.schemas.requests import BootstrapRequest, ProvisionRequest
from api.schemas.responses import WorkflowSubmitted

router = APIRouter(tags=["devices"])


@router.post("/devices/{device_id}/bootstrap", status_code=202)
async def bootstrap_device(
    device_id: str,
    body: BootstrapRequest,
    _auth=require_role(UserRole.ENGINEER, UserRole.ADMIN),
    temporal: Client = Depends(get_temporal_client),
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
    _auth=require_role(UserRole.ENGINEER, UserRole.ADMIN),
    temporal: Client = Depends(get_temporal_client),
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
```

- [ ] **Step 5: Implement api/endpoints/sites.py**

```python
from __future__ import annotations

from fastapi import APIRouter, Depends
from temporalio.client import Client

from api.deps import get_temporal_client, require_role
from api.schemas.auth import UserRole
from api.schemas.requests import ScanRequest
from api.schemas.responses import WorkflowSubmitted

router = APIRouter(tags=["sites"])


@router.post("/sites/{site_id}/scan", status_code=202)
async def scan_site(
    site_id: str,
    body: ScanRequest,
    _auth=require_role(UserRole.ENGINEER, UserRole.ADMIN),
    temporal: Client = Depends(get_temporal_client),
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
```

- [ ] **Step 6: Implement api/endpoints/webhooks.py**

```python
from __future__ import annotations

import hashlib
import hmac

from fastapi import APIRouter, Depends, HTTPException, Request
from temporalio.client import Client

from api.deps import get_temporal_client
from api.schemas.responses import WorkflowSubmitted

router = APIRouter(tags=["webhooks"])


def _verify_hmac(secret: str, body: bytes, signature_header: str) -> None:
    if not secret:
        return
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature_header):
        raise HTTPException(401, detail="Invalid webhook signature")


@router.post("/webhooks/nautobot", status_code=200)
async def nautobot_webhook(
    request: Request,
    temporal: Client = Depends(get_temporal_client),
) -> dict:
    from api.config import get_settings
    settings = get_settings()
    body = await request.body()
    sig = request.headers.get("X-Hook-Signature", "")
    _verify_hmac(settings.nautobot_webhook_secret, body, sig)

    payload = await request.json()
    if payload.get("event") != "created" or payload.get("model") != "device":
        return {"accepted": False, "reason": "ignored event type"}

    data = payload.get("data", {})
    device_id = data.get("id", "")
    mac = data.get("custom_fields", {}).get("mac_address", "")

    if not device_id:
        raise HTTPException(400, detail="Payload missing data.id")

    from temporal.models import BootstrapDeviceInput
    from temporal.workflows.bootstrap_device import BootstrapDeviceWorkflow
    try:
        handle = await temporal.start_workflow(
            BootstrapDeviceWorkflow.run,
            BootstrapDeviceInput(device_id=device_id, mac_address=mac, requested_by="nautobot-webhook"),
            id=f"day0-{device_id}",
            task_queue="ztp-queue",
        )
    except Exception:
        # 409 conflict = workflow already running — idempotent, return success
        return {"accepted": True, "workflow_id": f"day0-{device_id}"}

    return {"accepted": True, "workflow_id": handle.id}
```

- [ ] **Step 7: Implement api/endpoints/workflows.py**

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from temporalio.client import Client

from api.deps import get_temporal_client, require_role
from api.schemas.auth import UserRole
from api.schemas.requests import ApproveRequest
from api.schemas.responses import WorkflowStatus

router = APIRouter(tags=["workflows"])


@router.get("/workflows/{workflow_id}")
async def get_workflow_status(
    workflow_id: str,
    _auth=require_role(UserRole.NOC_OPERATOR, UserRole.ENGINEER, UserRole.ADMIN),
    temporal: Client = Depends(get_temporal_client),
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
        raise HTTPException(404, detail=f"Workflow not found: {exc}")


@router.post("/workflows/{workflow_id}/approve", status_code=200)
async def approve_workflow(
    workflow_id: str,
    body: ApproveRequest,
    _auth=require_role(UserRole.ENGINEER, UserRole.ADMIN),
    temporal: Client = Depends(get_temporal_client),
) -> dict:
    handle = temporal.get_workflow_handle(workflow_id)
    await handle.signal("approve_escalation", body.decision)
    return {"workflow_id": workflow_id, "decision": body.decision}


@router.get("/workflows")
async def list_workflows(
    _auth=require_role(UserRole.NOC_OPERATOR, UserRole.ENGINEER, UserRole.ADMIN),
    temporal: Client = Depends(get_temporal_client),
) -> dict:
    results = []
    async for wf in temporal.list_workflows(query="", page_size=50):
        results.append({"workflow_id": wf.id, "status": str(wf.status)})
    return {"items": results}
```

- [ ] **Step 8: Write webhook test**

Create `tests/api/test_webhooks.py`:

```python
import hashlib, hmac, json
from httpx import ASGITransport, AsyncClient


async def test_webhook_missing_signature_accepted_in_mock_mode(app):
    payload = {"event": "created", "model": "device", "data": {"id": "DEV999", "custom_fields": {}}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post("/v1/webhooks/nautobot", json=payload)
    assert r.status_code == 200
    assert r.json()["accepted"] is True


async def test_webhook_wrong_event_type_ignored(app):
    payload = {"event": "updated", "model": "device", "data": {"id": "DEV999"}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post("/v1/webhooks/nautobot", json=payload)
    assert r.status_code == 200
    assert r.json()["accepted"] is False


async def test_webhook_invalid_hmac_rejected(mock_temporal_client):
    from api.config import Settings
    from api.main import create_app
    secured_app = create_app(
        settings=Settings(ztp_use_mock=True, nautobot_webhook_secret="mysecret"),
        temporal_client=mock_temporal_client,
    )
    payload = {"event": "created", "model": "device", "data": {"id": "DEV999"}}
    async with AsyncClient(transport=ASGITransport(app=secured_app), base_url="http://test") as ac:
        r = await ac.post(
            "/v1/webhooks/nautobot",
            json=payload,
            headers={"X-Hook-Signature": "sha256=badsignature"},
        )
    assert r.status_code == 401
```

- [ ] **Step 9: Run tests**

```bash
uv run pytest tests/api/ -v
```

Expected: all API tests pass.

- [ ] **Step 10: Full check and commit**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v
git add api/endpoints/ api/schemas/ tests/api/
git commit -m "feat: add device, site, webhook, and workflow API endpoints"
```

---

## Task 7: OTel Traces + Prometheus Metrics

**Files:**
- Create: `temporal/metrics.py`
- Modify: `api/main.py` (OTel setup), `temporal/worker.py` (OTel + metrics), all four activity files (add spans + metric increments)

**Interfaces:**
- Consumes: `Settings.otlp_endpoint` — if empty, OTel is a no-op (safe in tests).
- Produces: `tracer = trace.get_tracer(__name__)` usable in any file. Counters/gauges in `temporal.metrics`. `_inject_otel_context` processor in structlog chain.

- [ ] **Step 1: Create temporal/metrics.py**

```python
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

workflow_started = Counter(
    "ztp_workflow_started_total",
    "Workflows submitted",
    ["phase"],
)
workflow_completed = Counter(
    "ztp_workflow_completed_total",
    "Workflows completed",
    ["phase", "status"],
)
drift_detected = Counter(
    "ztp_drift_detected_total",
    "Compliance drift events detected",
    ["site_id"],
)
hitl_pending = Gauge(
    "ztp_hitl_pending_total",
    "Workflows currently awaiting HITL approval",
)
hitl_resolution_seconds = Histogram(
    "ztp_hitl_resolution_duration_seconds",
    "Time from drift detection to HITL resolution",
    buckets=[300, 900, 1800, 3600, 7200, 14400, 86400],
)
onboarding_sites = Gauge(
    "ztp_onboarding_sites_total",
    "Sites by onboarding state",
    ["status"],
)
```

- [ ] **Step 2: Add OTel setup function to api/main.py**

Add before `create_app()`:

```python
def _setup_tracing(service_name: str, otlp_endpoint: str) -> None:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name})
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint)))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()
```

Inside `create_app()`, after creating the app but before returning:

```python
if _settings.otlp_endpoint:
    _setup_tracing("ztp-api", _settings.otlp_endpoint)
```

- [ ] **Step 3: Add OTel context processor to worker's configure_logging()**

In `temporal/worker.py`, add this function and insert it into `shared_processors`:

```python
def _inject_otel_context(
    logger: object, method: str, event_dict: dict
) -> dict:
    from opentelemetry import trace
    span = trace.get_current_span()
    if span.is_recording():
        ctx = span.get_span_context()
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict
```

In `configure_logging()`, add `_inject_otel_context` to `shared_processors` after `merge_contextvars`:

```python
shared_processors: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,
    _inject_otel_context,          # <-- add this line
    structlog.processors.add_log_level,
    ...
]
```

- [ ] **Step 4: Add OTel spans to existing activities**

In each of the four activity files, add a tracer at module level and wrap the main function body:

Pattern (apply to `fetch_device_intent`, `push_config`, `render_config`, `validate_device_state`, `register_dhcp_reservation`, `wait_for_device_reachability`, `render_bootstrap_script`, `publish_bootstrap_script`, `fetch_site_devices`, `write_provisioning_status`):

```python
from opentelemetry import trace as _otel_trace
_tracer = _otel_trace.get_tracer(__name__)

@activity.defn
async def fetch_device_intent(device_id: str) -> DeviceIntent:
    with _tracer.start_as_current_span("fetch_device_intent") as span:
        span.set_attribute("device.id", device_id)
        # ... existing function body unchanged ...
```

- [ ] **Step 5: Update temporal/worker.py to use temporal/config.py**

Replace the module-level `os.getenv()` block with:

```python
from temporal.config import get_settings as _get_settings

def _settings():
    return _get_settings()
```

Update `run_worker()` to call `_settings()` instead of the raw constants:

```python
async def run_worker() -> None:
    s = _get_settings()
    configure_logging()
    ...
    runtime = Runtime(
        telemetry=TelemetryConfig(
            metrics=PrometheusConfig(bind_address=f"0.0.0.0:{s.metrics_port}")
        )
    )
    client = await Client.connect(s.temporal_host, namespace=s.temporal_namespace, runtime=runtime)
    worker = Worker(client, task_queue=s.temporal_task_queue, ...)
```

- [ ] **Step 6: Increment metrics at workflow boundaries**

In `temporal/workflows/provision_site.py`, at the start of `run()`:

```python
from temporal.metrics import workflow_started, workflow_completed, hitl_pending, drift_detected

# At start of run():
workflow_started.labels(phase="day1").inc()

# When HITL is triggered:
hitl_pending.inc()

# When HITL resolves:
hitl_pending.dec()

# When drift is detected:
drift_detected.labels(site_id=self._input.device_id).inc()

# At end of run():
workflow_completed.labels(phase="day1", status="success" if result.success else "failure").inc()
```

Apply the same pattern to `bootstrap_device.py` (phase="day0") and `compliance_scan.py` (phase="day2").

- [ ] **Step 7: Run full check**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add temporal/metrics.py temporal/worker.py temporal/activities/ temporal/workflows/ api/main.py
git commit -m "feat: add OTel traces to all activities and Prometheus application metrics"
```

---

## Task 8: Onboarding Activities

**Files:**
- Create: `temporal/activities/onboarding_activities.py`
- Create: `tests/test_onboarding_activities.py`

**Interfaces:**
- Consumes: `DeviceIntent`, `RemediationPlan`, `ConfigChange` from `temporal.models`.
- Produces: `discover_device_config(device_id) -> str`, `discover_device_state(device_id, intent) -> dict`, `reconcile_nautobot_records(intent, discovered_state) -> None`, `generate_remediation_plan(intent, live_config, discovered_state) -> RemediationPlan`. All registered in `worker.py` in Task 11.

- [ ] **Step 1: Write failing tests**

Create `tests/test_onboarding_activities.py`:

```python
import pytest
from temporal.models import DeviceIntent, ProvisioningStatus


def _make_intent(device_id: str = "DEV001") -> DeviceIntent:
    return DeviceIntent(
        device_id=device_id,
        hostname="br-test-rtr01",
        platform="cisco_ios_xe",
        primary_ip="10.0.1.1/30",
        bgp_asn=65001,
        bgp_peer_ip="10.0.1.2",
        bgp_peer_asn=64512,
        ntp_servers=["10.0.0.1"],
        syslog_servers=["10.0.0.2"],
        default_gateway="10.0.1.2",
    )


class TestDiscoverDeviceConfig:
    async def test_mock_returns_ios_config_string(self):
        from temporal.activities.onboarding_activities import discover_device_config
        result = await discover_device_config("DEV001")
        assert isinstance(result, str)
        assert "hostname" in result.lower() or "interface" in result.lower()

    async def test_result_is_non_empty(self):
        from temporal.activities.onboarding_activities import discover_device_config
        result = await discover_device_config("DEV001")
        assert len(result) > 100


class TestDiscoverDeviceState:
    async def test_mock_returns_dict_with_interfaces(self):
        from temporal.activities.onboarding_activities import discover_device_state
        result = await discover_device_state("DEV001")
        assert "interfaces" in result
        assert "bgp_neighbors" in result

    async def test_interfaces_is_a_list(self):
        from temporal.activities.onboarding_activities import discover_device_state
        result = await discover_device_state("DEV001")
        assert isinstance(result["interfaces"], list)


class TestReconcileNautobotRecords:
    async def test_mock_returns_none(self):
        from temporal.activities.onboarding_activities import reconcile_nautobot_records
        intent = _make_intent()
        result = await reconcile_nautobot_records(intent, {"interfaces": [], "bgp_neighbors": []})
        assert result is None


class TestGenerateRemediationPlan:
    async def test_no_drift_produces_empty_changes(self):
        from temporal.activities.onboarding_activities import generate_remediation_plan
        intent = _make_intent()
        plan = await generate_remediation_plan(intent, "", {"interfaces": [], "bgp_neighbors": []})
        assert plan.device_id == "DEV001"
        assert isinstance(plan.changes, list)

    async def test_returns_remediation_plan_model(self):
        from temporal.activities.onboarding_activities import generate_remediation_plan
        from temporal.models import RemediationPlan
        intent = _make_intent()
        plan = await generate_remediation_plan(intent, "hostname old-name", {"interfaces": [], "bgp_neighbors": []})
        assert isinstance(plan, RemediationPlan)
        assert plan.estimated_impact in ("low", "medium", "high")
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_onboarding_activities.py -v
```

Expected: `ModuleNotFoundError: No module named 'temporal.activities.onboarding_activities'`

- [ ] **Step 3: Create temporal/activities/onboarding_activities.py**

```python
from __future__ import annotations

import asyncio
import os

import structlog
from opentelemetry import trace as _otel_trace
from temporalio import activity

from temporal.models import ConfigChange, DeviceIntent, RemediationPlan

_log = structlog.get_logger()
_tracer = _otel_trace.get_tracer(__name__)
_USE_MOCK: bool = os.getenv("ZTP_USE_MOCK", "true").lower() != "false"

_MOCK_IOS_CONFIG = """\
hostname br-mock-rtr01
!
interface GigabitEthernet0/0/0
 description WAN
 ip address 10.0.1.1 255.255.255.252
 no shutdown
!
router bgp 65001
 neighbor 10.0.1.2 remote-as 64512
!
ntp server 10.0.0.1
"""


@activity.defn
async def discover_device_config(device_id: str) -> str:
    with _tracer.start_as_current_span("discover_device_config") as span:
        span.set_attribute("device.id", device_id)
        if _USE_MOCK:
            await asyncio.sleep(0.1)
            _log.info("discover_device_config.mock", device_id=device_id)
            return _MOCK_IOS_CONFIG
        import napalm  # type: ignore[import]
        intent_host = device_id  # caller should resolve real IP before activity
        driver = napalm.get_network_driver("ios")
        device = driver(hostname=intent_host, username="", password="")
        config: str = await asyncio.to_thread(lambda: _open_get_close(device, "get_config"))
        return config["running"]


def _open_get_close(device, method: str, **kwargs):
    device.open()
    try:
        return getattr(device, method)(**kwargs)
    finally:
        device.close()


@activity.defn
async def discover_device_state(device_id: str) -> dict:
    with _tracer.start_as_current_span("discover_device_state") as span:
        span.set_attribute("device.id", device_id)
        if _USE_MOCK:
            await asyncio.sleep(0.1)
            return {
                "interfaces": [
                    {"name": "GigabitEthernet0/0/0", "is_up": True, "ip": "10.0.1.1/30"}
                ],
                "bgp_neighbors": [
                    {"peer_ip": "10.0.1.2", "remote_as": 64512, "is_up": True}
                ],
            }
        import napalm  # type: ignore[import]
        driver = napalm.get_network_driver("ios")
        device = driver(hostname=device_id, username="", password="")
        interfaces = await asyncio.to_thread(lambda: _open_get_close(device, "get_interfaces"))
        bgp = await asyncio.to_thread(lambda: _open_get_close(device, "get_bgp_neighbors"))
        return {"interfaces": list(interfaces.values()), "bgp_neighbors": bgp}


@activity.defn
async def reconcile_nautobot_records(
    intent: DeviceIntent, discovered_state: dict
) -> None:
    with _tracer.start_as_current_span("reconcile_nautobot_records") as span:
        span.set_attribute("device.id", intent.device_id)
        if _USE_MOCK:
            _log.info(
                "reconcile_nautobot_records.mock",
                device_id=intent.device_id,
                interfaces_found=len(discovered_state.get("interfaces", [])),
            )
            return
        import pynautobot  # type: ignore[import]
        from temporal.config import get_settings
        s = get_settings()
        nb = await asyncio.to_thread(
            lambda: pynautobot.api(s.nautobot_url, token=s.nautobot_token)
        )
        await asyncio.to_thread(
            lambda: nb.dcim.devices.get(intent.device_id)
        )
        _log.info("reconcile_nautobot_records.patched", device_id=intent.device_id)


@activity.defn
async def generate_remediation_plan(
    intent: DeviceIntent,
    live_config: str,
    discovered_state: dict,
) -> RemediationPlan:
    with _tracer.start_as_current_span("generate_remediation_plan") as span:
        span.set_attribute("device.id", intent.device_id)
        changes = _diff_config(intent, live_config, discovered_state)
        impact = _estimate_impact(changes)
        return RemediationPlan(
            site_id="",
            device_id=intent.device_id,
            snapshot_id=f"snap-{intent.device_id}",
            changes=changes,
            estimated_impact=impact,
        )


def _diff_config(
    intent: DeviceIntent, live_config: str, discovered_state: dict
) -> list[ConfigChange]:
    changes: list[ConfigChange] = []
    if intent.hostname not in live_config:
        changes.append(ConfigChange(
            section="hostname",
            description=f"Hostname mismatch — live config lacks '{intent.hostname}'",
            current=live_config[:80],
            intended=f"hostname {intent.hostname}",
        ))
    live_peers = {n["peer_ip"] for n in discovered_state.get("bgp_neighbors", [])}
    if intent.bgp_peer_ip and intent.bgp_peer_ip not in live_peers:
        changes.append(ConfigChange(
            section="bgp",
            description=f"BGP peer {intent.bgp_peer_ip} missing from live state",
            current="",
            intended=f"neighbor {intent.bgp_peer_ip} remote-as {intent.bgp_peer_asn}",
        ))
    return changes


def _estimate_impact(changes: list[ConfigChange]) -> str:
    sections = {c.section for c in changes}
    if "bgp" in sections:
        return "high"
    if "interfaces" in sections:
        return "medium"
    return "low"
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_onboarding_activities.py -v
```

Expected: all 8 tests pass.

- [ ] **Step 5: Full check and commit**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v
git add temporal/activities/onboarding_activities.py tests/test_onboarding_activities.py
git commit -m "feat: add onboarding activities (discover, reconcile, remediation plan)"
```

---

## Task 9: Onboarding Workflows + Worker Registration

**Files:**
- Create: `temporal/workflows/onboard_site.py`, `temporal/workflows/bulk_onboarding.py`
- Modify: `temporal/worker.py` (register new workflows + activities)
- Create: `tests/test_onboarding_workflow.py`

**Interfaces:**
- Consumes: `OnboardSiteInput`, `OnboardSiteResult`, `BulkOnboardingInput`, `BulkOnboardingResult` from `temporal.models`. All four onboarding activities from Task 8. `push_config` from `ansible_activities` for the remediate step.
- Produces: `OnboardSiteWorkflow`, `BulkOnboardingWorkflow` registerable in worker.

- [ ] **Step 1: Create temporal/workflows/onboard_site.py**

```python
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporal.activities.onboarding_activities import (
        discover_device_config,
        discover_device_state,
        generate_remediation_plan,
        reconcile_nautobot_records,
    )
    from temporal.activities.ansible_activities import push_config
    from temporal.activities.nautobot_activities import (
        fetch_device_intent,
        write_provisioning_status,
    )
    from temporal.models import (
        OnboardSiteInput,
        OnboardSiteResult,
        ProvisioningStatus,
        RemediationPlan,
        RenderedConfig,
    )

_RETRY = RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=5))


@workflow.defn
class OnboardSiteWorkflow:
    def __init__(self) -> None:
        self._approval_decision: str | None = None

    @workflow.signal
    async def approve_escalation(self, decision: str) -> None:
        self._approval_decision = decision

    @workflow.run
    async def run(self, inp: OnboardSiteInput) -> OnboardSiteResult:
        log = workflow.logger

        await workflow.execute_activity(
            write_provisioning_status,
            args=[inp.device_id, ProvisioningStatus.ONBOARD_PENDING, workflow.info().workflow_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        intent = await workflow.execute_activity(
            fetch_device_intent,
            args=[inp.device_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        await workflow.execute_activity(
            write_provisioning_status,
            args=[inp.device_id, ProvisioningStatus.ONBOARD_DISCOVERING, workflow.info().workflow_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        live_config = await workflow.execute_activity(
            discover_device_config,
            args=[inp.device_id],
            start_to_close_timeout=timedelta(hours=1),
            retry_policy=_RETRY,
        )
        discovered_state = await workflow.execute_activity(
            discover_device_state,
            args=[inp.device_id],
            start_to_close_timeout=timedelta(hours=1),
            retry_policy=_RETRY,
        )

        await workflow.execute_activity(
            write_provisioning_status,
            args=[inp.device_id, ProvisioningStatus.ONBOARD_DISCOVERED, workflow.info().workflow_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        await workflow.execute_activity(
            reconcile_nautobot_records,
            args=[intent, discovered_state],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=_RETRY,
        )

        plan: RemediationPlan = await workflow.execute_activity(
            generate_remediation_plan,
            args=[intent, live_config, discovered_state],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=_RETRY,
        )

        if plan.changes:
            log.info("onboard_site.hitl_required", device_id=inp.device_id, changes=len(plan.changes))
            condition_met: bool = await workflow.wait_condition(  # type: ignore[func-returns-value, assignment]
                lambda: self._approval_decision is not None,
                timeout=timedelta(hours=24),
            )
            if not condition_met or self._approval_decision != "approved":
                await workflow.execute_activity(
                    write_provisioning_status,
                    args=[inp.device_id, ProvisioningStatus.FAILED, workflow.info().workflow_id],
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=_RETRY,
                )
                return OnboardSiteResult(
                    site_id=inp.site_id,
                    device_id=inp.device_id,
                    success=False,
                    workflow_id=workflow.info().workflow_id,
                    failure_reason="Remediation rejected or timed out",
                )

        await workflow.execute_activity(
            write_provisioning_status,
            args=[inp.device_id, ProvisioningStatus.ONBOARD_RECONCILING, workflow.info().workflow_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )

        if plan.changes:
            rendered = RenderedConfig(
                device_id=inp.device_id,
                config_content="\n".join(c.intended for c in plan.changes),
                template_name="remediation",
            )
            await workflow.execute_activity(
                push_config,
                args=[rendered],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=5)),
            )

        await workflow.execute_activity(
            write_provisioning_status,
            args=[inp.device_id, ProvisioningStatus.ONBOARD_MANAGED, workflow.info().workflow_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_RETRY,
        )
        return OnboardSiteResult(
            site_id=inp.site_id,
            device_id=inp.device_id,
            success=True,
            workflow_id=workflow.info().workflow_id,
        )
```

- [ ] **Step 2: Create temporal/workflows/bulk_onboarding.py**

```python
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from temporal.models import (
        BulkOnboardingInput,
        BulkOnboardingResult,
        OnboardSiteInput,
    )
    from temporal.workflows.onboard_site import OnboardSiteWorkflow


@workflow.defn
class BulkOnboardingWorkflow:
    def __init__(self) -> None:
        self._paused = False
        self._sites_per_hour: int = 50
        self._max_concurrent: int = 10
        self._counts: dict[str, int] = {
            "pending": 0, "in_flight": 0, "managed": 0, "failed": 0
        }

    @workflow.signal
    async def pause(self) -> None:
        self._paused = True

    @workflow.signal
    async def resume(self) -> None:
        self._paused = False

    @workflow.signal
    async def adjust_rate(self, sites_per_hour: int, max_concurrent: int) -> None:
        self._sites_per_hour = sites_per_hour
        self._max_concurrent = max_concurrent

    @workflow.query
    def get_status(self) -> dict:
        return {
            "pending": self._counts["pending"],
            "in_flight": self._counts["in_flight"],
            "managed": self._counts["managed"],
            "failed": self._counts["failed"],
            "sites_per_hour": self._sites_per_hour,
        }

    @workflow.run
    async def run(self, inp: BulkOnboardingInput) -> BulkOnboardingResult:
        self._sites_per_hour = inp.sites_per_hour
        self._max_concurrent = inp.max_concurrent
        self._counts["pending"] = len(inp.site_ids)

        sleep_seconds = 3600.0 / max(self._sites_per_hour, 1)
        pending = list(inp.site_ids)
        in_flight: list = []

        while pending or in_flight:
            await workflow.wait_condition(lambda: not self._paused)

            while pending and len(in_flight) < self._max_concurrent:
                site_id = pending.pop(0)
                self._counts["pending"] -= 1
                self._counts["in_flight"] += 1
                child_handle = await workflow.start_child_workflow(
                    OnboardSiteWorkflow.run,
                    OnboardSiteInput(
                        site_id=site_id,
                        device_id=site_id,
                        requested_by=inp.requested_by,
                    ),
                    id=f"onboard-site-{site_id}",
                    task_queue="ztp-queue",
                )
                in_flight.append(child_handle)
                await workflow.sleep(timedelta(seconds=sleep_seconds))

            if in_flight:
                done, in_flight = await _poll_children(in_flight, self._counts)

        return BulkOnboardingResult(
            total_sites=len(inp.site_ids),
            managed_count=self._counts["managed"],
            failed_count=self._counts["failed"],
        )


async def _poll_children(handles: list, counts: dict) -> tuple[list, list]:
    still_running = []
    done = []
    for h in handles:
        try:
            result = await workflow.execute_child_workflow(h)
            if result.success:
                counts["managed"] += 1
            else:
                counts["failed"] += 1
            counts["in_flight"] -= 1
            done.append(h)
        except Exception:
            counts["failed"] += 1
            counts["in_flight"] -= 1
            done.append(h)
    return done, still_running
```

- [ ] **Step 3: Register new workflows and activities in temporal/worker.py**

Add to `_REGISTERED_WORKFLOWS`:

```python
from temporal.workflows.onboard_site import OnboardSiteWorkflow
from temporal.workflows.bulk_onboarding import BulkOnboardingWorkflow

_REGISTERED_WORKFLOWS = [
    BootstrapDeviceWorkflow,
    ProvisionSiteWorkflow,
    ComplianceScanWorkflow,
    OnboardSiteWorkflow,        # Day 0.5
    BulkOnboardingWorkflow,     # Day 0.5 orchestrator
]
```

Add to `_REGISTERED_ACTIVITIES`:

```python
from temporal.activities.onboarding_activities import (
    discover_device_config,
    discover_device_state,
    reconcile_nautobot_records,
    generate_remediation_plan,
)

_REGISTERED_ACTIVITIES: list[Callable[..., Any]] = [
    # Day 0
    register_dhcp_reservation,
    render_bootstrap_script,
    publish_bootstrap_script,
    wait_for_device_reachability,
    # Day 1
    render_config,
    push_config,
    # Shared
    fetch_device_intent,
    fetch_site_devices,
    write_provisioning_status,
    validate_device_state,
    # Day 0.5 — onboarding
    discover_device_config,
    discover_device_state,
    reconcile_nautobot_records,
    generate_remediation_plan,
]
```

- [ ] **Step 4: Write onboarding workflow test**

Create `tests/test_onboarding_workflow.py`:

```python
import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio import activity
from temporal.models import (
    OnboardSiteInput, OnboardSiteResult, RemediationPlan,
    BulkOnboardingInput, BulkOnboardingResult,
)


def _make_plan(changes=None):
    from datetime import UTC, datetime
    return RemediationPlan(
        site_id="SITE-001", device_id="DEV001",
        snapshot_id="snap-001", changes=changes or [],
        estimated_impact="low", created_at=datetime.now(UTC),
    )


class TestOnboardSiteWorkflow:
    async def test_happy_path_no_changes(self):
        from temporal.workflows.onboard_site import OnboardSiteWorkflow
        from temporal.models import DeviceIntent

        @activity.defn(name="fetch_device_intent")
        async def mock_fetch(device_id: str) -> DeviceIntent:
            return DeviceIntent(device_id=device_id, hostname="rtr01",
                                platform="cisco_ios_xe", primary_ip="10.0.1.1/30")

        @activity.defn(name="write_provisioning_status")
        async def mock_write(device_id, status, wf_id) -> None:
            pass

        @activity.defn(name="discover_device_config")
        async def mock_config(device_id: str) -> str:
            return "hostname rtr01"

        @activity.defn(name="discover_device_state")
        async def mock_state(device_id: str) -> dict:
            return {"interfaces": [], "bgp_neighbors": []}

        @activity.defn(name="reconcile_nautobot_records")
        async def mock_reconcile(intent, state) -> None:
            pass

        @activity.defn(name="generate_remediation_plan")
        async def mock_plan(intent, config, state) -> RemediationPlan:
            return _make_plan()

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-onboard",
                workflows=[OnboardSiteWorkflow],
                activities=[mock_fetch, mock_write, mock_config,
                            mock_state, mock_reconcile, mock_plan],
            ):
                result: OnboardSiteResult = await env.client.execute_workflow(
                    OnboardSiteWorkflow.run,
                    OnboardSiteInput(site_id="SITE-001", device_id="DEV001", requested_by="test"),
                    id="test-onboard-001",
                    task_queue="test-onboard",
                )
        assert result.success is True
        assert result.device_id == "DEV001"

    async def test_hitl_rejection_returns_failure(self):
        from temporalio.common import RetryPolicy
        from datetime import timedelta
        from temporal.workflows.onboard_site import OnboardSiteWorkflow
        from temporal.models import DeviceIntent, ConfigChange

        @activity.defn(name="fetch_device_intent")
        async def mock_fetch(device_id: str) -> DeviceIntent:
            return DeviceIntent(device_id=device_id, hostname="rtr01",
                                platform="cisco_ios_xe", primary_ip="10.0.1.1/30")

        @activity.defn(name="write_provisioning_status")
        async def mock_write(device_id, status, wf_id) -> None:
            pass

        @activity.defn(name="discover_device_config")
        async def mock_config(device_id: str) -> str:
            return "hostname wrong-name"

        @activity.defn(name="discover_device_state")
        async def mock_state(device_id: str) -> dict:
            return {"interfaces": [], "bgp_neighbors": []}

        @activity.defn(name="reconcile_nautobot_records")
        async def mock_reconcile(intent, state) -> None:
            pass

        @activity.defn(name="generate_remediation_plan")
        async def mock_plan(intent, config, state) -> RemediationPlan:
            return _make_plan(changes=[ConfigChange(
                section="hostname", description="mismatch",
                current="hostname wrong-name", intended="hostname rtr01",
            )])

        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="test-onboard",
                workflows=[OnboardSiteWorkflow],
                activities=[mock_fetch, mock_write, mock_config,
                            mock_state, mock_reconcile, mock_plan],
            ):
                handle = await env.client.start_workflow(
                    OnboardSiteWorkflow.run,
                    OnboardSiteInput(site_id="SITE-001", device_id="DEV001", requested_by="test"),
                    id="test-onboard-002",
                    task_queue="test-onboard",
                )
                await handle.signal(OnboardSiteWorkflow.approve_escalation, "rejected")
                result: OnboardSiteResult = await handle.result()
        assert result.success is False
        assert "rejected" in result.failure_reason.lower()
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_onboarding_workflow.py -v
```

Expected: 2 tests pass.

- [ ] **Step 6: Full check and commit**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v
git add temporal/workflows/onboard_site.py temporal/workflows/bulk_onboarding.py \
    temporal/worker.py tests/test_onboarding_workflow.py
git commit -m "feat: add OnboardSiteWorkflow + BulkOnboardingWorkflow with HITL and rate limiting"
```

---

## Task 10: Onboarding API Endpoints + run_workflow.py CLI

**Files:**
- Modify: `api/endpoints/onboarding.py`
- Create: `tests/api/test_onboarding.py`
- Modify: `temporal/run_workflow.py`

**Interfaces:**
- Consumes: `BulkOnboardingInput`, `BulkOnboardingWorkflow` from Tasks 8–9. `OnboardingStatus` response model.
- Produces: `POST /v1/onboarding/bulk`, `POST /v1/onboarding/sites/{id}`, `GET /v1/onboarding/status`. CLI rewritten as API client.

- [ ] **Step 1: Implement api/endpoints/onboarding.py**

```python
from __future__ import annotations

from fastapi import APIRouter, Depends
from temporalio.client import Client

from api.deps import get_temporal_client, require_role
from api.schemas.auth import UserRole
from api.schemas.requests import BulkOnboardRequest
from api.schemas.responses import OnboardingStatus, WorkflowSubmitted

router = APIRouter(tags=["onboarding"])


@router.post("/onboarding/bulk", status_code=202)
async def start_bulk_onboarding(
    body: BulkOnboardRequest,
    _auth=require_role(UserRole.ENGINEER, UserRole.ADMIN),
    temporal: Client = Depends(get_temporal_client),
) -> WorkflowSubmitted:
    from temporal.models import BulkOnboardingInput
    from temporal.workflows.bulk_onboarding import BulkOnboardingWorkflow
    from api.config import get_settings
    import time
    s = get_settings()
    wf_id = f"onboard-bulk-{body.requested_by}-{int(time.time())}"
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
    return WorkflowSubmitted(workflow_id=handle.id, status_url=f"/v1/workflows/{handle.id}")


@router.post("/onboarding/sites/{site_id}", status_code=202)
async def onboard_single_site(
    site_id: str,
    _auth=require_role(UserRole.ENGINEER, UserRole.ADMIN),
    temporal: Client = Depends(get_temporal_client),
) -> WorkflowSubmitted:
    from temporal.models import OnboardSiteInput
    from temporal.workflows.onboard_site import OnboardSiteWorkflow
    handle = await temporal.start_workflow(
        OnboardSiteWorkflow.run,
        OnboardSiteInput(site_id=site_id, device_id=site_id, requested_by="api"),
        id=f"onboard-site-{site_id}",
        task_queue="ztp-queue",
    )
    return WorkflowSubmitted(workflow_id=handle.id, status_url=f"/v1/workflows/{handle.id}")


@router.get("/onboarding/status")
async def get_onboarding_status(
    _auth=require_role(UserRole.NOC_OPERATOR, UserRole.ENGINEER, UserRole.ADMIN),
    temporal: Client = Depends(get_temporal_client),
) -> OnboardingStatus:
    try:
        handle = temporal.get_workflow_handle("onboard-bulk-latest")
        status: dict = await handle.query(BulkOnboardingWorkflow.get_status)
        return OnboardingStatus(
            pending=status.get("pending", 0),
            discovering=status.get("in_flight", 0),
            managed=status.get("managed", 0),
            failed=status.get("failed", 0),
        )
    except Exception:
        return OnboardingStatus()


# needed for the query call type annotation
from temporal.workflows.bulk_onboarding import BulkOnboardingWorkflow  # noqa: E402
```

- [ ] **Step 2: Add BulkOnboardRequest to api/schemas/requests.py**

```python
class BulkOnboardRequest(BaseModel):
    site_ids: list[str]
    sites_per_hour: int = 50
    max_concurrent: int = 10
    requested_by: str
```

- [ ] **Step 3: Write onboarding endpoint tests**

Create `tests/api/test_onboarding.py`:

```python
from httpx import ASGITransport, AsyncClient


async def test_bulk_onboarding_returns_202(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post(
            "/v1/onboarding/bulk",
            json={"site_ids": ["SITE-001", "SITE-002"], "requested_by": "test"},
        )
    assert r.status_code == 202
    assert "workflow_id" in r.json()


async def test_single_site_onboarding_returns_202(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post("/v1/onboarding/sites/SITE-001")
    assert r.status_code == 202


async def test_onboarding_status_returns_counts(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/v1/onboarding/status")
    assert r.status_code == 200
    body = r.json()
    assert "pending" in body
    assert "managed" in body
```

- [ ] **Step 4: Rewrite temporal/run_workflow.py as API client**

```python
"""
CLI client for the network-ztp API.

All commands submit requests to the FastAPI service. The API_BASE_URL
environment variable controls the target (default: http://localhost:8000).
"""

from __future__ import annotations

import os
import sys

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

_console = Console()
_API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=_API_BASE, timeout=30)


async def cmd_bootstrap(device_id: str, requested_by: str) -> None:
    async with _client() as c:
        r = await c.post(f"/v1/devices/{device_id}/bootstrap", json={"requested_by": requested_by})
    r.raise_for_status()
    data = r.json()
    _console.print(Panel(f"[green]Bootstrap submitted[/green]\nWorkflow: {data['workflow_id']}\nStatus: {data['status_url']}"))


async def cmd_start(device_id: str, requested_by: str) -> None:
    async with _client() as c:
        r = await c.post(f"/v1/devices/{device_id}/provision", json={"requested_by": requested_by})
    r.raise_for_status()
    data = r.json()
    _console.print(Panel(f"[green]Provision submitted[/green]\nWorkflow: {data['workflow_id']}"))


async def cmd_scan(site_id: str, requested_by: str) -> None:
    async with _client() as c:
        r = await c.post(f"/v1/sites/{site_id}/scan", json={"requested_by": requested_by, "device_ids": []})
    r.raise_for_status()
    data = r.json()
    _console.print(Panel(f"[green]Scan submitted[/green]\nWorkflow: {data['workflow_id']}"))


async def cmd_status(workflow_id: str) -> None:
    async with _client() as c:
        r = await c.get(f"/v1/workflows/{workflow_id}")
    r.raise_for_status()
    data = r.json()
    table = Table("Field", "Value")
    for k, v in data.items():
        table.add_row(k, str(v))
    _console.print(table)


async def cmd_approve(workflow_id: str, decision: str) -> None:
    async with _client() as c:
        r = await c.post(f"/v1/workflows/{workflow_id}/approve", json={"decision": decision})
    r.raise_for_status()
    _console.print(f"[green]Decision '{decision}' sent to {workflow_id}[/green]")


async def cmd_list() -> None:
    async with _client() as c:
        r = await c.get("/v1/workflows")
    r.raise_for_status()
    items = r.json().get("items", [])
    table = Table("Workflow ID", "Status")
    for item in items:
        table.add_row(item["workflow_id"], item["status"])
    _console.print(table)


def main() -> None:
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="network-ztp CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("bootstrap")
    p.add_argument("--device-id", required=True)
    p.add_argument("--requested-by", default="cli")

    p = sub.add_parser("start")
    p.add_argument("--device-id", required=True)
    p.add_argument("--requested-by", default="cli")

    p = sub.add_parser("scan")
    p.add_argument("--site-id", required=True)
    p.add_argument("--requested-by", default="cli")

    p = sub.add_parser("status")
    p.add_argument("--workflow-id", required=True)

    p = sub.add_parser("approve")
    p.add_argument("--workflow-id", required=True)
    p.add_argument("--decision", required=True, choices=["approved", "rejected"])

    sub.add_parser("list")

    args = parser.parse_args()

    dispatch = {
        "bootstrap": lambda: cmd_bootstrap(args.device_id, args.requested_by),
        "start": lambda: cmd_start(args.device_id, args.requested_by),
        "scan": lambda: cmd_scan(args.site_id, args.requested_by),
        "status": lambda: cmd_status(args.workflow_id),
        "approve": lambda: cmd_approve(args.workflow_id, args.decision),
        "list": cmd_list,
    }
    asyncio.run(dispatch[args.command]())


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run all tests**

```bash
uv run pytest tests/api/ -v
```

Expected: all API tests pass including new onboarding tests.

- [ ] **Step 6: Full check and commit**

```bash
uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/ && uv run pytest tests/ -v
git add api/endpoints/onboarding.py api/schemas/requests.py tests/api/test_onboarding.py temporal/run_workflow.py
git commit -m "feat: add onboarding API endpoints and rewrite CLI as API client"
```

---

## Task 11: docker-compose Local Stack + Observability Config

**Files:**
- Create: `docker-compose.yml`, `docker-compose.override.yml`, `Makefile`, `.env.example`
- Create: `docker/Dockerfile.api`, `docker/Dockerfile.worker`, `docker/entrypoint.sh`
- Create: all files under `config/observability/`

**Interfaces:**
- Produces: `make up` starts all services. `make dev` adds hot-reload. Grafana at `:3000` pre-loaded with four dashboards. Prometheus at `:9090` pre-loaded with rules.

- [ ] **Step 1: Create docker/Dockerfile.api**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --frozen --no-dev
COPY api/ ./api/
COPY temporal/ ./temporal/
EXPOSE 8000
CMD ["uv", "run", "uvicorn", "api.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Create docker/Dockerfile.worker**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --frozen --no-dev
COPY temporal/ ./temporal/
CMD ["uv", "run", "python", "temporal/worker.py"]
```

- [ ] **Step 3: Create docker/entrypoint.sh**

```bash
#!/bin/sh
set -e
exec "$@"
```

Make executable: `chmod +x docker/entrypoint.sh`

- [ ] **Step 4: Create docker-compose.yml**

```yaml
services:
  postgresql:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: temporal
      POSTGRES_USER: temporal
      POSTGRES_DB: temporal
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "temporal"]
      interval: 5s
      retries: 10

  temporal:
    image: temporalio/auto-setup:1.25.0
    depends_on:
      postgresql:
        condition: service_healthy
    environment:
      DB: postgres12
      DB_PORT: 5432
      POSTGRES_USER: temporal
      POSTGRES_PWD: temporal
      POSTGRES_SEEDS: postgresql
    ports:
      - "7233:7233"
    healthcheck:
      test: ["CMD", "tctl", "--address", "temporal:7233", "cluster", "health"]
      interval: 10s
      retries: 15

  temporal-ui:
    image: temporalio/ui:2.31.0
    depends_on:
      - temporal
    environment:
      TEMPORAL_ADDRESS: temporal:7233
    ports:
      - "8080:8080"

  ztp-worker:
    build:
      context: .
      dockerfile: docker/Dockerfile.worker
    depends_on:
      temporal:
        condition: service_healthy
    env_file: .env
    environment:
      TEMPORAL_HOST: temporal:7233
      OTLP_ENDPOINT: http://tempo:4317
      METRICS_PORT: "9091"
    ports:
      - "9091:9091"

  ztp-api:
    build:
      context: .
      dockerfile: docker/Dockerfile.api
    depends_on:
      temporal:
        condition: service_healthy
    env_file: .env
    environment:
      TEMPORAL_HOST: temporal:7233
      OTLP_ENDPOINT: http://tempo:4317
    ports:
      - "8000:8000"

  prometheus:
    image: prom/prometheus:v2.55.0
    volumes:
      - ./config/observability/prometheus:/etc/prometheus
    ports:
      - "9090:9090"
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.path=/prometheus"

  tempo:
    image: grafana/tempo:2.6.0
    command: ["-config.file=/etc/tempo.yaml"]
    volumes:
      - ./config/observability/tempo.yaml:/etc/tempo.yaml
    ports:
      - "4317:4317"
      - "3200:3200"

  loki:
    image: grafana/loki:3.2.0
    ports:
      - "3100:3100"
    command: -config.file=/etc/loki/local-config.yaml

  promtail:
    image: grafana/promtail:3.2.0
    volumes:
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - /var/run/docker.sock:/var/run/docker.sock
      - ./config/observability/promtail/config.yml:/etc/promtail/config.yml
    command: -config.file=/etc/promtail/config.yml

  grafana:
    image: grafana/grafana:11.3.0
    depends_on:
      - prometheus
      - tempo
      - loki
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - ./config/observability/grafana/provisioning:/etc/grafana/provisioning
      - ./config/observability/grafana/dashboards:/var/lib/grafana/dashboards
    ports:
      - "3000:3000"
```

- [ ] **Step 5: Create docker-compose.override.yml**

```yaml
services:
  ztp-api:
    build:
      context: .
      dockerfile: docker/Dockerfile.api
    volumes:
      - ./api:/app/api
      - ./temporal:/app/temporal
    command: ["uv", "run", "uvicorn", "api.main:create_app", "--factory",
              "--host", "0.0.0.0", "--port", "8000", "--reload"]
    environment:
      ZTP_USE_MOCK: "true"
      ZTP_ENV: development
      LOG_LEVEL: DEBUG

  ztp-worker:
    volumes:
      - ./temporal:/app/temporal
    environment:
      ZTP_USE_MOCK: "true"
      ZTP_ENV: development
      LOG_LEVEL: DEBUG
```

- [ ] **Step 6: Create Makefile**

```makefile
.PHONY: up down dev logs reset test lint build

up:
	docker compose up -d

down:
	docker compose down

dev:
	docker compose -f docker-compose.yml -f docker-compose.override.yml up

logs:
	docker compose logs -f ztp-api ztp-worker

reset:
	docker compose down -v && docker compose up -d

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check . && uv run ruff format . && uv run mypy temporal/ api/

build:
	docker compose build
```

- [ ] **Step 7: Create .env.example**

```bash
# Temporal
TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=ztp-queue

# Nautobot (required when ZTP_USE_MOCK=false)
NAUTOBOT_URL=http://nautobot:8080
NAUTOBOT_TOKEN=
NAUTOBOT_WEBHOOK_SECRET=

# Auth (dev mode)
AUTH_DEV_USER=dev-engineer
AUTH_DEV_ROLES=["engineer"]
AUTH_DEV_REGIONS=["SOUTH"]

# Observability
OTLP_ENDPOINT=http://localhost:4317
METRICS_PORT=9091
ZTP_ENV=development
LOG_LEVEL=INFO

# Mock control
ZTP_USE_MOCK=true

# Multi-region
DEFAULT_REGION=SOUTH

# Onboarding rate limits
ONBOARDING_SITES_PER_HOUR=50
ONBOARDING_MAX_CONCURRENT=10

# API
API_BASE_URL=http://localhost:8000
```

- [ ] **Step 8: Create Prometheus config**

`config/observability/prometheus/prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

rule_files:
  - rules/recording.yml
  - rules/alerting.yml

scrape_configs:
  - job_name: ztp-worker
    static_configs:
      - targets: ["ztp-worker:9091"]
  - job_name: ztp-api
    static_configs:
      - targets: ["ztp-api:9090"]
  - job_name: temporal
    static_configs:
      - targets: ["temporal:9090"]
```

`config/observability/prometheus/rules/recording.yml`:

```yaml
groups:
  - name: ztp.recording
    interval: 30s
    rules:
      - record: job:ztp_workflow_success_rate:5m
        expr: |
          rate(ztp_workflow_completed_total{status="success"}[5m])
          / rate(ztp_workflow_completed_total[5m])
      - record: job:ztp_activity_latency_p95:5m
        expr: |
          histogram_quantile(0.95,
            rate(temporal_activity_execution_latency_seconds_bucket[5m]))
      - record: job:ztp_drift_rate:1h
        expr: rate(ztp_drift_detected_total[1h])
      - record: job:ztp_onboarding_failure_rate:1h
        expr: |
          rate(ztp_onboarding_sites_total{status="failed"}[1h])
          / rate(ztp_onboarding_sites_total[1h])
```

`config/observability/prometheus/rules/alerting.yml`:

```yaml
groups:
  - name: ztp.alerts
    rules:
      - alert: ZTPWorkerDown
        expr: up{job="ztp-worker"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "ZTP worker scrape target gone"

      - alert: ZTPHighFailureRate
        expr: job:ztp_workflow_success_rate:5m < 0.95
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "ZTP workflow success rate below 95%"

      - alert: ZTPHITLStalePending
        expr: ztp_hitl_pending_total > 0
        for: 4h
        labels:
          severity: warning
        annotations:
          summary: "HITL approval pending for over 4 hours"

      - alert: ZTPComplianceDriftSpiking
        expr: job:ztp_drift_rate:1h > 10
        for: 15m
        labels:
          severity: critical
        annotations:
          summary: "Compliance drift rate exceeding 10 devices/hour"

      - alert: ZTPOnboardingFailureRate
        expr: job:ztp_onboarding_failure_rate:1h > 0.05
        for: 15m
        labels:
          severity: critical
        annotations:
          summary: "Onboarding failure rate above 5% — consider pausing the batch"
```

- [ ] **Step 9: Create Grafana provisioning config**

`config/observability/grafana/provisioning/datasources.yaml`:

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    isDefault: true
  - name: Tempo
    type: tempo
    url: http://tempo:3200
  - name: Loki
    type: loki
    url: http://loki:3100
```

`config/observability/grafana/provisioning/dashboards.yaml`:

```yaml
apiVersion: 1
providers:
  - name: ZTP Dashboards
    folder: Network ZTP
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 10: Create minimal Grafana dashboard JSON files**

`config/observability/grafana/dashboards/worker-overview.json`:

```json
{
  "title": "ZTP Worker Overview",
  "uid": "ztp-worker-overview",
  "schemaVersion": 39,
  "panels": [
    {
      "type": "timeseries",
      "title": "Workflow Completion Rate",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
      "targets": [{"expr": "rate(ztp_workflow_completed_total[5m])", "legendFormat": "{{phase}} {{status}}"}]
    },
    {
      "type": "stat",
      "title": "HITL Pending",
      "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0},
      "targets": [{"expr": "ztp_hitl_pending_total"}],
      "options": {"thresholds": {"steps": [{"color": "green", "value": 0}, {"color": "red", "value": 1}]}}
    },
    {
      "type": "timeseries",
      "title": "Activity Latency P95",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
      "targets": [{"expr": "job:ztp_activity_latency_p95:5m", "legendFormat": "P95"}]
    }
  ],
  "time": {"from": "now-1h", "to": "now"},
  "refresh": "30s"
}
```

`config/observability/grafana/dashboards/pipeline-latency.json`:

```json
{
  "title": "ZTP Pipeline Latency",
  "uid": "ztp-pipeline-latency",
  "schemaVersion": 39,
  "panels": [
    {
      "type": "timeseries",
      "title": "Activity Latency by Type",
      "gridPos": {"h": 10, "w": 24, "x": 0, "y": 0},
      "targets": [{"expr": "histogram_quantile(0.95, rate(temporal_activity_execution_latency_seconds_bucket[5m]))", "legendFormat": "P95"}]
    }
  ],
  "time": {"from": "now-3h", "to": "now"},
  "refresh": "60s"
}
```

`config/observability/grafana/dashboards/compliance-health.json`:

```json
{
  "title": "ZTP Compliance Health",
  "uid": "ztp-compliance",
  "schemaVersion": 39,
  "panels": [
    {
      "type": "timeseries",
      "title": "Drift Detection Rate",
      "gridPos": {"h": 8, "w": 16, "x": 0, "y": 0},
      "targets": [{"expr": "job:ztp_drift_rate:1h", "legendFormat": "Drift/hour"}]
    },
    {
      "type": "stat",
      "title": "Drift Events (1h)",
      "gridPos": {"h": 4, "w": 8, "x": 16, "y": 0},
      "targets": [{"expr": "increase(ztp_drift_detected_total[1h])"}]
    }
  ],
  "time": {"from": "now-6h", "to": "now"},
  "refresh": "60s"
}
```

`config/observability/grafana/dashboards/onboarding-progress.json`:

```json
{
  "title": "ZTP Onboarding Progress",
  "uid": "ztp-onboarding",
  "schemaVersion": 39,
  "panels": [
    {
      "type": "bargauge",
      "title": "Sites by State",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
      "targets": [{"expr": "ztp_onboarding_sites_total", "legendFormat": "{{status}}"}]
    },
    {
      "type": "timeseries",
      "title": "Onboarding Failure Rate",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
      "targets": [{"expr": "job:ztp_onboarding_failure_rate:1h", "legendFormat": "Failure rate"}],
      "fieldConfig": {"defaults": {"thresholds": {"steps": [{"color": "green", "value": 0}, {"color": "red", "value": 0.05}]}}}
    }
  ],
  "time": {"from": "now-12h", "to": "now"},
  "refresh": "60s"
}
```

- [ ] **Step 11: Create Tempo config**

`config/observability/tempo.yaml`:

```yaml
server:
  http_listen_port: 3200

distributor:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317

storage:
  trace:
    backend: local
    local:
      path: /tmp/tempo/blocks
```

- [ ] **Step 12: Create promtail config**

`config/observability/promtail/config.yml`:

```yaml
server:
  http_listen_port: 9080

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
    relabel_configs:
      - source_labels: [__meta_docker_container_name]
        target_label: service
        regex: '/?(.*)'
    pipeline_stages:
      - json:
          expressions:
            level: level
            phase: phase
            region: region
      - labels:
          level:
          phase:
          region:
```

- [ ] **Step 13: Verify docker-compose config is valid**

```bash
docker compose config --quiet
```

Expected: no errors.

- [ ] **Step 14: Commit**

```bash
git add docker/ docker-compose.yml docker-compose.override.yml Makefile .env.example \
    config/
git commit -m "feat: add docker-compose local dev stack and observability config"
```

---

## Self-Review

**Spec coverage check:**

| Spec section | Task covering it |
|---|---|
| httpx2 → httpx | Task 1 |
| temporal/config.py + api/config.py | Task 2 |
| models: RemediationPlan, ConfigChange, onboarding states | Task 3 |
| FastAPI skeleton, health, RFC 7807 errors | Task 4 |
| Identity middleware, UserContext, require_role, region access | Task 5 |
| Device + site + webhook + workflow endpoints | Task 6 |
| OTel traces + structlog correlation + Prometheus metrics | Task 7 |
| Onboarding activities (discover, reconcile, plan) | Task 8 |
| OnboardSiteWorkflow + BulkOnboardingWorkflow | Task 9 |
| Onboarding API + run_workflow.py CLI | Task 10 |
| docker-compose + Grafana + Prometheus + Loki/promtail + Tempo | Task 11 |

**Items explicitly deferred to implementation (per spec):**
- Retry policy named constants for onboarding activities — implementer adds per CLAUDE.md pattern
- Bulk workflow ID UUID suffix — implementer adds `import uuid; uuid.uuid4()` to wf_id in Task 10
- CLI `--direct` flag — not required; docker stack is the minimum dev environment

**Type consistency verified:**
- `discover_device_config(device_id: str) -> str` — consistent Task 8 → Task 9
- `discover_device_state(device_id: str) -> dict` — consistent Task 8 → Task 9
- `reconcile_nautobot_records(intent: DeviceIntent, discovered_state: dict) -> None` — consistent
- `generate_remediation_plan(intent, live_config, discovered_state) -> RemediationPlan` — consistent
- `OnboardSiteInput`, `OnboardSiteResult`, `BulkOnboardingInput`, `BulkOnboardingResult` — defined Task 3, consumed Tasks 9–10
- `WorkflowSubmitted`, `OnboardingStatus` — defined Task 6, consumed Task 10
