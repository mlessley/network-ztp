from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

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
    handle.describe = AsyncMock(
        return_value=MagicMock(
            status="RUNNING",
            start_time=None,
            close_time=None,
        )
    )

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
