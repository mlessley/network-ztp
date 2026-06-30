import pytest
from httpx import ASGITransport, AsyncClient

from api.config import Settings
from api.main import create_app


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
