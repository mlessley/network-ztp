from unittest.mock import AsyncMock, MagicMock

from httpx import ASGITransport, AsyncClient


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
