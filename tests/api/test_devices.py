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
