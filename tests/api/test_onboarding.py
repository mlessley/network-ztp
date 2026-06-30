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
        r = await ac.get("/v1/onboarding/status?requested_by=test-engineer")
    assert r.status_code == 200
    body = r.json()
    assert "pending" in body
    assert "managed" in body
