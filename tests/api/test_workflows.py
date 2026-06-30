from httpx import ASGITransport, AsyncClient


async def test_list_workflows_returns_200(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/v1/workflows")
    assert r.status_code == 200
    body = r.json()
    assert "items" in body
