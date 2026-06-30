from httpx import ASGITransport, AsyncClient


async def test_list_workflows_returns_200(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/v1/workflows")
    assert r.status_code == 200
    body = r.json()
    assert "items" in body


async def test_get_workflow_status(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.get("/v1/workflows/test-workflow-id-001")
    assert r.status_code == 200
    body = r.json()
    assert body["workflow_id"] == "test-workflow-id-001"
    assert "status" in body


async def test_approve_workflow(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post(
            "/v1/workflows/test-workflow-id-001/approve",
            json={"decision": "approved"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["decision"] == "approved"
