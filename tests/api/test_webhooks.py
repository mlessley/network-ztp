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
