from __future__ import annotations

import hashlib
import hmac

from fastapi import APIRouter, Depends, HTTPException, Request
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from api.deps import get_temporal_client

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
    temporal: Client = Depends(get_temporal_client),  # noqa: B008
) -> dict[str, object]:
    settings = request.app.state.settings
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
            BootstrapDeviceInput(
                device_id=device_id, mac_address=mac, requested_by="nautobot-webhook"
            ),
            id=f"day0-{device_id}",
            task_queue="ztp-queue",
        )
    except WorkflowAlreadyStartedError:
        return {"accepted": True, "workflow_id": f"day0-{device_id}"}

    return {"accepted": True, "workflow_id": handle.id}
