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
