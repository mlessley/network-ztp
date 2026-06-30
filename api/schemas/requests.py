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


class BulkOnboardRequest(BaseModel):
    site_ids: list[str]
    sites_per_hour: int = 50
    max_concurrent: int = 10
    requested_by: str


class AdjustRateRequest(BaseModel):
    sites_per_hour: int
    max_concurrent: int
