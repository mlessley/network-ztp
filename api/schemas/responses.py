from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class WorkflowSubmitted(BaseModel):
    workflow_id: str
    status_url: str


class WorkflowStatus(BaseModel):
    workflow_id: str
    status: str
    device_id: str | None = None
    site_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    failure_reason: str | None = None
    trace_id: str | None = None


class OnboardingStatus(BaseModel):
    pending: int = 0
    discovering: int = 0
    discovered: int = 0
    reconciling: int = 0
    managed: int = 0
    failed: int = 0
    sites_per_hour_actual: float = 0.0
    estimated_completion: datetime | None = None
