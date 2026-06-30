from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class UserRole(StrEnum):
    ADMIN = "admin"
    ENGINEER = "engineer"
    NOC_OPERATOR = "noc"
    SERVICE_ACCOUNT = "service"


@dataclass
class UserContext:
    username: str
    role: UserRole
    regions: list[str] = field(default_factory=list)
