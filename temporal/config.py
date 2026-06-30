from __future__ import annotations

from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "ztp-queue"

    nautobot_url: str = "http://localhost:8080"
    nautobot_token: str = ""
    nautobot_webhook_secret: str = ""

    otlp_endpoint: str = ""
    metrics_port: int = 9091
    ztp_env: Literal["development", "production"] = "development"
    log_level: str = "INFO"

    ztp_use_mock: bool = True

    onboarding_sites_per_hour: int = 50
    onboarding_max_concurrent: int = 10
    default_region: str = "SOUTH"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    @model_validator(mode="after")
    def require_live_credentials(self) -> Settings:
        if not self.ztp_use_mock:
            missing = [
                f for f in ("nautobot_token", "nautobot_webhook_secret") if not getattr(self, f)
            ]
            if missing:
                raise ValueError(f"Live mode requires: {', '.join(m.upper() for m in missing)}")
        return self


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
