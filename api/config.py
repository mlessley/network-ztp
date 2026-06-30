from __future__ import annotations

from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"

    api_port: int = 8000

    nautobot_url: str = "http://localhost:8080"
    nautobot_token: str = ""
    nautobot_webhook_secret: str = ""

    auth_dev_user: str = "dev-user"
    auth_dev_roles: list[str] = ["engineer"]
    auth_dev_regions: list[str] = ["SOUTH"]

    otlp_endpoint: str = ""
    ztp_env: Literal["development", "production"] = "development"
    log_level: str = "INFO"

    ztp_use_mock: bool = True
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
