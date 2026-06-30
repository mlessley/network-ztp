import pytest
from pydantic import ValidationError


class TestTemporalConfig:
    def test_defaults_are_valid(self):
        from temporal.config import Settings

        s = Settings()
        assert s.temporal_host == "localhost:7233"
        assert s.ztp_use_mock is True

    def test_live_mode_requires_nautobot_token(self):
        from temporal.config import Settings

        with pytest.raises(ValidationError, match="NAUTOBOT_TOKEN"):
            Settings(ztp_use_mock=False, nautobot_token="", nautobot_webhook_secret="")

    def test_live_mode_passes_with_credentials(self):
        from temporal.config import Settings

        s = Settings(
            ztp_use_mock=False,
            nautobot_token="tok",
            nautobot_webhook_secret="sec",
        )
        assert s.ztp_use_mock is False
