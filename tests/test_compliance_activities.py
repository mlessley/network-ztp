"""Unit tests for Day 2 compliance scan activities."""

from __future__ import annotations

from temporal.activities.nautobot_activities import fetch_site_devices


class TestFetchSiteDevices:
    async def test_returns_list_of_ids(self) -> None:
        result = await fetch_site_devices("SITE001")
        assert isinstance(result, list)
        assert len(result) > 0

    async def test_ids_are_strings(self) -> None:
        result = await fetch_site_devices("SITE001")
        assert all(isinstance(d, str) for d in result)

    async def test_different_sites_same_mock_shape(self) -> None:
        r1 = await fetch_site_devices("SITE001")
        r2 = await fetch_site_devices("SITE002")
        assert len(r1) == len(r2)
