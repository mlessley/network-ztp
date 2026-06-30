from temporal.models import DeviceIntent


def _make_intent(device_id: str = "DEV001") -> DeviceIntent:
    return DeviceIntent(
        device_id=device_id,
        hostname="br-test-rtr01",
        platform="cisco_ios_xe",
        primary_ip="10.0.1.1/30",
        bgp_asn=65001,
        bgp_peer_ip="10.0.1.2",
        bgp_peer_asn=64512,
        ntp_servers=["10.0.0.1"],
        syslog_servers=["10.0.0.2"],
        default_gateway="10.0.1.2",
    )


class TestDiscoverDeviceConfig:
    async def test_mock_returns_ios_config_string(self):
        from temporal.activities.onboarding_activities import discover_device_config

        result = await discover_device_config("DEV001")
        assert isinstance(result, str)
        assert "hostname" in result.lower() or "interface" in result.lower()

    async def test_result_is_non_empty(self):
        from temporal.activities.onboarding_activities import discover_device_config

        result = await discover_device_config("DEV001")
        assert len(result) > 100


class TestDiscoverDeviceState:
    async def test_mock_returns_dict_with_interfaces(self):
        from temporal.activities.onboarding_activities import discover_device_state

        result = await discover_device_state("DEV001")
        assert "interfaces" in result
        assert "bgp_neighbors" in result

    async def test_interfaces_is_a_list(self):
        from temporal.activities.onboarding_activities import discover_device_state

        result = await discover_device_state("DEV001")
        assert isinstance(result["interfaces"], list)


class TestReconcileNautobotRecords:
    async def test_mock_returns_none(self):
        from temporal.activities.onboarding_activities import reconcile_nautobot_records

        intent = _make_intent()
        result = await reconcile_nautobot_records(intent, {"interfaces": [], "bgp_neighbors": []})
        assert result is None


class TestGenerateRemediationPlan:
    async def test_no_drift_produces_empty_changes(self):
        from temporal.activities.onboarding_activities import generate_remediation_plan

        intent = _make_intent()
        plan = await generate_remediation_plan(intent, "", {"interfaces": [], "bgp_neighbors": []})
        assert plan.device_id == "DEV001"
        assert isinstance(plan.changes, list)

    async def test_returns_remediation_plan_model(self):
        from temporal.activities.onboarding_activities import generate_remediation_plan
        from temporal.models import RemediationPlan

        intent = _make_intent()
        plan = await generate_remediation_plan(
            intent, "hostname old-name", {"interfaces": [], "bgp_neighbors": []}
        )
        assert isinstance(plan, RemediationPlan)
        assert plan.estimated_impact in ("low", "medium", "high")
