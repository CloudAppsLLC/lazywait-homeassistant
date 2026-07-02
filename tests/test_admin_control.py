"""HA-free tests for the admin-control surface.

The conftest loads const.py + api.py + control.py without executing the
HA-importing package __init__ (minimal homeassistant stubs stand in when the
real package is absent), so this file covers the WS URL derivation, the
allowlist/denylist rules (widened in 26.7.8: curated OR not hard-denied), and
the state-snapshot builder (all domains + attribute filtering + area/device
enrichment). The WS push loops live in test_admin_ws_push.py.
"""

import aiohttp
import pytest

from conftest import (
    FakeDevice,
    FakeDeviceRegistry,
    FakeEntityRegistry,
    FakeRegistryEntry,
    FakeState,
)
from custom_components.lazywait import control
from custom_components.lazywait.api import LazyWaitApiClient
from custom_components.lazywait.const import (
    ADMIN_WS_PATH,
    AUTOMATION_OP_ALLOWLIST,
    DEVICE_CONTROL_ALLOWLIST,
    GENERIC_ATTRIBUTES,
    HARD_DENY_DOMAINS,
)


@pytest.mark.asyncio
async def test_ws_url_https_to_wss() -> None:
    async with aiohttp.ClientSession() as session:
        client = LazyWaitApiClient("https://apiv2.lazywait.com/v1", session, token="t")
        url = client.ws_url(ADMIN_WS_PATH)
        assert url == f"wss://apiv2.lazywait.com/v1{ADMIN_WS_PATH}"


@pytest.mark.asyncio
async def test_ws_url_http_to_ws() -> None:
    async with aiohttp.ClientSession() as session:
        client = LazyWaitApiClient("http://localhost:8080/v1", session, token="t")
        url = client.ws_url(ADMIN_WS_PATH)
        assert url == f"ws://localhost:8080/v1{ADMIN_WS_PATH}"


def test_device_allowlist_has_core_domains() -> None:
    assert "turn_on" in DEVICE_CONTROL_ALLOWLIST["light"]
    assert "unlock" in DEVICE_CONTROL_ALLOWLIST["lock"]
    assert "alarm_disarm" in DEVICE_CONTROL_ALLOWLIST["alarm_control_panel"]


def test_device_allowlist_gained_script_and_buttons() -> None:
    # 26.7.8: scripts/buttons joined the curated (write-tier) list.
    assert DEVICE_CONTROL_ALLOWLIST["script"] == {"turn_on", "turn_off", "toggle"}
    assert DEVICE_CONTROL_ALLOWLIST["button"] == {"press"}
    assert DEVICE_CONTROL_ALLOWLIST["input_button"] == {"press"}


def test_hard_denylist_blocks_dangerous_domains() -> None:
    for domain in ("homeassistant", "hassio", "shell_command", "python_script"):
        assert domain in HARD_DENY_DOMAINS
        # A denied domain must never appear in the positive allowlist.
        assert domain not in DEVICE_CONTROL_ALLOWLIST


def test_automation_ops_complete() -> None:
    for op in ("list", "enable", "disable", "trigger", "upsert", "delete"):
        assert op in AUTOMATION_OP_ALLOWLIST


def test_generic_attributes_are_display_only() -> None:
    assert GENERIC_ATTRIBUTES == {
        "friendly_name",
        "device_class",
        "unit_of_measurement",
        "icon",
    }


# ── Widened service gate (26.7.8) ────────────────────────────────────────────


def test_service_allowed_widened_to_non_denied_domains() -> None:
    # Curated entries still pass.
    assert control._service_allowed("light", "turn_on")
    assert control._service_allowed("script", "turn_on")
    # NEW: uncurated-but-not-denied domains pass (the cloud gates their tier).
    assert control._service_allowed("notify", "mobile_app_phone")
    assert control._service_allowed("mqtt", "publish")


def test_service_allowed_still_hard_denies() -> None:
    for domain in HARD_DENY_DOMAINS:
        assert not control._service_allowed(domain, "anything")
    assert not control._service_allowed("shell_command", "run")
    assert not control._service_allowed("homeassistant", "restart")
    assert not control._service_allowed("", "turn_on")
    assert not control._service_allowed("light", "")


def test_automation_config_allowed_widened() -> None:
    # Uncurated (notify) service refs now pass...
    assert control.automation_config_allowed(
        {"action": [{"service": "notify.mobile_app_phone", "data": {"message": "x"}}]}
    )
    # ...but hard-denied refs still poison the whole config, however nested.
    assert not control.automation_config_allowed(
        {
            "action": [
                {
                    "choose": [
                        {"sequence": [{"service": "shell_command.reboot_nvr"}]}
                    ]
                }
            ]
        }
    )
    assert not control.automation_config_allowed(
        {"action": [{"action": "hassio.host_reboot"}]}
    )


# ── State snapshot: all domains + attribute filtering + enrichment ──────────


def test_snapshot_reports_all_domains(make_hass) -> None:
    hass = make_hass(
        states=[
            FakeState("light.kitchen"),
            FakeState("sensor.temp", state="21.5"),
            FakeState("weird_domain.thing", state="idle"),
        ]
    )
    snapshot = control.build_state_snapshot(hass)
    by_id = {e["entity_id"]: e for e in snapshot}
    assert set(by_id) == {"light.kitchen", "sensor.temp", "weird_domain.thing"}
    assert by_id["light.kitchen"]["controllable"] is True
    assert by_id["sensor.temp"]["controllable"] is False
    assert by_id["weird_domain.thing"]["controllable"] is False
    assert by_id["weird_domain.thing"]["domain"] == "weird_domain"


def test_snapshot_generic_attribute_fallback(make_hass) -> None:
    hass = make_hass(
        states=[
            FakeState(
                "weird_domain.thing",
                attributes={
                    "friendly_name": "Thing",
                    "icon": "mdi:cog",
                    "device_class": "gadget",
                    "unit_of_measurement": "u",
                    "access_token": "SECRET",
                    "entity_picture": "/local/x.png",
                },
            ),
            FakeState(
                "light.kitchen",
                attributes={
                    "friendly_name": "Kitchen",
                    "brightness": 128,
                    "icon": "mdi:bulb",  # NOT in the light allowlist
                },
            ),
        ]
    )
    by_id = {e["entity_id"]: e for e in control.build_state_snapshot(hass)}
    # Unlisted domain → GENERIC_ATTRIBUTES only; secrets never pass.
    assert by_id["weird_domain.thing"]["attributes"] == {
        "friendly_name": "Thing",
        "icon": "mdi:cog",
        "device_class": "gadget",
        "unit_of_measurement": "u",
    }
    # Listed domain → friendly_name + its curated set (no generic extras).
    assert by_id["light.kitchen"]["attributes"] == {
        "friendly_name": "Kitchen",
        "brightness": 128,
    }


def test_snapshot_area_device_enrichment(make_hass) -> None:
    hass = make_hass(
        states=[
            FakeState("light.own_area"),
            FakeState("light.via_device"),
            FakeState("light.unregistered"),
        ],
        entity_registry=FakeEntityRegistry(
            [
                # Entity-level area override wins.
                FakeRegistryEntry("light.own_area", area_id="area_a", device_id="dev_1"),
                # No entity area → falls back to the owning device's area.
                FakeRegistryEntry("light.via_device", device_id="dev_2"),
            ]
        ),
        device_registry=FakeDeviceRegistry(
            [
                FakeDevice("dev_1", name="Lamp A", area_id="area_ignored"),
                FakeDevice("dev_2", name="Lamp B", area_id="area_b"),
            ]
        ),
    )
    by_id = {e["entity_id"]: e for e in control.build_state_snapshot(hass)}
    assert by_id["light.own_area"]["area_id"] == "area_a"
    assert by_id["light.own_area"]["device_id"] == "dev_1"
    assert by_id["light.via_device"]["area_id"] == "area_b"
    assert by_id["light.via_device"]["device_id"] == "dev_2"
    assert by_id["light.unregistered"]["area_id"] is None
    assert by_id["light.unregistered"]["device_id"] is None


def test_snapshot_survives_missing_registries(make_hass) -> None:
    # A hass with no registries (early boot) degrades to null enrichment.
    hass = make_hass(states=[FakeState("light.kitchen")])
    (entity,) = control.build_state_snapshot(hass)
    assert entity["area_id"] is None
    assert entity["device_id"] is None


def test_snapshot_controllable_first_then_cap(make_hass, monkeypatch) -> None:
    hass = make_hass(
        states=[
            FakeState("sensor.a"),
            FakeState("light.b"),
            FakeState("sensor.c"),
            FakeState("switch.d"),
        ]
    )
    monkeypatch.setattr(control, "ADMIN_STATE_MAX_ENTITIES", 3)
    snapshot = control.build_state_snapshot(hass)
    assert [e["entity_id"] for e in snapshot] == ["light.b", "switch.d", "sensor.a"]


def test_snapshot_only_entity_ids_filters(make_hass) -> None:
    hass = make_hass(states=[FakeState("light.a"), FakeState("light.b")])
    snapshot = control.build_state_snapshot(hass, only_entity_ids={"light.b"})
    assert [e["entity_id"] for e in snapshot] == ["light.b"]
