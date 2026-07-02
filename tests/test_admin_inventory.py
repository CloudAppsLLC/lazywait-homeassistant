"""Unit tests for the inventory builders in control.py (26.7.8):

  * build_registry_snapshot — areas + devices metadata, caps, name rules
  * build_services_catalog  — domain/service names + trimmed descriptions
  * page_state_snapshot     — WS paging math

All run against the conftest fakes (no real homeassistant needed).
"""

import pytest

from conftest import (
    FakeArea,
    FakeAreaRegistry,
    FakeDevice,
    FakeDeviceRegistry,
)
from custom_components.lazywait import control


# ── registry_snapshot ────────────────────────────────────────────────────────


def test_registry_snapshot_shapes(make_hass) -> None:
    hass = make_hass(
        area_registry=FakeAreaRegistry(
            [FakeArea("area_1", "Kitchen"), FakeArea("area_2", "Bar")]
        ),
        device_registry=FakeDeviceRegistry(
            [
                FakeDevice(
                    "dev_1",
                    name="Hue Bridge",
                    manufacturer="Signify",
                    model="BSB002",
                    area_id="area_1",
                ),
                FakeDevice(
                    "dev_2",
                    name="Bulb",
                    name_by_user="Bar Bulb",  # user rename wins
                    via_device_id="dev_1",
                ),
            ]
        ),
    )
    snapshot = control.build_registry_snapshot(hass)
    assert snapshot["areas"] == [
        {"id": "area_1", "name": "Kitchen"},
        {"id": "area_2", "name": "Bar"},
    ]
    by_id = {d["id"]: d for d in snapshot["devices"]}
    assert by_id["dev_1"] == {
        "id": "dev_1",
        "name": "Hue Bridge",
        "manufacturer": "Signify",
        "model": "BSB002",
        "area_id": "area_1",
        "via_device_id": None,
    }
    assert by_id["dev_2"]["name"] == "Bar Bulb"
    assert by_id["dev_2"]["via_device_id"] == "dev_1"
    assert by_id["dev_2"]["manufacturer"] is None


def test_registry_snapshot_skips_unnamed_and_trims(make_hass) -> None:
    long_name = "x" * 300
    hass = make_hass(
        area_registry=FakeAreaRegistry([FakeArea("area_1", long_name)]),
        device_registry=FakeDeviceRegistry(
            [
                FakeDevice("dev_named", name=long_name, manufacturer=long_name),
                FakeDevice("dev_unnamed"),  # no name at all → skipped
                FakeDevice("dev_nonstr", name=42),  # non-string name → skipped
            ]
        ),
    )
    snapshot = control.build_registry_snapshot(hass)
    assert snapshot["areas"][0]["name"] == "x" * 128
    assert [d["id"] for d in snapshot["devices"]] == ["dev_named"]
    assert snapshot["devices"][0]["name"] == "x" * 128
    assert snapshot["devices"][0]["manufacturer"] == "x" * 128


def test_registry_snapshot_caps_trim_not_fail(make_hass, monkeypatch) -> None:
    hass = make_hass(
        area_registry=FakeAreaRegistry(
            [FakeArea(f"area_{i}", f"Area {i}") for i in range(5)]
        ),
        device_registry=FakeDeviceRegistry(
            [FakeDevice(f"dev_{i}", name=f"Device {i}") for i in range(5)]
        ),
    )
    monkeypatch.setattr(control, "ADMIN_REGISTRY_MAX_AREAS", 2)
    monkeypatch.setattr(control, "ADMIN_REGISTRY_MAX_DEVICES", 3)
    snapshot = control.build_registry_snapshot(hass)
    assert len(snapshot["areas"]) == 2
    assert len(snapshot["devices"]) == 3


def test_registry_snapshot_empty_without_registries(make_hass) -> None:
    snapshot = control.build_registry_snapshot(make_hass())
    assert snapshot == {"areas": [], "devices": []}


# ── services_catalog ─────────────────────────────────────────────────────────


def _services_hass(make_hass, **kwargs):
    return make_hass(
        services={
            "light": {"turn_on": object(), "turn_off": object()},
            "notify": {"mobile_app_phone": object()},
            # Hard-denied domains must never reach the catalog.
            "shell_command": {"reboot_nvr": object()},
            "homeassistant": {"restart": object()},
        },
        **kwargs,
    )


@pytest.mark.asyncio
async def test_services_catalog_excludes_hard_denied(make_hass) -> None:
    hass = _services_hass(make_hass)
    domains = await control.build_services_catalog(hass, descriptions={})
    names = [d["domain"] for d in domains]
    assert names == ["light", "notify"]
    light = domains[0]
    assert [s["name"] for s in light["services"]] == ["turn_off", "turn_on"]


@pytest.mark.asyncio
async def test_services_catalog_descriptions_trimmed_or_omitted(make_hass) -> None:
    hass = _services_hass(make_hass)
    long_text = "Turn on. " * 40  # >> 140 chars
    domains = await control.build_services_catalog(
        hass,
        descriptions={"light": {"turn_on": {"description": long_text}}},
    )
    light = {s["name"]: s for s in domains[0]["services"]}
    assert len(light["turn_on"]["description"]) == 140
    # Absent description → key omitted entirely (not null).
    assert "description" not in light["turn_off"]


@pytest.mark.asyncio
async def test_services_catalog_caps_trim_not_fail(make_hass, monkeypatch) -> None:
    hass = make_hass(
        services={
            f"domain_{i:02d}": {f"svc_{j}": object() for j in range(4)}
            for i in range(5)
        }
    )
    monkeypatch.setattr(control, "ADMIN_SERVICES_MAX_DOMAINS", 3)
    monkeypatch.setattr(control, "ADMIN_SERVICES_MAX_PER_DOMAIN", 2)
    domains = await control.build_services_catalog(hass, descriptions={})
    assert len(domains) == 3
    assert all(len(d["services"]) == 2 for d in domains)


@pytest.mark.asyncio
async def test_services_catalog_fetches_descriptions_when_absent(
    make_hass, monkeypatch
) -> None:
    hass = _services_hass(make_hass)

    async def _fake_descriptions(_hass):
        return {"light": {"turn_on": {"description": "From the fetch path"}}}

    monkeypatch.setattr(control, "async_get_all_descriptions", _fake_descriptions)
    domains = await control.build_services_catalog(hass)
    light = {s["name"]: s for s in domains[0]["services"]}
    assert light["turn_on"]["description"] == "From the fetch path"


# ── snapshot paging math ─────────────────────────────────────────────────────


def test_page_state_snapshot_single_page_under_limit() -> None:
    entities = [{"entity_id": f"sensor.s{i}"} for i in range(10)]
    assert control.page_state_snapshot(entities, page_size=1500) == [entities]


def test_page_state_snapshot_empty_still_one_page() -> None:
    # An empty page 1 is meaningful (full:true clears the cloud cache).
    assert control.page_state_snapshot([], page_size=1500) == [[]]


def test_page_state_snapshot_exact_boundary() -> None:
    entities = [{"entity_id": f"sensor.s{i}"} for i in range(1500)]
    assert len(control.page_state_snapshot(entities, page_size=1500)) == 1


def test_page_state_snapshot_splits_over_limit() -> None:
    entities = [{"entity_id": f"sensor.s{i}"} for i in range(1501)]
    pages = control.page_state_snapshot(entities, page_size=1500)
    assert [len(p) for p in pages] == [1500, 1]
    # No entity lost or duplicated across pages.
    flat = [e["entity_id"] for page in pages for e in page]
    assert flat == [e["entity_id"] for e in entities]


def test_page_state_snapshot_many_pages() -> None:
    entities = list(range(32))
    pages = control.page_state_snapshot(entities, page_size=10)
    assert [len(p) for p in pages] == [10, 10, 10, 2]
