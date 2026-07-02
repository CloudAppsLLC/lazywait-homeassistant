"""Unit tests for the admin-WS push additions (26.7.8) in ws_client.py:

  * hello capabilities gained inventory / services_catalog / scripts
  * connect + resync push state_snapshot → registry_snapshot → services_catalog
  * state snapshots page when huge (page 1 full:true, pages 2+ full:false)
  * EVENT_STATE_CHANGED deltas batch behind the debounce window (full:false)
  * per-connection helpers (listener, periodic task) never leak or duplicate
  * the services-description map is fetched once per connection

Everything runs against the conftest fakes — no aiohttp socket, no real HA.
"""

import asyncio
from types import SimpleNamespace

import pytest

from conftest import FakeEvent, FakeState, FakeWs
from custom_components.lazywait import const, control, ws_client


def _make_socket(hass) -> ws_client.LazyWaitAdminSocket:
    return ws_client.LazyWaitAdminSocket(
        hass, SimpleNamespace(), SimpleNamespace(), "branch-1"
    )


@pytest.mark.asyncio
async def test_hello_capabilities_advertise_inventory(make_hass, fake_ws) -> None:
    socket = _make_socket(make_hass())
    await socket._send_hello(fake_ws)
    (frame,) = fake_ws.sent
    assert frame["type"] == "hello"
    caps = frame["capabilities"]
    assert caps["inventory"] is True
    assert caps["services_catalog"] is True
    assert caps["scripts"] is True
    assert caps["device_control"] is True


@pytest.mark.asyncio
async def test_send_inventory_order_and_shapes(make_hass, fake_ws) -> None:
    hass = make_hass(
        states=[FakeState("light.kitchen")],
        services={"light": {"turn_on": object()}},
        service_descriptions={},
    )
    socket = _make_socket(hass)
    await socket._send_inventory(fake_ws)
    types = [f["type"] for f in fake_ws.sent]
    assert types == ["state_snapshot", "registry_snapshot", "services_catalog"]
    state, registry, services = fake_ws.sent
    assert all(f["branchId"] == "branch-1" and f["v"] == 1 for f in fake_ws.sent)
    assert state["full"] is True and state["page"] == 1 and state["pages"] == 1
    assert state["entities"][0]["entity_id"] == "light.kitchen"
    assert registry["areas"] == [] and registry["devices"] == []
    assert services["domains"] == [{"domain": "light", "services": [{"name": "turn_on"}]}]


@pytest.mark.asyncio
async def test_resync_request_resends_all_three(make_hass, fake_ws) -> None:
    hass = make_hass(states=[FakeState("light.kitchen")], service_descriptions={})
    socket = _make_socket(hass)
    await socket._handle_text(fake_ws, '{"type": "resync_request"}')
    assert [f["type"] for f in fake_ws.sent] == [
        "state_snapshot",
        "registry_snapshot",
        "services_catalog",
    ]


@pytest.mark.asyncio
async def test_state_snapshot_pages_when_huge(make_hass, fake_ws) -> None:
    page_size = const.ADMIN_STATE_SNAPSHOT_PAGE_SIZE
    hass = make_hass(states=[FakeState(f"sensor.s{i}") for i in range(page_size + 100)])
    socket = _make_socket(hass)
    await socket._send_state_snapshot(fake_ws)
    assert len(fake_ws.sent) == 2
    first, second = fake_ws.sent
    assert first["full"] is True and first["page"] == 1 and first["pages"] == 2
    assert len(first["entities"]) == page_size
    assert second["full"] is False and second["page"] == 2 and second["pages"] == 2
    assert len(second["entities"]) == 100


@pytest.mark.asyncio
async def test_delta_flush_pages_when_huge(make_hass, fake_ws, monkeypatch) -> None:
    # A bulk update dirtying more entities than one frame safely carries must
    # ship as MULTIPLE full:false frames — one giant delta frame would trip the
    # cloud's 512 KB maxPayload and kill the socket.
    monkeypatch.setattr(ws_client, "ADMIN_STATE_DELTA_DEBOUNCE_SECONDS", 0)
    page_size = const.ADMIN_STATE_SNAPSHOT_PAGE_SIZE
    count = page_size + 50
    hass = make_hass(states=[FakeState(f"sensor.s{i}") for i in range(count)])
    socket = _make_socket(hass)
    socket._ws = fake_ws
    for i in range(count):
        socket._on_state_changed(FakeEvent(f"sensor.s{i}"))
    await socket._delta_flush_task
    assert len(fake_ws.sent) == 2
    assert all(f["type"] == "state_snapshot" and f["full"] is False for f in fake_ws.sent)
    assert [len(f["entities"]) for f in fake_ws.sent] == [page_size, 50]


@pytest.mark.asyncio
async def test_delta_flush_reflushes_changes_landed_mid_send(
    make_hass, fake_ws, monkeypatch
) -> None:
    # A change arriving while a batch is mid-send must ship in a follow-up
    # window, not sit until the 60s periodic push.
    monkeypatch.setattr(ws_client, "ADMIN_STATE_DELTA_DEBOUNCE_SECONDS", 0)
    hass = make_hass(states=[FakeState("light.a"), FakeState("switch.b")])
    socket = _make_socket(hass)
    socket._ws = fake_ws
    original_send = fake_ws.send_str

    async def send_and_inject(payload: str) -> None:
        await original_send(payload)
        # Simulate a state change landing during the send await.
        if len(fake_ws.sent) == 1:
            socket._pending_deltas.add("switch.b")

    fake_ws.send_str = send_and_inject
    socket._on_state_changed(FakeEvent("light.a"))
    await socket._delta_flush_task
    assert [
        {e["entity_id"] for e in f["entities"]} for f in fake_ws.sent
    ] == [{"light.a"}, {"switch.b"}]


@pytest.mark.asyncio
async def test_delta_debounce_batches_changes(make_hass, fake_ws, monkeypatch) -> None:
    monkeypatch.setattr(ws_client, "ADMIN_STATE_DELTA_DEBOUNCE_SECONDS", 0)
    hass = make_hass(states=[FakeState("light.a"), FakeState("switch.b")])
    socket = _make_socket(hass)
    socket._ws = fake_ws
    # A burst: three events, one for an entity that no longer exists.
    socket._on_state_changed(FakeEvent("light.a"))
    first_task = socket._delta_flush_task
    socket._on_state_changed(FakeEvent("switch.b"))
    socket._on_state_changed(FakeEvent("sensor.gone"))
    # The burst shares ONE flush task (no task per event).
    assert socket._delta_flush_task is first_task
    await first_task
    (frame,) = fake_ws.sent
    assert frame["type"] == "state_snapshot"
    assert frame["full"] is False
    assert {e["entity_id"] for e in frame["entities"]} == {"light.a", "switch.b"}
    # A later burst gets its own flush + frame.
    socket._on_state_changed(FakeEvent("light.a"))
    assert socket._delta_flush_task is not first_task
    await socket._delta_flush_task
    assert len(fake_ws.sent) == 2


@pytest.mark.asyncio
async def test_delta_event_without_entity_id_ignored(make_hass, fake_ws) -> None:
    socket = _make_socket(make_hass())
    socket._ws = fake_ws
    socket._on_state_changed(FakeEvent(None))
    assert socket._pending_deltas == set()
    assert socket._delta_flush_task is None


@pytest.mark.asyncio
async def test_push_helpers_no_duplicates_and_clean_teardown(
    make_hass, fake_ws
) -> None:
    hass = make_hass()
    socket = _make_socket(hass)
    socket._start_push_helpers(fake_ws)
    assert len(hass.bus.listeners) == 1
    first_periodic = socket._periodic_task
    # A second start (reconnect) must NOT stack a second listener/task.
    socket._start_push_helpers(fake_ws)
    assert len(hass.bus.listeners) == 1
    assert socket._periodic_task is not first_periodic
    await asyncio.gather(first_periodic, return_exceptions=True)
    assert first_periodic.cancelled()
    # Teardown removes the listener and cancels the running task.
    second_periodic = socket._periodic_task
    socket._pending_deltas.add("light.a")
    socket._stop_push_helpers()
    assert hass.bus.listeners == []
    assert socket._periodic_task is None
    assert socket._pending_deltas == set()
    await asyncio.gather(second_periodic, return_exceptions=True)
    assert second_periodic.cancelled()


@pytest.mark.asyncio
async def test_periodic_full_push_sends_state_and_registry(
    make_hass, fake_ws, monkeypatch
) -> None:
    monkeypatch.setattr(ws_client, "ADMIN_STATE_FULL_PUSH_SECONDS", 0.001)
    hass = make_hass(states=[FakeState("light.a")])
    socket = _make_socket(hass)
    socket._start_push_helpers(fake_ws)
    try:
        await asyncio.sleep(0.05)
    finally:
        socket._stop_push_helpers()
    types = {f["type"] for f in fake_ws.sent}
    assert types == {"state_snapshot", "registry_snapshot"}


@pytest.mark.asyncio
async def test_stop_tears_down_helpers_and_socket(make_hass, fake_ws) -> None:
    hass = make_hass()
    socket = _make_socket(hass)
    socket._ws = fake_ws
    socket._start_push_helpers(fake_ws)
    await socket.stop()
    assert socket._stopped is True
    assert fake_ws.closed is True
    assert hass.bus.listeners == []
    assert socket._periodic_task is None


@pytest.mark.asyncio
async def test_service_descriptions_cached_per_connection(
    make_hass, fake_ws, monkeypatch
) -> None:
    hass = make_hass(services={"light": {"turn_on": object()}})
    socket = _make_socket(hass)
    calls = 0

    async def _count_descriptions(_hass):
        nonlocal calls
        calls += 1
        return {}

    monkeypatch.setattr(control, "get_service_descriptions", _count_descriptions)
    await socket._send_services_catalog(fake_ws)
    await socket._send_services_catalog(fake_ws)
    assert calls == 1
    assert len(fake_ws.sent) == 2
