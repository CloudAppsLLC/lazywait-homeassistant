"""In-process camera discovery from the HA state machine.

`list_from_hass_states` is the reliable discovery path for Hikvision/NVR
channels that never auto-register as go2rtc streams. It reads `camera.*`
entities straight off `hass.states.async_all('camera')` — no token, no HTTP.

These tests use lightweight fakes (no homeassistant import) mirroring the
shape of a HA `State` (`.entity_id`, `.attributes`, `.state`) and a `hass`
object exposing `states.async_all`.
"""

from custom_components.lazywait.camera import list_from_hass_states


class _FakeState:
    def __init__(self, entity_id: str, friendly_name: str | None, state: str) -> None:
        self.entity_id = entity_id
        self.attributes = (
            {"friendly_name": friendly_name} if friendly_name is not None else {}
        )
        self.state = state


class _FakeStates:
    def __init__(self, states: list[_FakeState]) -> None:
        self._states = states
        self.requested_domain: str | None = None

    def async_all(self, domain: str | None = None) -> list[_FakeState]:
        self.requested_domain = domain
        return list(self._states)


class _FakeHass:
    def __init__(self, states: list[_FakeState]) -> None:
        self.states = _FakeStates(states)


def test_maps_camera_entities_to_picker_rows() -> None:
    hass = _FakeHass(
        [
            _FakeState("camera.nvr_channel_1", "Entrance", "idle"),
            _FakeState("camera.nvr_channel_2", "Back Door", "recording"),
        ]
    )
    cameras = list_from_hass_states(hass)
    assert cameras == [
        {"id": "camera.nvr_channel_1", "name": "Entrance", "online": True},
        {"id": "camera.nvr_channel_2", "name": "Back Door", "online": True},
    ]
    # Discovery filters by the camera domain via the stable public API.
    assert hass.states.requested_domain == "camera"


def test_unavailable_and_unknown_are_offline() -> None:
    hass = _FakeHass(
        [
            _FakeState("camera.a", "A", "unavailable"),
            _FakeState("camera.b", "B", "unknown"),
            _FakeState("camera.c", "C", "streaming"),
        ]
    )
    online = {c["id"]: c["online"] for c in list_from_hass_states(hass)}
    assert online == {"camera.a": False, "camera.b": False, "camera.c": True}


def test_falls_back_to_entity_id_when_no_friendly_name() -> None:
    hass = _FakeHass([_FakeState("camera.no_name", None, "idle")])
    cameras = list_from_hass_states(hass)
    assert cameras == [
        {"id": "camera.no_name", "name": "camera.no_name", "online": True}
    ]


def test_none_hass_returns_empty() -> None:
    assert list_from_hass_states(None) == []


def test_non_camera_entities_are_ignored() -> None:
    # Defensive: even if async_all returns a non-camera state, it's dropped.
    hass = _FakeHass(
        [
            _FakeState("binary_sensor.motion", "Motion", "on"),
            _FakeState("camera.real", "Real", "idle"),
        ]
    )
    cameras = list_from_hass_states(hass)
    assert cameras == [{"id": "camera.real", "name": "Real", "online": True}]


def test_state_machine_error_returns_empty() -> None:
    class _BoomStates:
        def async_all(self, domain: str | None = None) -> list[_FakeState]:
            raise RuntimeError("state machine unavailable")

    class _BoomHass:
        states = _BoomStates()

    assert list_from_hass_states(_BoomHass()) == []
