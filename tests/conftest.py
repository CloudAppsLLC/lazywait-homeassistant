"""Pytest bootstrap for the HA-free surface of the integration.

The package's own `custom_components/lazywait/__init__.py` imports homeassistant
at module top level, so a normal `from custom_components.lazywait.api import ...`
would execute the package `__init__` (importing homeassistant) and fail in a
bare environment. To stay HA-free we load modules directly from their file
paths via importlib and register them under the dotted names tests use,
WITHOUT executing the package `__init__`.

Two tiers of module:

  * Truly HA-free (`const.py`, `api.py`, `camera.py`, `camerasnapshot.py`) —
    load as-is.
  * HA-importing but unit-testable (`control.py`, `automations.py`,
    `ws_client.py`) — these only need a handful of homeassistant symbols
    (HomeAssistant/callback/ConfigEntry/EVENT_STATE_CHANGED and the
    entity/device/area registry + service-description helpers). When the real
    homeassistant package is absent we install MINIMAL stub modules first:
    the registry helpers' `async_get(hass)` read fake registries straight off
    the test's fake hass object (see the fixtures below). When homeassistant
    IS installed the stubs are skipped and the real package is used.

A synthetic parent package `custom_components.lazywait` (never executing the
real `__init__.py`) is registered so the modules' relative imports
(`from .const import ...`, `from . import control`) resolve from sys.modules.
"""

import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_PKG_DIR = _REPO_ROOT / "custom_components" / "lazywait"


def _install_ha_stubs() -> None:
    """Register minimal `homeassistant.*` stubs when the real package is absent."""
    if importlib.util.find_spec("homeassistant") is not None:
        return

    def _mod(name: str, is_pkg: bool = False) -> types.ModuleType:
        module = types.ModuleType(name)
        if is_pkg:
            module.__path__ = []  # type: ignore[attr-defined]
        sys.modules[name] = module
        return module

    root = _mod("homeassistant", is_pkg=True)

    core = _mod("homeassistant.core")

    class HomeAssistant:  # noqa: D401 - typing placeholder only
        """Stub of homeassistant.core.HomeAssistant (annotations only)."""

    def callback(func):
        return func

    core.HomeAssistant = HomeAssistant
    core.callback = callback

    const = _mod("homeassistant.const")
    const.EVENT_STATE_CHANGED = "state_changed"

    config_entries = _mod("homeassistant.config_entries")

    class ConfigEntry:  # noqa: D401 - typing placeholder only
        """Stub of homeassistant.config_entries.ConfigEntry."""

    config_entries.ConfigEntry = ConfigEntry

    helpers = _mod("homeassistant.helpers", is_pkg=True)

    er_mod = _mod("homeassistant.helpers.entity_registry")
    er_mod.async_get = lambda hass: hass.entity_registry
    dr_mod = _mod("homeassistant.helpers.device_registry")
    dr_mod.async_get = lambda hass: hass.device_registry
    ar_mod = _mod("homeassistant.helpers.area_registry")
    ar_mod.async_get = lambda hass: hass.area_registry

    service_mod = _mod("homeassistant.helpers.service")

    async def async_get_all_descriptions(hass):
        return hass.service_descriptions

    service_mod.async_get_all_descriptions = async_get_all_descriptions

    root.core = core
    root.const = const
    root.config_entries = config_entries
    root.helpers = helpers
    helpers.entity_registry = er_mod
    helpers.device_registry = dr_mod
    helpers.area_registry = ar_mod
    helpers.service = service_mod


def _install_parent_packages() -> None:
    """Synthetic `custom_components(.lazywait)` packages so relative imports in
    the loaded modules resolve WITHOUT running the HA-heavy real __init__.py."""
    for name, path in (
        ("custom_components", _REPO_ROOT / "custom_components"),
        ("custom_components.lazywait", _PKG_DIR),
    ):
        if name not in sys.modules:
            pkg = types.ModuleType(name)
            pkg.__path__ = [str(path)]  # type: ignore[attr-defined]
            sys.modules[name] = pkg


def _load(module_name: str, file_name: str) -> None:
    spec = importlib.util.spec_from_file_location(
        module_name, _PKG_DIR / file_name
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    # Bind the submodule onto the synthetic parent package so
    # `from . import <name>` finds it as an attribute too.
    parent_name, _, child = module_name.rpartition(".")
    parent = sys.modules.get(parent_name)
    if parent is not None:
        setattr(parent, child, module)


_install_ha_stubs()
_install_parent_packages()

# Register the dotted names the tests import, sourced straight from the files
# so the HA-importing package __init__ never runs.
_load("custom_components.lazywait.const", "const.py")
_load("custom_components.lazywait.api", "api.py")
# camera.py imports only aiohttp + stdlib (no homeassistant), so it loads HA-free.
_load("custom_components.lazywait.camera", "camera.py")
# camerasnapshot.py guards its homeassistant import behind TYPE_CHECKING (the
# real async_get_image is imported lazily inside the function), so it loads
# HA-free too — capture_snapshot degrades to None when the symbol is absent.
_load("custom_components.lazywait.camerasnapshot", "camerasnapshot.py")
# These import homeassistant symbols satisfied by the stubs above (or the real
# package when installed).
_load("custom_components.lazywait.control", "control.py")
_load("custom_components.lazywait.automations", "automations.py")
_load("custom_components.lazywait.ws_client", "ws_client.py")
# telemetry.py imports only stubbed homeassistant symbols at module level
# (homeassistant.helpers.event is imported lazily inside _resubscribe), so the
# pure decision logic (config parsing / buffer / batching) tests load HA-free.
_load("custom_components.lazywait.telemetry", "telemetry.py")


# ── Shared fakes (unit-level stand-ins for hass + the admin WS) ──────────────


class FakeState:
    """Mimics homeassistant.core.State for snapshot building."""

    def __init__(self, entity_id: str, state: str = "on", attributes: dict | None = None):
        self.entity_id = entity_id
        self.state = state
        self.attributes = attributes if attributes is not None else {}


class FakeStates:
    def __init__(self, states):
        self._states = list(states)

    def async_all(self, domain: str | None = None):
        if domain is None:
            return list(self._states)
        return [s for s in self._states if s.entity_id.startswith(f"{domain}.")]


class FakeRegistryEntry:
    """Entity-registry row: area override + owning device."""

    def __init__(self, entity_id: str, area_id: str | None = None, device_id: str | None = None):
        self.entity_id = entity_id
        self.area_id = area_id
        self.device_id = device_id


class FakeEntityRegistry:
    def __init__(self, entries=()):
        self._entries = {e.entity_id: e for e in entries}

    def async_get(self, entity_id: str):
        return self._entries.get(entity_id)


class FakeDevice:
    def __init__(
        self,
        device_id: str,
        name: str | None = None,
        name_by_user: str | None = None,
        manufacturer: str | None = None,
        model: str | None = None,
        area_id: str | None = None,
        via_device_id: str | None = None,
    ):
        self.id = device_id
        self.name = name
        self.name_by_user = name_by_user
        self.manufacturer = manufacturer
        self.model = model
        self.area_id = area_id
        self.via_device_id = via_device_id


class FakeDeviceRegistry:
    def __init__(self, devices=()):
        self.devices = {d.id: d for d in devices}

    def async_get(self, device_id: str):
        return self.devices.get(device_id)


class FakeArea:
    def __init__(self, area_id: str, name):
        self.id = area_id
        self.name = name


class FakeAreaRegistry:
    def __init__(self, areas=()):
        self.areas = {a.id: a for a in areas}


class FakeServiceRegistry:
    """Mimics hass.services — async_services() name map only."""

    def __init__(self, services: dict | None = None):
        self._services = services or {}

    def async_services(self):
        return self._services


class FakeBus:
    """Mimics hass.bus.async_listen incl. the returned unsubscribe callable."""

    def __init__(self):
        self.listeners = []
        self.unsubscribe_count = 0

    def async_listen(self, event_type, listener):
        entry = (event_type, listener)
        self.listeners.append(entry)

        def _unsub():
            if entry in self.listeners:
                self.listeners.remove(entry)
                self.unsubscribe_count += 1

        return _unsub


class FakeConfig:
    def as_dict(self):
        return {"version": "2026.6.0"}


class FakeHass:
    """Bare-minimum hass. Registries are OPTIONAL — omitting one exercises the
    graceful null-enrichment paths (control catches the AttributeError)."""

    def __init__(
        self,
        states=(),
        entity_registry=None,
        device_registry=None,
        area_registry=None,
        services=None,
        service_descriptions=None,
    ):
        self.states = FakeStates(states)
        self.bus = FakeBus()
        self.config = FakeConfig()
        self.services = FakeServiceRegistry(services)
        if entity_registry is not None:
            self.entity_registry = entity_registry
        if device_registry is not None:
            self.device_registry = device_registry
        if area_registry is not None:
            self.area_registry = area_registry
        if service_descriptions is not None:
            self.service_descriptions = service_descriptions


class FakeWs:
    """Captures frames sent on the admin socket, parsed back from JSON."""

    def __init__(self):
        self.sent = []
        self.closed = False
        self.close_code = None

    async def send_str(self, data: str) -> None:
        if self.closed:
            raise RuntimeError("ws closed")
        self.sent.append(json.loads(data))

    async def close(self) -> None:
        self.closed = True


class FakeEvent:
    """EVENT_STATE_CHANGED bus event carrying just the entity_id."""

    def __init__(self, entity_id: str | None):
        self.data = {"entity_id": entity_id} if entity_id else {}


@pytest.fixture
def make_hass():
    """Factory for a FakeHass (kwargs pass through to the constructor)."""

    def _make(**kwargs) -> FakeHass:
        return FakeHass(**kwargs)

    return _make


@pytest.fixture
def fake_ws() -> FakeWs:
    return FakeWs()
