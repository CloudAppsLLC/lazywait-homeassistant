"""In-process executor for cloud-issued automation operations.

All reads + control use the in-process ``hass`` (no HA HTTP token exists). The
mechanism per op:

  list        — enumerate ``automation.*`` states (alias/mode/last_triggered).
  get_config  — read a single automation's full config from the automation
                component's in-memory config store, for the dashboard Edit
                builder.
  enable      — automation.turn_on   (service call)
  disable     — automation.turn_off
  trigger     — automation.trigger
  reload      — automation.reload
  upsert      — write the automation into the config store + reload.
  delete      — remove it from the config store + reload.

CRITICAL feasibility note (verified against the cloud design + camera.py): the
integration holds NO HA long-lived token, so we CANNOT call HA's authenticated
``/api/config/automation/config`` REST API (it would 401). We write through the
automation component's in-process config view instead. If the branch uses a
SPLIT config layout (``automation: !include_dir_*``) there is no single
``automations.yaml`` to upsert into — writing one would silently shadow/duplicate
— so we DETECT that and return ``HA_AUTOMATION_CONFIG_UNSUPPORTED`` (control
still works; only create/edit is refused) rather than a phantom success.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

# The file the automation component reads when configured the default way:
#   automation: !include automations.yaml
AUTOMATIONS_YAML = "automations.yaml"


def list_automations(hass: HomeAssistant) -> dict[str, Any]:
    """Return { automations: [{ id, entityId, alias, state, mode, lastTriggered }] }."""
    out: list[dict[str, Any]] = []
    for state in hass.states.async_all("automation"):
        attrs = state.attributes or {}
        out.append(
            {
                "id": attrs.get("id") or state.entity_id.split(".", 1)[1],
                "entityId": state.entity_id,
                "alias": attrs.get("friendly_name") or state.entity_id,
                "state": str(state.state),
                "mode": attrs.get("mode"),
                "lastTriggered": attrs.get("last_triggered"),
            }
        )
    return {"automations": out}


async def _call(hass: HomeAssistant, service: str, entity_id: str) -> dict[str, Any]:
    try:
        await hass.services.async_call(
            "automation", service, {"entity_id": entity_id}, blocking=False
        )
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("automation.%s on %s failed: %s", service, entity_id, err)
        return {"status": "error", "errorKey": "HA_COMMAND_FAILED"}
    return {"status": "ok"}


def _entity_id_for(automation_id: str) -> str:
    """Map a cloud-sent automation id to its entity_id (accepts either form)."""
    if automation_id.startswith("automation."):
        return automation_id
    return f"automation.{automation_id}"


async def set_enabled(
    hass: HomeAssistant, automation_id: str, enabled: bool
) -> dict[str, Any]:
    return await _call(hass, "turn_on" if enabled else "turn_off", _entity_id_for(automation_id))


async def trigger(hass: HomeAssistant, automation_id: str) -> dict[str, Any]:
    return await _call(hass, "trigger", _entity_id_for(automation_id))


async def reload(hass: HomeAssistant) -> dict[str, Any]:
    try:
        await hass.services.async_call("automation", "reload", {}, blocking=True)
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("automation.reload failed: %s", err)
        return {"status": "error", "errorKey": "HA_COMMAND_FAILED"}
    return {"status": "ok"}


def _automations_yaml_path(hass: HomeAssistant) -> str:
    return hass.config.path(AUTOMATIONS_YAML)


def _split_config_in_use(hass: HomeAssistant) -> bool:
    """Heuristic: the default layout has a writable automations.yaml at the
    config root. A split layout (!include_dir_*) won't, so create/edit can't
    target a single file — refuse rather than shadow-write."""
    path = _automations_yaml_path(hass)
    # If automations.yaml is missing entirely AND there are automation.* entities
    # loaded, the branch is almost certainly using a split/dir include.
    if not os.path.exists(path):
        has_entities = bool(hass.states.async_all("automation"))
        return has_entities
    return False


async def get_config(hass: HomeAssistant, automation_id: str) -> dict[str, Any]:
    """Read a single automation's full config for the Edit builder.

    Reads the automations.yaml store (the default-layout source). Returns
    { automation: { id, config } } or an error key.
    """
    if _split_config_in_use(hass):
        return {"status": "error", "errorKey": "HA_AUTOMATION_CONFIG_UNSUPPORTED"}
    raw_id = automation_id.split(".", 1)[1] if automation_id.startswith("automation.") else automation_id
    configs = await _read_yaml_list(hass)
    for item in configs:
        if str(item.get("id")) == str(raw_id):
            return {"status": "ok", "automation": {"id": raw_id, "config": item}}
    return {"status": "error", "errorKey": "HA_AUTOMATION_NOT_FOUND"}


async def upsert(
    hass: HomeAssistant, automation_id: str | None, config: dict[str, Any]
) -> dict[str, Any]:
    """Create (id None) or edit an automation in automations.yaml, then reload.

    HA allocates/keeps the id; the cloud never chooses it. An invalid config is
    caught on reload → HA_AUTOMATION_WRITE_FAILED, and the temp+rename write
    means a bad file never corrupts the existing automations.
    """
    if _split_config_in_use(hass):
        return {"status": "error", "errorKey": "HA_AUTOMATION_CONFIG_UNSUPPORTED"}

    # Defense-in-depth: reject a config that names a denylisted / non-allowlisted
    # service anywhere in its action/trigger/condition blocks (the hard-deny
    # invariant must hold for the automation path, not only direct call_service).
    from .control import automation_config_allowed

    if not automation_config_allowed(config):
        return {"status": "error", "errorKey": "HA_COMMAND_NOT_ALLOWED"}

    from homeassistant.util import uuid as uuid_util

    configs = await _read_yaml_list(hass)
    raw_id = (
        automation_id.split(".", 1)[1]
        if automation_id and automation_id.startswith("automation.")
        else automation_id
    )
    new_id = str(raw_id) if raw_id else uuid_util.random_uuid_hex()

    entry = dict(config)
    entry["id"] = new_id

    replaced = False
    for idx, item in enumerate(configs):
        if str(item.get("id")) == new_id:
            configs[idx] = entry
            replaced = True
            break
    if not replaced:
        configs.append(entry)

    ok = await _write_yaml_list(hass, configs)
    if not ok:
        return {"status": "error", "errorKey": "HA_AUTOMATION_WRITE_FAILED"}

    reloaded = await reload(hass)
    if reloaded.get("status") != "ok":
        return {"status": "error", "errorKey": "HA_AUTOMATION_WRITE_FAILED"}

    return {
        "status": "ok",
        "result": {"automationId": new_id, "entityId": f"automation.{new_id}"},
    }


async def delete(hass: HomeAssistant, automation_id: str) -> dict[str, Any]:
    if _split_config_in_use(hass):
        return {"status": "error", "errorKey": "HA_AUTOMATION_CONFIG_UNSUPPORTED"}
    raw_id = automation_id.split(".", 1)[1] if automation_id.startswith("automation.") else automation_id
    configs = await _read_yaml_list(hass)
    remaining = [c for c in configs if str(c.get("id")) != str(raw_id)]
    if len(remaining) == len(configs):
        return {"status": "error", "errorKey": "HA_AUTOMATION_NOT_FOUND"}
    ok = await _write_yaml_list(hass, remaining)
    if not ok:
        return {"status": "error", "errorKey": "HA_AUTOMATION_WRITE_FAILED"}
    await reload(hass)
    return {"status": "ok"}


async def _read_yaml_list(hass: HomeAssistant) -> list[dict[str, Any]]:
    """Read automations.yaml as a list of dicts (HA's automation store format)."""
    path = _automations_yaml_path(hass)

    def _read() -> list[dict[str, Any]]:
        if not os.path.exists(path):
            return []
        from homeassistant.util.yaml import load_yaml

        loaded = load_yaml(path)
        if isinstance(loaded, list):
            return [c for c in loaded if isinstance(c, dict)]
        return []

    return await hass.async_add_executor_job(_read)


async def _write_yaml_list(hass: HomeAssistant, configs: list[dict[str, Any]]) -> bool:
    """Atomically write automations.yaml (temp file + rename) so a failed write
    never corrupts the existing automations."""
    path = _automations_yaml_path(hass)

    def _write() -> bool:
        try:
            from homeassistant.util.yaml import dump

            tmp = f"{path}.lazywait.tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                handle.write(dump(configs))
            os.replace(tmp, path)
            return True
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("automations.yaml write failed: %s", err)
            return False

    return await hass.async_add_executor_job(_write)
