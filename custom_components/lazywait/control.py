"""In-process executor for cloud-issued device commands + state snapshots.

Everything here runs against the in-process ``hass`` object — NO HA HTTP token
is involved (the integration never holds one; it only has the cloud bearer).
``hass.services.async_call`` and ``hass.states`` are the in-process APIs.

Security: the cloud already allowlist-checks every command, but this is an
INDEPENDENT second gate (the two could drift). A command is executed only when
its {domain, service} is in ``DEVICE_CONTROL_ALLOWLIST`` AND its domain is not in
``HARD_DENY_DOMAINS``. The target is passed as the ``target`` kwarg (NOT merged
into data — merging breaks ``area_id``/``device_id`` resolution). Calls are
non-blocking (``blocking=False``): we ack on dispatch and the truth is reflected
back to the cloud via the next state snapshot, so a slow zwave/zigbee actuator
can't blow the command deadline and revert an optimistic dashboard toggle that
actually succeeded.
"""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.core import HomeAssistant

from .const import (
    ATTRIBUTE_ALLOWLIST,
    DEVICE_CONTROL_ALLOWLIST,
    HARD_DENY_DOMAINS,
    REPORTED_DOMAINS,
)

_LOGGER = logging.getLogger(__name__)


def _service_allowed(domain: str, service: str) -> bool:
    """Independent HA-side allowlist check (mirrors the cloud, never trusts it)."""
    if domain in HARD_DENY_DOMAINS:
        return False
    return service in DEVICE_CONTROL_ALLOWLIST.get(domain, set())


def _iter_service_refs(node: Any):
    """Yield every service string ('domain.service') referenced anywhere in an
    automation config tree. HA actions name a service under the 'service' key
    (legacy) or 'action' key (new schema), and nest inside choose/if/repeat/
    parallel/sequence/then/else branches — so we recurse through the whole
    structure rather than only the top-level action list."""
    if isinstance(node, dict):
        for key in ("service", "action"):
            val = node.get(key)
            # 'action' can be a service string OR a nested block; only treat a
            # 'domain.service' string as a service reference.
            if isinstance(val, str) and "." in val:
                yield val
        for value in node.values():
            yield from _iter_service_refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_service_refs(item)


def automation_config_allowed(config: Any) -> bool:
    """Defense-in-depth: an automation config may name services inside its
    action/trigger/condition blocks that the device-call allowlist would never
    permit (shell_command.*, homeassistant.restart, hassio.*). Reject the whole
    config if ANY embedded service is denylisted or outside the device
    allowlist — the hard-deny invariant must hold for the automation path too,
    not just direct call_service commands."""
    for ref in _iter_service_refs(config):
        domain, _, service = ref.partition(".")
        if not domain or not service:
            continue
        if not _service_allowed(domain, service):
            return False
    return True


async def call_device_service(
    hass: HomeAssistant, command: dict[str, Any]
) -> dict[str, Any]:
    """Execute a call_service command. Returns a result dict for command_result.

    command: { domain, service, target: {entity_id|device_id|area_id}, data }
    Returns: { status: 'ok'|'error', errorKey?: str }
    """
    domain = str(command.get("domain") or "")
    service = str(command.get("service") or "")
    target = command.get("target") or {}
    data = command.get("data") or {}

    if not domain or not service:
        return {"status": "error", "errorKey": "HA_COMMAND_FAILED"}
    if not _service_allowed(domain, service):
        return {"status": "error", "errorKey": "HA_COMMAND_NOT_ALLOWED"}
    if not isinstance(target, dict) or not (
        target.get("entity_id") or target.get("device_id") or target.get("area_id")
    ):
        return {"status": "error", "errorKey": "HA_COMMAND_FAILED"}

    try:
        await hass.services.async_call(
            domain,
            service,
            dict(data) if isinstance(data, dict) else {},
            blocking=False,
            target=dict(target),
        )
    except Exception as err:  # noqa: BLE001 - surface any HA error as a result
        _LOGGER.debug("device service call failed: %s.%s: %s", domain, service, err)
        return {"status": "error", "errorKey": "HA_SERVICE_REJECTED"}

    return {"status": "ok"}


def _filter_attributes(domain: str, attributes: Any) -> dict[str, Any]:
    """Keep only friendly_name + the per-domain allowlisted attributes — raw HA
    attributes can leak camera creds / GPS / tokens."""
    out: dict[str, Any] = {}
    if not isinstance(attributes, dict):
        return out
    friendly = attributes.get("friendly_name")
    if isinstance(friendly, str):
        out["friendly_name"] = friendly
    for key in ATTRIBUTE_ALLOWLIST.get(domain, set()):
        if key in attributes:
            out[key] = attributes[key]
    return out


def build_state_snapshot(hass: HomeAssistant) -> list[dict[str, Any]]:
    """Enumerate reported-domain entities into the cloud's entity shape.

    Only ``REPORTED_DOMAINS`` are included; attributes are per-domain allowlisted.
    ``controllable`` is true when the domain is in the device-control allowlist
    (the dashboard renders a control affordance only for those).
    """
    entities: list[dict[str, Any]] = []
    for state in hass.states.async_all():
        entity_id = state.entity_id
        domain = entity_id.split(".", 1)[0]
        if domain not in REPORTED_DOMAINS:
            continue
        entities.append(
            {
                "entity_id": entity_id,
                "domain": domain,
                "state": str(state.state),
                "attributes": _filter_attributes(domain, state.attributes),
                "controllable": domain in DEVICE_CONTROL_ALLOWLIST,
            }
        )
    return entities
