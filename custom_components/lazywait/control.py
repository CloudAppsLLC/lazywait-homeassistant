"""In-process executor for cloud-issued device commands + state snapshots.

Everything here runs against the in-process ``hass`` object — NO HA HTTP token
is involved (the integration never holds one; it only has the cloud bearer).
``hass.services.async_call`` and ``hass.states`` are the in-process APIs.

Security: the cloud already allowlist-checks every command, but this is an
INDEPENDENT second gate (the two could drift). A command is executed only when
its domain is not in ``HARD_DENY_DOMAINS`` — the curated
``DEVICE_CONTROL_ALLOWLIST`` is the cloud's write tier; anything outside it is
gated behind the cloud's delete tier, which HA cannot see, so HA's own gate is
the hard-deny backstop. The target is passed as the ``target`` kwarg (NOT merged
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
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.service import async_get_all_descriptions

from .const import (
    ADMIN_REGISTRY_MAX_AREAS,
    ADMIN_REGISTRY_MAX_DEVICES,
    ADMIN_REGISTRY_STRING_MAX_CHARS,
    ADMIN_SERVICE_DESCRIPTION_MAX_CHARS,
    ADMIN_SERVICES_MAX_DOMAINS,
    ADMIN_SERVICES_MAX_PER_DOMAIN,
    ADMIN_STATE_MAX_ENTITIES,
    ADMIN_STATE_SNAPSHOT_PAGE_SIZE,
    ATTRIBUTE_ALLOWLIST,
    DEVICE_CONTROL_ALLOWLIST,
    GENERIC_ATTRIBUTES,
    HARD_DENY_DOMAINS,
)

_LOGGER = logging.getLogger(__name__)


def _service_allowed(domain: str, service: str) -> bool:
    """Independent HA-side gate (mirrors the cloud, never trusts it).

    Widened (26.7.8): any domain NOT in HARD_DENY_DOMAINS is allowed. The
    curated DEVICE_CONTROL_ALLOWLIST is only a TIER marker for the cloud
    (curated = write tier, everything else = delete tier); HA cannot see
    tiers, so its own gate is the absolute hard-deny backstop.
    """
    if not domain or not service:
        return False
    return domain not in HARD_DENY_DOMAINS


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
    action/trigger/condition blocks that must never run (shell_command.*,
    homeassistant.restart, hassio.*). Reject the whole config if ANY embedded
    service is hard-denied — the hard-deny invariant must hold for the
    automation path too, not just direct call_service commands. Non-denied
    services outside the curated list pass (same widened rule as
    _service_allowed; the cloud already gates automation upsert at its
    delete tier)."""
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
    attributes can leak camera creds / GPS / tokens. Domains without an
    ATTRIBUTE_ALLOWLIST entry fall back to GENERIC_ATTRIBUTES (display-only
    metadata) instead of shipping nothing — ALL domains are reported now, and
    most only need a name/unit/icon to render."""
    out: dict[str, Any] = {}
    if not isinstance(attributes, dict):
        return out
    allowed = ATTRIBUTE_ALLOWLIST.get(domain)
    if allowed is None:
        allowed = GENERIC_ATTRIBUTES
    friendly = attributes.get("friendly_name")
    if isinstance(friendly, str):
        out["friendly_name"] = friendly
    for key in allowed:
        if key in attributes:
            out[key] = attributes[key]
    return out


def _get_registries(hass: HomeAssistant) -> tuple[Any, Any]:
    """Entity + device registries, or (None, None) when unavailable (early
    boot / bare test hass) — snapshots must degrade to null enrichment, never
    take the WS loop down."""
    try:
        return er.async_get(hass), dr.async_get(hass)
    except Exception:  # noqa: BLE001 - enrichment is best-effort
        return None, None


def _resolve_area_device(
    ent_reg: Any, dev_reg: Any, entity_id: str
) -> tuple[str | None, str | None]:
    """(area_id, device_id) for an entity: the entity-registry area override
    wins, else the owning device's area, else None."""
    if ent_reg is None:
        return None, None
    try:
        entry = ent_reg.async_get(entity_id)
    except Exception:  # noqa: BLE001
        return None, None
    if entry is None:
        return None, None
    device_id = entry.device_id or None
    area_id = entry.area_id or None
    if area_id is None and device_id and dev_reg is not None:
        try:
            device = dev_reg.async_get(device_id)
        except Exception:  # noqa: BLE001
            device = None
        if device is not None:
            area_id = device.area_id or None
    return area_id, device_id


def build_state_snapshot(
    hass: HomeAssistant, only_entity_ids: set[str] | None = None
) -> list[dict[str, Any]]:
    """Enumerate entities into the cloud's entity shape.

    ALL domains are reported (the old REPORTED_DOMAINS whitelist is gone);
    safety lives in attribute filtering (_filter_attributes). Each entity is
    enriched with area_id/device_id from the entity/device registries so the
    dashboard can group by area. ``controllable`` marks the curated write-tier
    domains (the dashboard renders inline controls only for those).

    Controllable entities sort first, then the list is trimmed to
    ADMIN_STATE_MAX_ENTITIES — on a monster install the entities the user can
    actually drive survive the cut.

    ``only_entity_ids`` narrows the build to the given ids — the debounced
    delta path in ws_client uses this to ship `full:false` batches.
    """
    ent_reg, dev_reg = _get_registries(hass)
    entities: list[dict[str, Any]] = []
    for state in hass.states.async_all():
        entity_id = state.entity_id
        if only_entity_ids is not None and entity_id not in only_entity_ids:
            continue
        domain = entity_id.split(".", 1)[0]
        area_id, device_id = _resolve_area_device(ent_reg, dev_reg, entity_id)
        entities.append(
            {
                "entity_id": entity_id,
                "domain": domain,
                "state": str(state.state),
                "attributes": _filter_attributes(domain, state.attributes),
                "controllable": domain in DEVICE_CONTROL_ALLOWLIST,
                "area_id": area_id,
                "device_id": device_id,
            }
        )
    # Stable controllable-first order, THEN the cap (contract A1).
    entities.sort(key=lambda e: not e["controllable"])
    return entities[:ADMIN_STATE_MAX_ENTITIES]


def page_state_snapshot(
    entities: list[dict[str, Any]],
    page_size: int = ADMIN_STATE_SNAPSHOT_PAGE_SIZE,
) -> list[list[dict[str, Any]]]:
    """Split a snapshot into WS pages. Always at least one page (an empty
    page 1 with `full:true` is meaningful — it clears the cloud cache)."""
    if len(entities) <= page_size:
        return [entities]
    return [entities[i : i + page_size] for i in range(0, len(entities), page_size)]


def _trim_str(value: Any) -> str | None:
    """Registry strings pass through as-is but capped (contract: 128 chars);
    non-strings become None rather than leaking odd types into the frame."""
    if not isinstance(value, str) or not value:
        return None
    return value[:ADMIN_REGISTRY_STRING_MAX_CHARS]


def build_registry_snapshot(hass: HomeAssistant) -> dict[str, list[dict[str, Any]]]:
    """Area + device registry metadata for the `registry_snapshot` frame.

    Devices report name_by_user || name (unnamed devices are skipped — the
    dashboard can't render them). Caps trim rather than fail. Cheap enough to
    resend on the 60s periodic tick (registries rarely change).
    """
    areas: list[dict[str, Any]] = []
    devices: list[dict[str, Any]] = []
    try:
        area_reg = ar.async_get(hass)
        for area in area_reg.areas.values():
            name = _trim_str(getattr(area, "name", None))
            if not name:
                continue
            areas.append({"id": area.id, "name": name})
            if len(areas) >= ADMIN_REGISTRY_MAX_AREAS:
                break
    except Exception:  # noqa: BLE001 - inventory is best-effort
        pass
    try:
        dev_reg = dr.async_get(hass)
        for device in dev_reg.devices.values():
            name = _trim_str(
                getattr(device, "name_by_user", None) or getattr(device, "name", None)
            )
            if not name:
                continue
            devices.append(
                {
                    "id": device.id,
                    "name": name,
                    "manufacturer": _trim_str(getattr(device, "manufacturer", None)),
                    "model": _trim_str(getattr(device, "model", None)),
                    "area_id": getattr(device, "area_id", None) or None,
                    "via_device_id": getattr(device, "via_device_id", None) or None,
                }
            )
            if len(devices) >= ADMIN_REGISTRY_MAX_DEVICES:
                break
    except Exception:  # noqa: BLE001
        pass
    return {"areas": areas, "devices": devices}


async def get_service_descriptions(hass: HomeAssistant) -> dict[str, Any]:
    """Fetch HA's full service-description map. Can be slow (it loads every
    integration's services.yaml) — ws_client caches the result per connection
    and feeds it back into build_services_catalog."""
    return await async_get_all_descriptions(hass)


async def build_services_catalog(
    hass: HomeAssistant, descriptions: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Domain/service name catalog for the `services_catalog` frame.

    Names come from hass.services.async_services(); descriptions (optional,
    trimmed, omitted when absent) from the pre-fetched map. HARD_DENY_DOMAINS
    are excluded entirely — the dashboard must never even offer them. No
    field/selector schemas in v1 of this frame. Caps trim rather than fail.
    """
    if descriptions is None:
        descriptions = await get_service_descriptions(hass)
    services_map = hass.services.async_services()
    domains: list[dict[str, Any]] = []
    for domain in sorted(services_map):
        if domain in HARD_DENY_DOMAINS:
            continue
        described = descriptions.get(domain) if isinstance(descriptions, dict) else None
        services: list[dict[str, Any]] = []
        for name in sorted(services_map[domain]):
            entry: dict[str, Any] = {"name": name}
            meta = described.get(name) if isinstance(described, dict) else None
            text = meta.get("description") if isinstance(meta, dict) else None
            if isinstance(text, str) and text.strip():
                entry["description"] = text.strip()[:ADMIN_SERVICE_DESCRIPTION_MAX_CHARS]
            services.append(entry)
            if len(services) >= ADMIN_SERVICES_MAX_PER_DOMAIN:
                break
        domains.append({"domain": domain, "services": services})
        if len(domains) >= ADMIN_SERVICES_MAX_DOMAINS:
            break
    return domains
