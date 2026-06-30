"""Constants for the LazyWait Home Assistant integration."""

from __future__ import annotations

DOMAIN = "lazywait"

# Default cloud entrypoint. The branch admin can override the base URL in the
# config flow (e.g. for a white-label partner host), but this is the canonical
# production /v1 entrypoint. It must match the base URL the cloud returns from
# /pair so HA stores a single source of truth.
DEFAULT_BASE_URL = "https://apiv2.lazywait.com/v1"

# Config-entry data keys.
CONF_BASE_URL = "base_url"
CONF_TOKEN = "enrollment_token"
CONF_BRANCH_ID = "branch_id"

# Pairing-code field key in the config flow form.
CONF_PAIRING_CODE = "pairing_code"

# How often the coordinator polls /config + flushes the event buffer. The cloud
# config carries its own pollIntervalSeconds; this is only the floor used before
# the first successful config fetch.
DEFAULT_POLL_INTERVAL_SECONDS = 30

# Identifies this component build to the cloud /status heartbeat.
INTEGRATION_VERSION = "26.6.6"
# Event types the component can emit to the cloud. Mirrors the cloud's
# discriminated union (absence | presence | device_state).
EVENT_ABSENCE = "absence"
EVENT_PRESENCE = "presence"
EVENT_DEVICE_STATE = "device_state"

# ── Admin control: persistent outbound WebSocket ────────────────────────────
# HA opens ONE persistent outbound WebSocket to the cloud for near-instant
# device/automation control (the cloud can never dial in — NAT/outbound-only).
# The path is appended to the ws/wss form of CONF_BASE_URL.
ADMIN_WS_PATH = "/integrations/home-assistant/admin/ws"

# Reconnect backoff: exponential 1→2→4…→cap, with jitter, after a transient
# drop. A 401 at the HTTP upgrade handshake is NOT retried on this schedule — it
# means a dead/rotated token and triggers HA's reauth flow instead.
ADMIN_WS_BACKOFF_START_SECONDS = 1
ADMIN_WS_BACKOFF_CAP_SECONDS = 30

# How often the WS client pushes a full state snapshot even absent a change
# (a floor; deltas are pushed on change, debounced).
ADMIN_STATE_FULL_PUSH_SECONDS = 60

# ── Control allowlists (mirror the cloud's haCommandAllowlist.ts) ───────────
# Positive allowlist of {domain: {services}} the cloud may drive. HA re-checks
# this locally before async_call — a second independent gate so a widened cloud
# allowlist still can't run a service HA didn't sanction.
DEVICE_CONTROL_ALLOWLIST: dict[str, set[str]] = {
    "light": {"turn_on", "turn_off", "toggle"},
    "switch": {"turn_on", "turn_off", "toggle"},
    "fan": {"turn_on", "turn_off", "toggle", "set_percentage"},
    "cover": {"open_cover", "close_cover", "stop_cover", "set_cover_position"},
    "climate": {"set_temperature", "set_hvac_mode", "turn_on", "turn_off"},
    "media_player": {"turn_on", "turn_off", "media_play", "media_pause", "volume_set"},
    "scene": {"turn_on"},
    "input_boolean": {"turn_on", "turn_off", "toggle"},
    "humidifier": {"turn_on", "turn_off"},
    "vacuum": {"start", "pause", "return_to_base"},
    # High-blast-radius — the cloud gates these behind a higher permission; HA
    # still allows them only via this explicit map (never a wildcard).
    "lock": {"lock", "unlock"},
    "alarm_control_panel": {"alarm_arm_home", "alarm_arm_away", "alarm_disarm"},
}

# Automation ops the cloud may drive.
AUTOMATION_OP_ALLOWLIST: set[str] = {
    "list",
    "get_config",
    "enable",
    "disable",
    "trigger",
    "reload",
    "upsert",
    "delete",
}

# Hard denylist — domains NEVER callable, independent of the allowlist. A
# defense-in-depth backstop so a cloud bug can't reach host/supervisor/
# script-execution services.
HARD_DENY_DOMAINS: set[str] = {
    "homeassistant",
    "hassio",
    "shell_command",
    "python_script",
    "recorder",
    "backup",
    "system_log",
    "persistent_notification",
}

# Domains whose entities are reported to the cloud state cache (controllable +
# read-only sensors useful as automation triggers). Anything else is omitted.
REPORTED_DOMAINS: set[str] = set(DEVICE_CONTROL_ALLOWLIST.keys()) | {
    "binary_sensor",
    "sensor",
    "automation",
    "person",
    "device_tracker",
}

# Per-domain attribute allowlist for the state snapshot. Raw HA attributes leak
# camera stream creds / GPS / access tokens — only these are pushed to the
# cloud. friendly_name is always included on top of the per-domain set.
ATTRIBUTE_ALLOWLIST: dict[str, set[str]] = {
    "light": {"brightness", "color_temp", "rgb_color", "supported_color_modes"},
    "fan": {"percentage", "preset_mode"},
    "cover": {"current_position", "current_tilt_position"},
    "climate": {
        "current_temperature",
        "temperature",
        "hvac_mode",
        "hvac_modes",
        "hvac_action",
    },
    "media_player": {"volume_level", "media_title", "source"},
    "humidifier": {"humidity", "current_humidity"},
    "vacuum": {"battery_level", "status"},
    "sensor": {"unit_of_measurement", "device_class"},
    "binary_sensor": {"device_class"},
    "alarm_control_panel": {"code_format"},
    "automation": {"last_triggered", "mode"},
}
