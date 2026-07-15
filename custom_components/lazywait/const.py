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

# Identifies this component build to the cloud /status heartbeat. The cloud
# gates the Smart Branch telemetry feature on >= 26.8.0 (telemetrySupport.ts).
INTEGRATION_VERSION = "26.7.24"
# ── Near-live camera snapshot loop ──────────────────────────────────────────
# A SEPARATE lightweight loop (not the 30s poll) captures a JPEG for each camera
# the dashboard is viewing NOW and posts it, giving a near-live view without
# WebRTC. Concurrency is capped to the (usually single) camera actually being
# watched so it can't hammer the NVR.
#
# This is the TARGET wall-clock period, enforced by a fixed-rate scheduler
# (sleep = max(0, period - work_time)) so cadence doesn't drift by the tick's
# work time. 0.4s targets ~2.5 fps; the real ceiling is the NVR still-grab cost
# (each tick pulls a fresh keyframe), so this is an upper bound, not a promise.
SNAPSHOT_LOOP_INTERVAL_SECONDS = 0.4
# Cameras captured per tick. In the grid, 1 slot goes to the MAIN (primary)
# camera every tick and the rest round-robin the thumbnail (secondary) set, so
# with 3 the main stays fast while 2 thumbnails refresh per tick (an 11-camera
# grid cycles every ~2s). Bounded low on purpose — each capture is a fresh NVR
# still-grab, so this caps the load a cheap NVR sees.
SNAPSHOT_MAX_CONCURRENT = 3
# Event types the component can emit to the cloud. Mirrors the cloud's
# discriminated union (absence | presence | device_state | sensor_reading).
EVENT_ABSENCE = "absence"
EVENT_PRESENCE = "presence"
EVENT_DEVICE_STATE = "device_state"
EVENT_SENSOR_READING = "sensor_reading"

# ── Smart Branch telemetry (spec §3.3, integration >= 26.8.0) ────────────────
# The cloud /config ships monitored_entities + report_interval_seconds +
# heartbeat_interval_seconds + significant_change; these are the local
# defaults/floors used when an older cloud omits them (see telemetry.py).
TELEMETRY_DEFAULT_REPORT_INTERVAL_SECONDS = 60
TELEMETRY_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 300
# Binary/non-numeric entities report ONLY state transitions, at most one per
# entity per this window — raw per-flip streams never leave the branch (the
# heartbeat reconciles the final state).
TELEMETRY_BINARY_MIN_INTERVAL_SECONDS = 60
# Events per POST /events batch. The cloud hard-caps the body at 500; 400
# leaves headroom so a batch assembled at the cap never bounces.
TELEMETRY_MAX_EVENTS_PER_BATCH = 400
# Outbox ceiling during an extended cloud outage — oldest readings drop first
# (mirrors the coordinator's absence-event buffer cap).
TELEMETRY_MAX_BUFFERED_EVENTS = 1000
# After a significant change wakes the flush loop, wait this long so
# co-occurring changes (temp + humidity in the same second) share one batch.
TELEMETRY_WAKE_COALESCE_SECONDS = 1.0
# The ONLY attributes a sensor_reading ships. Deliberately tighter than the
# admin-snapshot allowlist: these strings enter LLM contexts cloud-side
# (spec §6.1), so nothing that could carry a secret or an injection surface
# beyond a display name crosses the wire.
TELEMETRY_ATTRIBUTE_ALLOWLIST: set[str] = {
    "friendly_name",
    "device_class",
    "unit_of_measurement",
}

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

# How often the WS client pushes a full state snapshot (+ registry snapshot)
# even absent a change — the floor that heals the cloud cache after an apiv2
# restart or a missed delta. Driven by the per-connection periodic task in
# ws_client.py.
ADMIN_STATE_FULL_PUSH_SECONDS = 60

# State-changed deltas are batched and flushed this many seconds after the
# first change in a burst (trailing batch, NOT a resetting timer — a busy
# zigbee mesh must still flush every window instead of starving forever).
ADMIN_STATE_DELTA_DEBOUNCE_SECONDS = 2

# ── Snapshot size safety (mirror the cloud's haStateRegistryService caps) ───
# A snapshot larger than this is split into pages: page 1 `full:true` (resets
# the cloud cache), pages 2+ `full:false` (merged in) — one giant WS frame
# would trip the cloud's per-message size limit (512 KB maxPayload). At the
# observed ~340 bytes/entity (entity_id + generic attributes + two 32-hex
# registry ids) 700 entities ≈ 240 KB — half the cap, so page 1's heavier
# controllable entities still fit. Delta flushes page through the same helper.
ADMIN_STATE_SNAPSHOT_PAGE_SIZE = 700
# Hard entity ceiling per snapshot, trimmed AFTER controllable-first ordering
# so the entities the dashboard can actually drive survive the cut.
ADMIN_STATE_MAX_ENTITIES = 1900

# ── Inventory frame caps (registry_snapshot / services_catalog) ─────────────
# Trim, never fail: a monster install still gets a useful (partial) inventory.
ADMIN_REGISTRY_MAX_AREAS = 200
ADMIN_REGISTRY_MAX_DEVICES = 500
ADMIN_REGISTRY_STRING_MAX_CHARS = 128
ADMIN_SERVICES_MAX_DOMAINS = 150
ADMIN_SERVICES_MAX_PER_DOMAIN = 100
ADMIN_SERVICE_DESCRIPTION_MAX_CHARS = 140

# ── Control allowlists (mirror the cloud's haCommandAllowlist.ts) ───────────
# Curated {domain: {services}} the cloud drives at its normal (write) tier.
# Anything OUTSIDE this map but not hard-denied is still callable — the cloud
# gates those behind its delete tier; HA cannot see tiers, so its own gate is
# the HARD_DENY_DOMAINS backstop (see _service_allowed). This map also feeds
# the `controllable` flag on state snapshots.
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
    # Scripts/buttons run whatever the branch admin authored INSIDE HA — the
    # cloud only pulls the trigger, so these sit at the normal write tier.
    "script": {"turn_on", "turn_off", "toggle"},
    "button": {"press"},
    "input_button": {"press"},
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

# ALL entity domains are reported to the cloud state cache (the old
# REPORTED_DOMAINS whitelist is gone — the dashboard now renders the full
# inventory). Safety moved from domain filtering to attribute filtering:
# per-domain ATTRIBUTE_ALLOWLIST below, with GENERIC_ATTRIBUTES as the only
# fallback for unlisted domains. Raw HA attributes leak camera stream creds /
# GPS / access tokens — never ship them. friendly_name is always included on
# top of the per-domain set.
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
    "script": {"last_triggered", "mode"},
}

# The ONLY attributes shipped for domains without an ATTRIBUTE_ALLOWLIST entry.
# Deliberately display-only metadata — nothing here can carry a secret.
GENERIC_ATTRIBUTES: set[str] = {
    "friendly_name",
    "device_class",
    "unit_of_measurement",
    "icon",
}
