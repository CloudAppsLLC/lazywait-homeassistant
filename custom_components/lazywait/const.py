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
INTEGRATION_VERSION = "26.6.3"
# Event types the component can emit to the cloud. Mirrors the cloud's
# discriminated union (absence | presence | device_state).
EVENT_ABSENCE = "absence"
EVENT_PRESENCE = "presence"
EVENT_DEVICE_STATE = "device_state"
