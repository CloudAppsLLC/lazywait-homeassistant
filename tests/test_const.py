"""Value assertions for the integration constants.

const.py is pure constants (no homeassistant import), so it imports cleanly in
a bare test environment.
"""

from custom_components.lazywait.const import (
    CONF_BASE_URL,
    CONF_BRANCH_ID,
    CONF_PAIRING_CODE,
    CONF_TOKEN,
    DEFAULT_BASE_URL,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DOMAIN,
    EVENT_ABSENCE,
    EVENT_DEVICE_STATE,
    EVENT_PRESENCE,
    INTEGRATION_VERSION,
)


def test_domain() -> None:
    assert DOMAIN == "lazywait"


def test_default_base_url() -> None:
    assert DEFAULT_BASE_URL == "https://apiv2.lazywait.com/v1"


def test_config_entry_keys() -> None:
    assert CONF_BASE_URL == "base_url"
    assert CONF_TOKEN == "enrollment_token"
    assert CONF_BRANCH_ID == "branch_id"


def test_pairing_code_key() -> None:
    assert CONF_PAIRING_CODE == "pairing_code"


def test_default_poll_interval_seconds() -> None:
    assert DEFAULT_POLL_INTERVAL_SECONDS == 30


def test_integration_version() -> None:
    assert INTEGRATION_VERSION == "1.0.0"


def test_event_types() -> None:
    assert EVENT_ABSENCE == "absence"
    assert EVENT_PRESENCE == "presence"
    assert EVENT_DEVICE_STATE == "device_state"
