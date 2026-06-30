"""HA-free tests for the admin-control surface.

Only the pure pieces are exercised here (the conftest loads const.py + api.py
without executing the HA-importing package __init__): the WS URL derivation and
the allowlist/denylist constants. The in-process executors (control.py /
automations.py / ws_client.py) import homeassistant and are covered by the HA
test environment, not this bare one.
"""

import aiohttp
import pytest

from custom_components.lazywait.api import LazyWaitApiClient
from custom_components.lazywait.const import (
    ADMIN_WS_PATH,
    AUTOMATION_OP_ALLOWLIST,
    DEVICE_CONTROL_ALLOWLIST,
    HARD_DENY_DOMAINS,
)


@pytest.mark.asyncio
async def test_ws_url_https_to_wss() -> None:
    async with aiohttp.ClientSession() as session:
        client = LazyWaitApiClient("https://apiv2.lazywait.com/v1", session, token="t")
        url = client.ws_url(ADMIN_WS_PATH)
        assert url == f"wss://apiv2.lazywait.com/v1{ADMIN_WS_PATH}"


@pytest.mark.asyncio
async def test_ws_url_http_to_ws() -> None:
    async with aiohttp.ClientSession() as session:
        client = LazyWaitApiClient("http://localhost:8080/v1", session, token="t")
        url = client.ws_url(ADMIN_WS_PATH)
        assert url == f"ws://localhost:8080/v1{ADMIN_WS_PATH}"


def test_device_allowlist_has_core_domains() -> None:
    assert "turn_on" in DEVICE_CONTROL_ALLOWLIST["light"]
    assert "unlock" in DEVICE_CONTROL_ALLOWLIST["lock"]
    assert "alarm_disarm" in DEVICE_CONTROL_ALLOWLIST["alarm_control_panel"]


def test_hard_denylist_blocks_dangerous_domains() -> None:
    for domain in ("homeassistant", "hassio", "shell_command", "python_script"):
        assert domain in HARD_DENY_DOMAINS
        # A denied domain must never appear in the positive allowlist.
        assert domain not in DEVICE_CONTROL_ALLOWLIST


def test_automation_ops_complete() -> None:
    for op in ("list", "enable", "disable", "trigger", "upsert", "delete"):
        assert op in AUTOMATION_OP_ALLOWLIST
