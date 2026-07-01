"""LazyWaitAdminSocket — the persistent OUTBOUND admin-control WebSocket.

HA is outbound-only behind NAT, so the cloud can never dial in. This client
opens ONE long-lived WebSocket to the cloud, authenticated by the SAME
enrollment bearer the HTTP path uses, and:

  * sends a ``hello`` frame (branch + versions + capabilities) on connect,
  * pushes a full ``state_snapshot`` of the branch's entities on connect,
  * receives ``command`` frames, executes them in-process (control.py /
    automations.py), and posts a ``command_result`` back on the same socket,
  * answers the cloud's WS-level pings (aiohttp ``heartbeat``) to keep the
    half-open detection honest.

Lifecycle: started as one asyncio.Task in async_setup_entry and cancelled in
async_unload_entry. It NEVER raises out of the task (an unraised exception in a
bare task is swallowed by asyncio and would silently kill control) — a 401 at
the upgrade handshake explicitly kicks off HA's reauth instead of a tight
reconnect storm; any other drop reconnects with exponential backoff + jitter.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
import logging
import random
from typing import Any

import aiohttp
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .api import LazyWaitApiClient
from .const import (
    ADMIN_WS_BACKOFF_CAP_SECONDS,
    ADMIN_WS_BACKOFF_START_SECONDS,
    ADMIN_WS_PATH,
    INTEGRATION_VERSION,
)

_LOGGER = logging.getLogger(__name__)


def _json_default(obj: Any) -> Any:
    """json.dumps fallback for values HA embeds that aren't natively JSON.

    HA entity attributes + automation configs carry datetime/date/time objects
    (e.g. automation `last_triggered`, sensor timestamps). Plain json.dumps raises
    'Object of type datetime is not JSON serializable', which was crashing the
    admin WS on EVERY connect at _send_full_snapshot → the control socket never
    stayed up. Serialize temporal types as ISO strings and anything else as its
    string form so a stray unexpected type can never take the socket down."""
    if isinstance(obj, (_dt.datetime, _dt.date, _dt.time)):
        return obj.isoformat()
    if isinstance(obj, _dt.timedelta):
        return obj.total_seconds()
    if isinstance(obj, set):
        return list(obj)
    return str(obj)


def _dumps(payload: Any) -> str:
    """json.dumps that never raises on HA's datetime-bearing payloads."""
    return json.dumps(payload, default=_json_default)


# aiohttp WS heartbeat (seconds) — auto-answers the cloud's 25s WS ping and
# detects a half-open socket.
_WS_HEARTBEAT = 25

# Cloud close codes (mirror haConnectionHubService).
_CLOSE_SUPERSEDED = 4001
_CLOSE_BRANCH_MISMATCH = 4002


class LazyWaitAdminSocket:
    """Owns the admin WebSocket connection + reconnect loop for one branch."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        client: LazyWaitApiClient,
        branch_id: str,
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._client = client
        self._branch_id = branch_id
        self._stopped = False
        self._ws: aiohttp.ClientWebSocketResponse | None = None

    @property
    def is_connected(self) -> bool:
        """True while the socket is open (the coordinator skips its HTTP
        command-drain fallback when this is true)."""
        return self._ws is not None and not self._ws.closed

    async def run(self) -> None:
        """Connect-and-serve loop with backoff. Returns only when stopped."""
        # INFO — proves the bare task actually started running. A bare
        # asyncio.Task that raises before any log line swallows the error
        # silently; this line rules that out (if it's absent, the task never
        # got scheduled or the coroutine failed at creation).
        _LOGGER.info("Admin WS run loop started (branch %s)", self._branch_id)
        backoff = ADMIN_WS_BACKOFF_START_SECONDS
        while not self._stopped:
            try:
                await self._connect_and_serve()
                # Clean close (e.g. superseded) → reconnect promptly.
                backoff = ADMIN_WS_BACKOFF_START_SECONDS
            except aiohttp.WSServerHandshakeError as err:
                if err.status == 401:
                    # Dead/rotated token: an unraised ConfigEntryAuthFailed from a
                    # bare task is swallowed, so trigger reauth explicitly and stop
                    # reconnecting (no tight 401 storm) until reauth completes.
                    _LOGGER.warning("Admin WS auth rejected (401); starting reauth")
                    self._entry.async_start_reauth(self._hass)
                    return
                _LOGGER.debug("Admin WS handshake failed (%s); backing off", err.status)
            except _BranchMismatch:
                _LOGGER.error("Admin WS branch mismatch; stopping")
                return
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - loop must never die
                # WARNING (not debug) so a persistent connect failure is visible
                # in the HA log instead of silently backing off forever. Includes
                # the exception type so a TLS/DNS/handshake cause is identifiable.
                _LOGGER.warning(
                    "Admin WS connect/serve failed (%s: %s); backing off",
                    type(err).__name__,
                    err,
                )

            if self._stopped:
                return
            # Exponential backoff with jitter.
            delay = min(backoff, ADMIN_WS_BACKOFF_CAP_SECONDS)
            delay = delay * (0.5 + random.random())  # noqa: S311 - jitter, not crypto
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, ADMIN_WS_BACKOFF_CAP_SECONDS)

    async def stop(self) -> None:
        """Signal the loop to stop and close the socket."""
        self._stopped = True
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()

    async def _connect_and_serve(self) -> None:
        url = self._client.ws_url(ADMIN_WS_PATH)
        headers = self._client.auth_headers()
        _LOGGER.info("Admin WS connecting to %s", url)
        async with self._client.session.ws_connect(
            url, headers=headers, heartbeat=_WS_HEARTBEAT
        ) as ws:
            self._ws = ws
            _LOGGER.info("Admin WS connected (branch %s)", self._branch_id)
            await self._send_hello(ws)
            await self._send_full_snapshot(ws)
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_text(ws, msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING):
                    if ws.close_code == _CLOSE_BRANCH_MISMATCH:
                        raise _BranchMismatch()
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
            self._ws = None

    async def _send_hello(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        # Report whether create/edit is possible (split config disables it).
        from .automations import _split_config_in_use

        split = False
        try:
            split = _split_config_in_use(self._hass)
        except Exception:  # noqa: BLE001
            split = False
        await ws.send_str(
            _dumps(
                {
                    "v": 1,
                    "type": "hello",
                    "branchId": self._branch_id,
                    "haVersion": self._hass.config.as_dict().get("version", ""),
                    "integrationVersion": INTEGRATION_VERSION,
                    "capabilities": {
                        "device_control": True,
                        "automation_control": True,
                        "automation_write": not split,
                        "split_config": split,
                    },
                }
            )
        )

    async def _send_full_snapshot(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        from . import control  # noqa: PLC0415 - lazy: avoid circular import via __init__

        entities = control.build_state_snapshot(self._hass)
        await ws.send_str(
            _dumps(
                {
                    "v": 1,
                    "type": "state_snapshot",
                    "branchId": self._branch_id,
                    "full": True,
                    "page": 1,
                    "pages": 1,
                    "entities": entities,
                }
            )
        )

    async def _handle_text(self, ws: aiohttp.ClientWebSocketResponse, data: str) -> None:
        try:
            frame = json.loads(data)
        except (ValueError, TypeError):
            return
        if not isinstance(frame, dict):
            return
        ftype = frame.get("type")
        if ftype == "command":
            await self._handle_command(ws, frame)
        elif ftype == "resync_request":
            await self._send_full_snapshot(ws)

    async def _handle_command(
        self, ws: aiohttp.ClientWebSocketResponse, frame: dict[str, Any]
    ) -> None:
        command_id = frame.get("commandId")
        if not command_id:
            return
        # Cheap receipt so the dashboard can show "sent".
        await ws.send_str(
            _dumps({"v": 1, "type": "ack", "commandId": command_id})
        )
        result = await self._execute(frame)
        await ws.send_str(
            _dumps(
                {
                    "v": 1,
                    "type": "command_result",
                    "commandId": command_id,
                    "status": result.get("status", "error"),
                    "errorKey": result.get("errorKey"),
                    "result": result.get("result"),
                }
            )
        )

    async def _execute(self, frame: dict[str, Any]) -> dict[str, Any]:
        """Dispatch one command to the in-process executors. Never raises."""
        from . import control  # noqa: PLC0415 - lazy: avoid circular import via __init__

        try:
            kind = frame.get("kind")
            if kind == "call_service":
                return await control.call_device_service(self._hass, frame)
            if kind == "get_state":
                return {
                    "status": "ok",
                    "result": {"entities": control.build_state_snapshot(self._hass)},
                }
            if kind == "automation_op":
                return await self._execute_automation(frame)
            return {"status": "error", "errorKey": "HA_COMMAND_FAILED"}
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("command execution errored: %s", err)
            return {"status": "error", "errorKey": "HA_COMMAND_FAILED"}

    async def _execute_automation(self, frame: dict[str, Any]) -> dict[str, Any]:
        from . import automations  # noqa: PLC0415 - lazy: avoid circular import via __init__

        op = frame.get("op")
        target = frame.get("target") or {}
        data = frame.get("data") or {}
        automation_id = target.get("automation_id")
        hass = self._hass

        if op == "list":
            return {"status": "ok", "result": automations.list_automations(hass)}
        if op == "get_config" and automation_id:
            return await automations.get_config(hass, str(automation_id))
        if op == "enable" and automation_id:
            return await automations.set_enabled(hass, str(automation_id), True)
        if op == "disable" and automation_id:
            return await automations.set_enabled(hass, str(automation_id), False)
        if op == "trigger" and automation_id:
            return await automations.trigger(hass, str(automation_id))
        if op == "reload":
            return await automations.reload(hass)
        if op == "upsert":
            config = data.get("config") if isinstance(data, dict) else None
            if not isinstance(config, dict):
                return {"status": "error", "errorKey": "HA_AUTOMATION_WRITE_FAILED"}
            return await automations.upsert(hass, automation_id, config)
        if op == "delete" and automation_id:
            return await automations.delete(hass, str(automation_id))
        return {"status": "error", "errorKey": "HA_COMMAND_FAILED"}


class _BranchMismatch(Exception):
    """Raised when the cloud closes the socket 4002 (branch mismatch)."""
