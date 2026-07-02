"""LazyWaitAdminSocket — the persistent OUTBOUND admin-control WebSocket.

HA is outbound-only behind NAT, so the cloud can never dial in. This client
opens ONE long-lived WebSocket to the cloud, authenticated by the SAME
enrollment bearer the HTTP path uses, and:

  * sends a ``hello`` frame (branch + versions + capabilities) on connect,
  * pushes the full inventory on connect (and again on ``resync_request``):
    ``state_snapshot`` (paged when huge) → ``registry_snapshot`` (areas +
    devices) → ``services_catalog`` (callable domain/service names),
  * re-pushes ``state_snapshot`` + ``registry_snapshot`` every 60s
    (ADMIN_STATE_FULL_PUSH_SECONDS) so a cloud restart self-heals,
  * pushes debounced ``full:false`` state deltas from an EVENT_STATE_CHANGED
    listener (2s batches) so the dashboard tracks live state between pushes,
  * receives ``command`` frames, executes them in-process (control.py /
    automations.py), and posts a ``command_result`` back on the same socket,
  * answers the cloud's WS-level pings (aiohttp ``heartbeat``) to keep the
    half-open detection honest.

Lifecycle: started as one asyncio.Task in async_setup_entry and cancelled in
async_unload_entry. It NEVER raises out of the task (an unraised exception in a
bare task is swallowed by asyncio and would silently kill control) — a 401 at
the upgrade handshake explicitly kicks off HA's reauth instead of a tight
reconnect storm; any other drop reconnects with exponential backoff + jitter.
The per-connection push helpers (periodic task, state listener, delta flusher)
are torn down on every socket close/stop/reconnect — no leaks, no duplicate
listeners across reconnects.
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
from homeassistant.const import EVENT_STATE_CHANGED
from homeassistant.core import HomeAssistant, callback

from .api import LazyWaitApiClient
from .const import (
    ADMIN_STATE_DELTA_DEBOUNCE_SECONDS,
    ADMIN_STATE_FULL_PUSH_SECONDS,
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
    admin WS on EVERY connect at _send_state_snapshot → the control socket never
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
        # Per-connection push helpers — created in _start_push_helpers after a
        # successful connect, torn down in _stop_push_helpers on EVERY exit
        # path (close/error/stop/reconnect) so listeners never stack up.
        self._periodic_task: asyncio.Task | None = None
        self._delta_flush_task: asyncio.Task | None = None
        self._unsub_state_changed: Any | None = None
        self._pending_deltas: set[str] = set()
        # async_get_all_descriptions loads every integration's services.yaml
        # (slow) — cache the map per connection; it's static enough for one.
        self._service_descriptions: dict[str, Any] | None = None

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
        """Signal the loop to stop, tear down push helpers, close the socket."""
        self._stopped = True
        self._stop_push_helpers()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()

    async def _connect_and_serve(self) -> None:
        url = self._client.ws_url(ADMIN_WS_PATH)
        headers = self._client.auth_headers()
        _LOGGER.info("Admin WS connecting to %s", url)
        # The services-description cache is per-connection.
        self._service_descriptions = None
        async with self._client.session.ws_connect(
            url, headers=headers, heartbeat=_WS_HEARTBEAT
        ) as ws:
            self._ws = ws
            try:
                _LOGGER.info("Admin WS connected (branch %s)", self._branch_id)
                await self._send_hello(ws)
                await self._send_inventory(ws)
                self._start_push_helpers(ws)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_text(ws, msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING):
                        if ws.close_code == _CLOSE_BRANCH_MISMATCH:
                            raise _BranchMismatch()
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break
            finally:
                # EVERY exit path (clean close, error, cancel) tears the
                # helpers down — a reconnect must never stack a second
                # state-changed listener or periodic task.
                self._stop_push_helpers()
                self._ws = None

    # ── Per-connection push helpers ──────────────────────────────────────────

    def _start_push_helpers(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Start the periodic full-push task + the state-changed listener."""
        self._stop_push_helpers()  # belt-and-suspenders against double-start
        self._unsub_state_changed = self._hass.bus.async_listen(
            EVENT_STATE_CHANGED, self._on_state_changed
        )
        self._periodic_task = asyncio.create_task(self._periodic_full_push(ws))

    def _stop_push_helpers(self) -> None:
        """Unsubscribe + cancel everything started for the current socket."""
        if self._unsub_state_changed is not None:
            try:
                self._unsub_state_changed()
            except Exception:  # noqa: BLE001 - teardown must never raise
                pass
            self._unsub_state_changed = None
        for attr in ("_periodic_task", "_delta_flush_task"):
            task: asyncio.Task | None = getattr(self, attr)
            if task is not None and not task.done():
                task.cancel()
            setattr(self, attr, None)
        self._pending_deltas.clear()

    @callback
    def _on_state_changed(self, event: Any) -> None:
        """EVENT_STATE_CHANGED bus listener — accumulate, flush after the
        debounce window. The first event of a burst schedules the flush;
        later events just join the batch (a fixed trailing window, NOT a
        resetting timer, so a chatty mesh can't starve the flush forever)."""
        data = getattr(event, "data", None)
        entity_id = data.get("entity_id") if isinstance(data, dict) else None
        if not entity_id:
            return
        self._pending_deltas.add(entity_id)
        if self._delta_flush_task is None or self._delta_flush_task.done():
            self._delta_flush_task = asyncio.create_task(self._flush_deltas())

    async def _flush_deltas(self) -> None:
        """Sleep out the debounce window, then ship the batch as `full:false`
        state_snapshot frames — PAGED through the same helper as the full
        snapshot, because a bulk update (integration reload, template-sensor
        recalc) can dirty more entities than fit one WS frame. Loops while new
        changes landed during a send, so nothing waits for the 60s full push.
        Never raises (a failed delta is healed by the 60s full push)."""
        from . import control  # noqa: PLC0415 - lazy: avoid circular import via __init__

        try:
            while True:
                await asyncio.sleep(ADMIN_STATE_DELTA_DEBOUNCE_SECONDS)
                pending, self._pending_deltas = self._pending_deltas, set()
                ws = self._ws
                if not pending or ws is None or ws.closed:
                    return
                entities = control.build_state_snapshot(
                    self._hass, only_entity_ids=pending
                )
                if entities:
                    # Deltas always merge (full:false on every page); page/pages
                    # are informational — the cloud merges by entity_id.
                    for page in control.page_state_snapshot(entities):
                        await ws.send_str(
                            _dumps(
                                {
                                    "v": 1,
                                    "type": "state_snapshot",
                                    "branchId": self._branch_id,
                                    "full": False,
                                    "page": 1,
                                    "pages": 1,
                                    "entities": page,
                                }
                            )
                        )
                # Changes that arrived while we were sending are in the fresh
                # set — flush them next window instead of stranding them until
                # the periodic push. (Events land in _pending_deltas during our
                # awaits; the emptiness check and return are one synchronous
                # step, so nothing can slip in after it.)
                if not self._pending_deltas:
                    return
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 - must never kill the loop
            _LOGGER.debug("Admin WS delta push failed: %s", err)

    async def _periodic_full_push(
        self, ws: aiohttp.ClientWebSocketResponse
    ) -> None:
        """Every ADMIN_STATE_FULL_PUSH_SECONDS re-push state + registry so the
        cloud cache self-heals after an apiv2 restart or a missed delta."""
        try:
            while not ws.closed:
                await asyncio.sleep(ADMIN_STATE_FULL_PUSH_SECONDS)
                if ws.closed:
                    return
                await self._send_state_snapshot(ws)
                await self._send_registry_snapshot(ws)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 - must never kill the loop
            # A send on a closing socket lands here; the run loop owns the
            # reconnect, this task just bows out.
            _LOGGER.debug("Admin WS periodic push stopped: %s", err)

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
                        # 26.7.8+: full inventory (all domains + registry
                        # frames), services catalog, and script triggering.
                        "inventory": True,
                        "services_catalog": True,
                        "scripts": True,
                        # 26.8.0+: Smart Branch telemetry — sensor_reading
                        # batches for the curated monitored_entities set. The
                        # cloud's authoritative gate is integrationVersion
                        # (telemetrySupport.ts); this flag mirrors it for
                        # capability-based consumers.
                        "telemetry": True,
                    },
                }
            )
        )

    async def _send_inventory(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """The full inventory push: state → registry → services (contract order)."""
        await self._send_state_snapshot(ws)
        await self._send_registry_snapshot(ws)
        await self._send_services_catalog(ws)

    async def _send_state_snapshot(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        from . import control  # noqa: PLC0415 - lazy: avoid circular import via __init__

        entities = control.build_state_snapshot(self._hass)
        pages = control.page_state_snapshot(entities)
        total = len(pages)
        for idx, page in enumerate(pages, start=1):
            await ws.send_str(
                _dumps(
                    {
                        "v": 1,
                        "type": "state_snapshot",
                        "branchId": self._branch_id,
                        # Page 1 `full:true` resets the cloud cache; pages 2+
                        # `full:false` merge in (the cloud's merge semantics
                        # make paging correct).
                        "full": idx == 1,
                        "page": idx,
                        "pages": total,
                        "entities": page,
                    }
                )
            )

    async def _send_registry_snapshot(
        self, ws: aiohttp.ClientWebSocketResponse
    ) -> None:
        from . import control  # noqa: PLC0415 - lazy: avoid circular import via __init__

        registry = control.build_registry_snapshot(self._hass)
        await ws.send_str(
            _dumps(
                {
                    "v": 1,
                    "type": "registry_snapshot",
                    "branchId": self._branch_id,
                    "areas": registry["areas"],
                    "devices": registry["devices"],
                }
            )
        )

    async def _send_services_catalog(
        self, ws: aiohttp.ClientWebSocketResponse
    ) -> None:
        from . import control  # noqa: PLC0415 - lazy: avoid circular import via __init__

        if self._service_descriptions is None:
            try:
                self._service_descriptions = await control.get_service_descriptions(
                    self._hass
                )
            except Exception as err:  # noqa: BLE001 - names-only beats no catalog
                _LOGGER.debug("service descriptions unavailable: %s", err)
                self._service_descriptions = {}
        domains = await control.build_services_catalog(
            self._hass, self._service_descriptions
        )
        await ws.send_str(
            _dumps(
                {
                    "v": 1,
                    "type": "services_catalog",
                    "branchId": self._branch_id,
                    "domains": domains,
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
            # The cloud found its cache missing/stale — re-push everything.
            await self._send_inventory(ws)

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
