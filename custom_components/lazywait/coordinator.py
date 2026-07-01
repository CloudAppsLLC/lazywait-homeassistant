"""Coordinator: polls cloud config, pushes events, sends heartbeats.

The coordinator is the single owner of the cloud connection for one paired
branch. On each refresh it:
  1. GET /config — applies the latest thresholds/entity-map/version.
  2. Flushes any buffered events to POST /events (with an Idempotency-Key so a
     re-flush after a reconnect is de-duped cloud-side).
  3. POST /status — reports HA version, online state, current config version.

A 401 anywhere raises ConfigEntryAuthFailed, which makes HA start the reauth
flow (re-prompt for a fresh pairing code) instead of silently going stale.

Local presence/absence DECISIONS are made by HA automations (or a future local
helper) and handed to `queue_event`; the cloud only stores thresholds and fans
out notifications. The coordinator never decides absence itself.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import LazyWaitApiClient, LazyWaitApiError, LazyWaitAuthError
from .camera import Go2RtcTarget, answer_offer, list_cameras
from .camerasnapshot import capture_snapshot, encode_snapshot
from .const import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    DOMAIN,
    INTEGRATION_VERSION,
    SNAPSHOT_LOOP_INTERVAL_SECONDS,
    SNAPSHOT_MAX_CONCURRENT,
)
from .mediarelay import MediaRelayManager

if TYPE_CHECKING:
    from .ws_client import LazyWaitAdminSocket

_LOGGER = logging.getLogger(__name__)

# Cap the in-memory buffer so an extended cloud outage can't grow it unbounded.
# Oldest events are dropped first (a stale absence is less useful than a fresh
# one); the drop is logged so the gap is visible.
_MAX_BUFFER = 1000

# Throttle camera-list reporting so it can't spam the cloud even if the poll
# interval is shortened. The default cycle is 30s, so once per cycle is fine;
# this is the floor between two reports.
_CAMERA_REPORT_INTERVAL_SECONDS = 30


class LazyWaitCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Owns the cloud poll/push loop for one branch."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: LazyWaitApiClient,
        branch_id: str,
        go2rtc_target: Go2RtcTarget | None = None,
        default_stream_id: str = "",
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{branch_id}",
            update_interval=timedelta(seconds=DEFAULT_POLL_INTERVAL_SECONDS),
        )
        self._client = client
        self._branch_id = branch_id
        self._buffer: deque[dict[str, Any]] = deque(maxlen=_MAX_BUFFER)
        self._config_version = 0
        self._last_event_at: str | None = None
        # Monotonic counter → a stable Idempotency-Key per flush attempt so a
        # retried flush of the same batch is recognized cloud-side.
        self._flush_seq = 0
        # Live-camera signaling: where the local go2rtc is + the default stream
        # to answer with when the cloud offer carries no explicit cameraId.
        self._go2rtc_target = go2rtc_target or Go2RtcTarget()
        self._default_stream_id = default_stream_id
        # Monotonic timestamp of the last camera-list report; gates the throttle
        # (0.0 → report on the first cycle). monotonic() so a clock change can't
        # skew the interval.
        self._last_camera_report = 0.0
        # The persistent admin-control WebSocket + its task, attached after setup.
        # When the socket is connected, control flows over it (sub-second); when
        # it's down, the coordinator drains queued commands over the 30s poll.
        self._admin_socket: LazyWaitAdminSocket | None = None
        self._admin_task: asyncio.Task | None = None
        # Branch-side stream push: ffmpeg pullers that pull the local NVR RTSP and
        # push SRT to the cloud MediaMTX. Driven entirely by config.media_relay;
        # reconciled each cycle. Best-effort — never breaks the poll loop.
        self._media_relay = MediaRelayManager(hass)
        # Near-live snapshot loop: a SEPARATE ~1s asyncio task (the 30s poll is
        # far too slow to feel live). It polls which cameras the dashboard is
        # viewing now and captures+posts a JPEG for each. Started in
        # async_setup_entry, cancelled on unload alongside the media relay.
        self._snapshot_task: asyncio.Task | None = None

    def attach_admin_socket(
        self, socket: LazyWaitAdminSocket, task: asyncio.Task
    ) -> None:
        """Record the admin WebSocket + its task (started in async_setup_entry)."""
        self._admin_socket = socket
        self._admin_task = task

    def start_snapshot_loop(self) -> None:
        """Start the ~1s near-live snapshot loop (called from async_setup_entry).

        Kept OUT of the 30s poll cycle deliberately: a still image refreshed once
        every 30s isn't "live", so this runs on its own lightweight cadence. The
        task swallows everything (see ``_snapshot_loop``); cancelled on unload in
        ``shutdown_admin_socket`` alongside the admin socket + media relay.
        """
        if self._snapshot_task is not None:
            return
        self._snapshot_task = self.hass.loop.create_task(self._snapshot_loop())
        # INFO (not debug) so the HA log CONFIRMS the near-live loop started —
        # its absence in the log means setup never reached here.
        _LOGGER.info("LazyWait near-live snapshot loop started (branch %s)", self._branch_id)

    async def shutdown_admin_socket(self) -> None:
        """Stop the admin socket task + snapshot loop + media-relay pushers (unload)."""
        if self._admin_socket is not None:
            await self._admin_socket.stop()
        if self._admin_task is not None:
            self._admin_task.cancel()
            try:
                await self._admin_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._admin_socket = None
        self._admin_task = None
        # Cancel the near-live snapshot loop so it doesn't outlive the entry.
        if self._snapshot_task is not None:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._snapshot_task = None
        # Tear down any running ffmpeg SRT pushers so they don't outlive the entry.
        await self._media_relay.stop_all()

    async def _snapshot_loop(self) -> None:
        """Near-live snapshot loop: poll viewed cameras, capture+post, ~1 fps.

        Runs forever until cancelled. Each tick:
          1. GET .../camera/snapshot/requests → the cameras being watched NOW
             (usually 0 or 1; the dashboard registers a camera while its live
             view is open).
          2. For each viewed camera (capped by ``SNAPSHOT_MAX_CONCURRENT``),
             capture a JPEG in-process and POST it.
          3. Sleep ~1s.

        This loop MUST NEVER raise out — a raised exception would kill the task
        silently and stop the near-live view until the next reload. Every branch
        (including the request poll and each capture/post) is wrapped so the loop
        is self-healing: a transient error just skips a tick. Only
        ``asyncio.CancelledError`` propagates, so unload can stop it cleanly.
        """
        while True:
            try:
                await self._snapshot_tick()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - loop must never die
                _LOGGER.debug("snapshot loop tick errored (ignored): %s", err)
            try:
                await asyncio.sleep(SNAPSHOT_LOOP_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                raise

    async def _snapshot_tick(self) -> None:
        """One snapshot cycle: fetch the viewed-camera list, capture+post each.

        Isolated from the loop so the loop body stays a thin try/sleep. A dead
        token surfaces nowhere here (the 30s poll already owns reauth); we just
        skip the tick on any request failure so the near-live path never fights
        the main coordinator over the reauth flow.
        """
        try:
            resp = await self._client.snapshot_requests()
        except LazyWaitApiError as err:
            _LOGGER.debug("snapshot requests poll failed (ignored): %s", err)
            return
        except Exception as err:  # noqa: BLE001 - never break the loop
            _LOGGER.debug("snapshot requests poll errored (ignored): %s", err)
            return

        camera_ids = resp.get("cameraIds") if isinstance(resp, dict) else None
        if not isinstance(camera_ids, list) or not camera_ids:
            return  # Nobody watching → capture nothing.

        # Cap concurrency: only the cameras actually being viewed, and no more
        # than SNAPSHOT_MAX_CONCURRENT at once (usually 1). Dedupe + keep order.
        seen: set[str] = set()
        targets: list[str] = []
        for cid in camera_ids:
            if isinstance(cid, str) and cid and cid not in seen:
                seen.add(cid)
                targets.append(cid)
            if len(targets) >= SNAPSHOT_MAX_CONCURRENT:
                break

        await asyncio.gather(
            *(self._capture_and_post(cid) for cid in targets)
        )

    async def _capture_and_post(self, camera_id: str) -> None:
        """Capture one camera's current frame in-process and upload it.

        Both capture and post are best-effort: ``capture_snapshot`` returns None
        on any failure (never raises), and a failed post is swallowed. A dropped
        frame is fine — the next tick (~1s later) tries again.
        """
        captured = await capture_snapshot(self.hass, camera_id)
        if captured is None:
            # WARNING (not silent) — this is THE failure that shows a black
            # "no signal" panel: HA couldn't get a still frame for the camera
            # (async_get_image failed / camera unavailable / no stream). Visible
            # so the cause is diagnosable from the HA log.
            _LOGGER.warning(
                "LazyWait snapshot: capture returned NO image for %s "
                "(camera_proxy/async_get_image failed — camera unavailable or "
                "no still available)",
                camera_id,
            )
            return
        content, content_type = captured
        try:
            await self._client.post_snapshot(
                camera_id, encode_snapshot(content), content_type
            )
            _LOGGER.info(
                "LazyWait snapshot: posted %s (%s bytes) for %s",
                content_type,
                len(content),
                camera_id,
            )
        except LazyWaitApiError as err:
            _LOGGER.warning("LazyWait snapshot POST failed for %s: %s", camera_id, err)
        except Exception as err:  # noqa: BLE001 - never break the loop
            _LOGGER.warning("LazyWait snapshot POST errored for %s: %s", camera_id, err)

    @property
    def branch_id(self) -> str:
        return self._branch_id

    @property
    def config_version(self) -> int:
        return self._config_version

    def queue_event(
        self, event_type: str, entity_id: str, occurred_at: str, payload: dict[str, Any]
    ) -> None:
        """Buffer an event for the next flush. Called by automations/helpers."""
        if len(self._buffer) == self._buffer.maxlen:
            _LOGGER.warning(
                "LazyWait event buffer full (%s); dropping oldest event",
                self._buffer.maxlen,
            )
        self._buffer.append(
            {
                "type": event_type,
                "entityId": entity_id,
                "occurredAt": occurred_at,
                "payload": payload or {},
            }
        )
        self._last_event_at = occurred_at

    async def _async_update_data(self) -> dict[str, Any]:
        """One poll cycle: config → flush events → heartbeat."""
        try:
            config = await self._client.get_config()
            new_version = config.get("version")
            if isinstance(new_version, int):
                self._config_version = new_version
                # Apply the cloud's poll interval if it changed.
                interval = config.get("pollIntervalSeconds")
                if isinstance(interval, int) and interval > 0:
                    self.update_interval = timedelta(seconds=interval)

            await self._flush_events()

            await self._client.report_status(
                ha_version=HA_VERSION,
                integration_version=INTEGRATION_VERSION,
                online=True,
                config_version=self._config_version,
                last_event_at=self._last_event_at,
            )

            # Best-effort: report the discovered camera list (throttled) so the
            # cloud always has a fresh picker, then service any pending
            # live-camera WebRTC offer. Both are isolated so a camera-path hiccup
            # never fails the main poll cycle.
            await self._report_cameras()
            await self._pump_camera_signaling()

            # Degraded admin-control path: when the persistent WebSocket is down,
            # drain any queued admin commands over this HTTP poll so control
            # still works (within ~30s) instead of silently stalling. Best-effort
            # and isolated — never fails the main cycle.
            await self._drain_admin_commands()

            # Branch-side stream push: converge the ffmpeg SRT pushers onto the
            # cloud's media_relay directive (start enabled cameras, stop removed
            # ones, restart dead ones). Best-effort and isolated inside the
            # manager — never fails this cycle.
            await self._media_relay.reconcile(config.get("media_relay"))

            return config
        except LazyWaitAuthError as err:
            # Token rotated/revoked → HA opens the reauth flow.
            raise ConfigEntryAuthFailed(str(err)) from err
        except LazyWaitApiError as err:
            raise UpdateFailed(f"LazyWait cloud poll failed: {err}") from err

    async def _report_cameras(self) -> None:
        """Enumerate local cameras and push the list to the cloud (throttled).

        Discovery (go2rtc streams + HA camera entities) is best-effort and never
        raises; the cloud caches the list in memory and serves it to the
        dashboard picker. Throttled to ~once per
        ``_CAMERA_REPORT_INTERVAL_SECONDS`` so a shortened poll interval can't
        spam the cloud. Strictly best-effort — any failure (other than a dead
        token, which the calls above already surfaced) is logged and swallowed so
        the camera path can never break the coordinator cycle.
        """
        now = time.monotonic()
        if now - self._last_camera_report < _CAMERA_REPORT_INTERVAL_SECONDS:
            return
        # Stamp BEFORE the await so two cycles can't both slip past the throttle.
        self._last_camera_report = now

        session = self._client._session  # noqa: SLF001 - same package reuse
        try:
            # Pass the in-process HomeAssistant instance so discovery can read
            # camera.* entities straight from hass.states (the reliable path for
            # Hikvision/NVR channels that never auto-register with go2rtc) — no
            # token, no HTTP.
            cameras = await list_cameras(
                session, self._go2rtc_target, hass=self.hass
            )
        except Exception as err:  # noqa: BLE001 - discovery must never break us
            _LOGGER.debug("camera discovery errored (ignored): %s", err)
            return

        try:
            await self._client.report_cameras(cameras)
            _LOGGER.debug("Reported %s camera(s) to cloud", len(cameras))
        except LazyWaitAuthError:
            raise
        except LazyWaitApiError as err:
            _LOGGER.debug("camera report failed (ignored): %s", err)
        except Exception as err:  # noqa: BLE001 - never break the loop
            _LOGGER.debug("camera report errored (ignored): %s", err)

    async def _pump_camera_signaling(self) -> None:
        """Service one pending live-camera WebRTC offer, if any.

        Polls the cloud for a pending SDP offer for this branch; when present,
        produces an answer and posts it back. The answer comes from HA's NATIVE
        camera WebRTC API first (``answer_offer`` hands the offer to the camera
        component, which routes to its correctly configured go2rtc provider —
        no loopback port guessing), with the legacy go2rtc loopback POST as a
        last-resort fallback. This is SIGNALING ONLY — the resulting media flows
        peer-to-peer (dashboard <-> go2rtc) over Twilio TURN and never traverses
        HA's poll path.

        Strictly best-effort: any failure is logged and swallowed so the camera
        path can never break the main coordinator cycle. A LazyWaitAuthError is
        the one exception we re-raise — a dead token must surface as reauth, and
        it would have already failed the config/heartbeat calls above anyway.
        """
        try:
            pending = await self._client.camera_poll()
        except LazyWaitAuthError:
            raise
        except LazyWaitApiError as err:
            _LOGGER.debug("camera poll failed (ignored): %s", err)
            return
        except Exception as err:  # noqa: BLE001 - never break the loop
            _LOGGER.debug("camera poll errored (ignored): %s", err)
            return

        if not isinstance(pending, dict) or not pending.get("pending"):
            return

        session_id = pending.get("sessionId")
        offer_sdp = pending.get("offer")
        camera_id = pending.get("cameraId") or ""
        if not session_id or not offer_sdp:
            _LOGGER.debug("camera poll returned a malformed pending offer; skipping")
            return

        # Reuse the API client's aiohttp session for the go2rtc fallback call.
        session = self._client._session  # noqa: SLF001 - same package reuse

        # Pass the in-process HomeAssistant object + the signaling session id so
        # answer_offer can use HA's NATIVE camera WebRTC API first (HA routes the
        # offer to its correctly configured go2rtc provider — no loopback port
        # guessing). The go2rtc loopback POST stays as a last-resort fallback
        # inside answer_offer.
        result = await answer_offer(
            session,
            self._go2rtc_target,
            camera_id=camera_id,
            offer_sdp=offer_sdp,
            default_stream_id=self._default_stream_id,
            hass=self.hass,
        )

        if not result.ok or not result.answer_sdp:
            _LOGGER.warning(
                "Live camera: no SDP answer for session %s (%s)",
                session_id,
                (result.fallback or {}).get("reason") if result.fallback else "unknown",
            )
            return

        try:
            await self._client.camera_answer(session_id, result.answer_sdp)
            _LOGGER.info("Live camera: answered session %s", session_id)
        except LazyWaitAuthError:
            raise
        except LazyWaitApiError as err:
            _LOGGER.debug("camera answer post failed (ignored): %s", err)
        except Exception as err:  # noqa: BLE001 - never break the loop
            _LOGGER.debug("camera answer post errored (ignored): %s", err)

    async def _drain_admin_commands(self) -> None:
        """When the admin WS is down, claim + execute queued admin commands over
        the HTTP poll, then post the result. One command per cycle keeps it
        simple; the WS path handles the fast/common case. Best-effort: any error
        is logged and swallowed so the camera/event path is never affected."""
        # If the persistent socket is up, it's already servicing commands — skip.
        if self._admin_socket is not None and self._admin_socket.is_connected:
            return
        try:
            resp = await self._client.poll_commands()
        except LazyWaitAuthError:
            raise
        except LazyWaitApiError as err:
            _LOGGER.debug("admin command poll failed (ignored): %s", err)
            return
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("admin command poll errored (ignored): %s", err)
            return

        if not isinstance(resp, dict) or not resp.get("pending"):
            return
        command = resp.get("command")
        if not isinstance(command, dict):
            return
        command_id = command.get("commandId")
        if not command_id:
            return

        result = await self._execute_admin_command(command)
        try:
            await self._client.post_command_result(str(command_id), result)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("admin command result post failed (ignored): %s", err)

    async def _execute_admin_command(self, command: dict[str, Any]) -> dict[str, Any]:
        """Execute one queued command via the in-process executors. Mirrors the
        WS client's dispatch so both paths run identical logic."""
        # Lazy import to break a circular import: __init__.py imports this module
        # (coordinator) at top level, so a top-level `from . import automations,
        # control` here re-enters the partially-initialized package and HA fails
        # the whole integration with "cannot import name 'automations' ... most
        # likely due to a circular import". Importing the submodules HERE (after
        # the package finished initializing) sidesteps the cycle entirely.
        from . import automations, control  # noqa: PLC0415

        try:
            kind = command.get("kind")
            if kind == "call_service":
                return await control.call_device_service(self.hass, command)
            if kind == "get_state":
                return {
                    "status": "ok",
                    "result": {"entities": control.build_state_snapshot(self.hass)},
                }
            if kind == "automation_op":
                op = command.get("op")
                target = command.get("target") or {}
                data = command.get("data") or {}
                aid = target.get("automation_id")
                if op == "list":
                    return {"status": "ok", "result": automations.list_automations(self.hass)}
                if op == "get_config" and aid:
                    return await automations.get_config(self.hass, str(aid))
                if op == "enable" and aid:
                    return await automations.set_enabled(self.hass, str(aid), True)
                if op == "disable" and aid:
                    return await automations.set_enabled(self.hass, str(aid), False)
                if op == "trigger" and aid:
                    return await automations.trigger(self.hass, str(aid))
                if op == "reload":
                    return await automations.reload(self.hass)
                if op == "upsert":
                    config = data.get("config") if isinstance(data, dict) else None
                    if not isinstance(config, dict):
                        return {"status": "error", "errorKey": "HA_AUTOMATION_WRITE_FAILED"}
                    return await automations.upsert(self.hass, aid, config)
                if op == "delete" and aid:
                    return await automations.delete(self.hass, str(aid))
            return {"status": "error", "errorKey": "HA_COMMAND_FAILED"}
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("admin command execution errored: %s", err)
            return {"status": "error", "errorKey": "HA_COMMAND_FAILED"}

    async def _flush_events(self) -> None:
        """Drain the buffer to the cloud; re-buffer on failure for next cycle."""
        if not self._buffer:
            return
        batch = list(self._buffer)
        self._flush_seq += 1
        idempotency_key = f"{self._branch_id}:{self._flush_seq}"
        try:
            await self._client.push_events(batch, idempotency_key=idempotency_key)
            # Only clear what we successfully sent — events queued DURING the
            # await stay buffered for the next flush.
            for _ in range(len(batch)):
                if self._buffer:
                    self._buffer.popleft()
        except LazyWaitAuthError:
            raise
        except LazyWaitApiError as err:
            _LOGGER.warning(
                "LazyWait event flush failed (%s buffered): %s", len(batch), err
            )
            # Leave the buffer intact; next cycle retries with the SAME seq+1
            # only after a success, so dedup holds.
            self._flush_seq -= 1
