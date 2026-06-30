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

import logging
import time
from collections import deque
from datetime import timedelta
from typing import Any

from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import LazyWaitApiClient, LazyWaitApiError, LazyWaitAuthError
from .camera import Go2RtcTarget, answer_offer, list_cameras
from .const import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    DOMAIN,
    INTEGRATION_VERSION,
)

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
        asks the local go2rtc to produce an answer and posts it back. This is
        SIGNALING ONLY — the resulting media flows peer-to-peer (dashboard <->
        go2rtc) over Twilio TURN and never traverses HA's poll path.

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

        # Reuse the API client's aiohttp session for the local go2rtc call.
        session = self._client._session  # noqa: SLF001 - same package reuse

        result = await answer_offer(
            session,
            self._go2rtc_target,
            camera_id=camera_id,
            offer_sdp=offer_sdp,
            default_stream_id=self._default_stream_id,
        )

        if not result.ok or not result.answer_sdp:
            _LOGGER.warning(
                "Live camera: go2rtc produced no answer for session %s (%s)",
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
