"""Branch-side stream push: pull local NVR RTSP, push SRT to the cloud MediaMTX.

The cloud can never dial into a branch (NAT / outbound-only), so the branch does
the pushing. For each camera the cloud enabled, this module:

  1. Resolves the camera's LOCAL RTSP source via HA's stream helper
     (``async_get_stream_source`` / ``camera.async_get_stream_source``) — the
     same RTSP the go2rtc/stream integration already knows for that
     ``camera.*`` entity, so no creds are hard-coded here.
  2. Runs an ``ffmpeg`` subprocess that pulls that RTSP (TCP transport) and
     PUSHES it as SRT to the cloud MediaMTX with ``-c copy`` — NO transcode, so
     it's cheap (no CPU-heavy re-encode on the branch box).

The exact push command:

    ffmpeg -rtsp_transport tcp -i <rtsp> -c copy -f mpegts \
      'srt://<host>:<port>?streamid=publish:tenant-<client>/camera-<id>&passphrase=<token>'

MediaMTX on the VPS accepts that SRT publish on the path
``tenant-<client>/camera-<id>`` (passthrough — MediaMTX does not re-encode
either) and the dashboard plays HLS (Caddy TLS proxy) or WebRTC-WHEP from
MediaMTX. The SAME dashboard ``<video>`` is fed to the in-browser face
recognizer — recognition never runs here.

Cloud drives WHICH cameras + endpoint + per-stream token via the existing
``/config`` poll's ``media_relay`` block::

    config.media_relay = {
      "srtHost": "media.example.com",
      "srtPort": 8890,
      "streams": [
        {"cameraId": "camera.front_door", "streamId": "tenant-abc/camera-1",
         "passphrase": "…"},
        …
      ]
    }

``cameraId`` is the HA ``camera.*`` entity id (used to resolve the RTSP source);
``streamId`` is the MediaMTX path the cloud minted; ``passphrase`` is the
per-stream SRT token.

Everything here is BEST-EFFORT: a failed RTSP resolve, a missing ffmpeg binary,
or a crashed pusher is logged and swallowed — the media relay must NEVER break
the coordinator's poll/event/heartbeat cycle. The coordinator calls
``reconcile`` each cycle to converge the running pushers onto the desired set.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from typing import Any

_LOGGER = logging.getLogger(__name__)

# Give a freshly-spawned ffmpeg a moment to fail loudly (bad RTSP, unreachable
# SRT host) before we treat it as "running". Short so reconcile stays snappy.
_STARTUP_GRACE_SECONDS = 1.5

# Floor between automatic restarts of a pusher that keeps exiting, so a
# permanently-broken stream (dead camera, wrong passphrase) can't spin ffmpeg in
# a tight loop. The next reconcile cycle (~30s) is the natural retry cadence.
_RESTART_MIN_INTERVAL_SECONDS = 10.0


class _Pusher:
    """One ffmpeg subprocess pushing a single camera's RTSP out as SRT.

    Owns its subprocess lifecycle: start, liveness check, stop. It does NOT
    self-restart on a schedule — the coordinator's periodic ``reconcile`` is the
    single driver, so restart policy lives in one place (``MediaRelayManager``).
    """

    def __init__(
        self,
        camera_id: str,
        stream_id: str,
        passphrase: str,
        srt_host: str,
        srt_port: int,
    ) -> None:
        self.camera_id = camera_id
        self.stream_id = stream_id
        self.passphrase = passphrase
        self.srt_host = srt_host
        self.srt_port = srt_port
        self._proc: asyncio.subprocess.Process | None = None
        self._rtsp: str | None = None
        # Monotonic timestamp of the last spawn; gates the restart floor.
        self._last_start = 0.0

    @property
    def is_running(self) -> bool:
        """True while the ffmpeg subprocess is alive (returncode still None)."""
        return self._proc is not None and self._proc.returncode is None

    def desired_key(self) -> tuple[str, str, str, str, int]:
        """Identity used to detect config drift (a change → restart).

        If the cloud rotates the passphrase, moves the SRT host/port, or repoints
        the streamId, the key changes and reconcile tears down + respawns.
        """
        return (
            self.camera_id,
            self.stream_id,
            self.passphrase,
            self.srt_host,
            self.srt_port,
        )

    def _build_srt_url(self) -> str:
        """The SRT publish URL for this stream.

        ``streamid=publish:<path>`` is MediaMTX's convention for a publisher on
        ``<path>``; ``passphrase`` is the per-stream SRT encryption key the cloud
        minted. The whole URL is one argv element (no shell), so the ``&`` needs
        no quoting/escaping here — that quoting only matters in the shell form
        shown in the module docstring.
        """
        return (
            f"srt://{self.srt_host}:{self.srt_port}"
            f"?streamid=publish:{self.stream_id}"
            f"&passphrase={self.passphrase}"
        )

    def _build_ffmpeg_args(self, rtsp_url: str) -> list[str]:
        """The ffmpeg argv. ``-c copy`` = remux only, NO transcode (cheap).

        ``-rtsp_transport tcp`` avoids UDP packet loss on the LAN pull;
        ``-f mpegts`` is the container SRT/MediaMTX expects for a raw MPEG-TS
        publish. ``-nostdin`` so ffmpeg never blocks reading our (closed) stdin;
        ``-loglevel warning`` keeps the HA log readable.
        """
        return [
            "ffmpeg",
            "-nostdin",
            "-loglevel",
            "warning",
            "-rtsp_transport",
            "tcp",
            "-i",
            rtsp_url,
            "-c",
            "copy",
            "-f",
            "mpegts",
            self._build_srt_url(),
        ]

    async def start(self, hass: Any) -> bool:
        """Resolve the RTSP source and spawn ffmpeg. Returns True if it launched.

        Best-effort: any failure (no RTSP, ffmpeg missing, spawn error, immediate
        exit) is logged and returns False; the manager retries next cycle. NEVER
        raises.
        """
        loop = asyncio.get_running_loop()
        self._last_start = loop.time()

        rtsp_url = await _resolve_rtsp_source(hass, self.camera_id)
        if not rtsp_url:
            _LOGGER.warning(
                "media relay: no RTSP source for %s; skipping push", self.camera_id
            )
            return False
        self._rtsp = rtsp_url

        args = self._build_ffmpeg_args(rtsp_url)
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except FileNotFoundError:
            # ffmpeg not in the image/PATH — the add-on Dockerfile must `apk add
            # ffmpeg`. Log once per attempt; never raise.
            _LOGGER.error(
                "media relay: ffmpeg binary not found — the add-on image must "
                "install ffmpeg (apk add ffmpeg). Camera %s not relayed.",
                self.camera_id,
            )
            self._proc = None
            return False
        except Exception as err:  # noqa: BLE001 - spawn must never break us
            _LOGGER.warning(
                "media relay: failed to start ffmpeg for %s: %s",
                self.camera_id,
                err,
            )
            self._proc = None
            return False

        # Give ffmpeg a beat to fail fast (bad RTSP, unreachable SRT). If it
        # already exited, surface that and report not-running so reconcile retries.
        try:
            await asyncio.wait_for(
                self._proc.wait(), timeout=_STARTUP_GRACE_SECONDS
            )
        except asyncio.TimeoutError:
            # Still alive after the grace window → treat as running (the healthy
            # path). log the SRT target (NOT the passphrase) for observability.
            _LOGGER.info(
                "media relay: pushing %s → srt://%s:%s (%s)",
                self.camera_id,
                self.srt_host,
                self.srt_port,
                self.stream_id,
            )
            return True

        # Exited within the grace window → failed to start.
        _LOGGER.warning(
            "media relay: ffmpeg for %s exited immediately (rc=%s); will retry",
            self.camera_id,
            self._proc.returncode,
        )
        return False

    async def stop(self) -> None:
        """Terminate the ffmpeg subprocess. Best-effort; never raises."""
        proc = self._proc
        self._proc = None
        if proc is None or proc.returncode is not None:
            return
        try:
            proc.terminate()
        except ProcessLookupError:
            return
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("media relay: terminate errored for %s: %s", self.camera_id, err)
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            # Didn't die on SIGTERM → SIGKILL.
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("media relay: wait errored for %s: %s", self.camera_id, err)

    def can_restart(self, now: float) -> bool:
        """True if enough time has passed since the last spawn to retry."""
        return (now - self._last_start) >= _RESTART_MIN_INTERVAL_SECONDS


async def _resolve_rtsp_source(hass: Any, camera_id: str) -> str | None:
    """Resolve a ``camera.*`` entity's LOCAL RTSP source URL.

    Tries HA's stream helper first (``homeassistant.helpers.async_get_stream_source``
    on newer builds), then the camera component's ``async_get_stream_source``,
    finally the entity's own ``stream_source`` coroutine. Returns the RTSP URL
    (creds embedded by HA — never hard-coded here) or ``None``. NEVER raises.
    """
    if hass is None or not camera_id or not camera_id.startswith("camera."):
        return None

    # 1) Newer HA: a helper on homeassistant.helpers.
    try:
        from homeassistant.helpers import (  # type: ignore  # noqa: PLC0415
            async_get_stream_source,
        )
    except Exception:  # noqa: BLE001 - helper absent on this build → try next
        async_get_stream_source = None  # type: ignore[assignment]
    if callable(async_get_stream_source):
        try:
            src = await async_get_stream_source(hass, camera_id)
            if isinstance(src, str) and src.strip():
                return src.strip()
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("helper stream-source resolve failed for %s: %s", camera_id, err)

    # 2) camera component helper (stable across most builds).
    try:
        from homeassistant.components.camera import (  # type: ignore  # noqa: PLC0415
            async_get_stream_source as cam_get_stream_source,
            get_camera_from_entity_id,
        )
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("camera stream-source API unavailable for %s: %s", camera_id, err)
        return None

    if callable(cam_get_stream_source):
        try:
            src = await cam_get_stream_source(hass, camera_id)
            if isinstance(src, str) and src.strip():
                return src.strip()
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("camera.async_get_stream_source failed for %s: %s", camera_id, err)

    # 3) Last resort: the entity's own stream_source coroutine.
    try:
        camera = get_camera_from_entity_id(hass, camera_id)
        get_src = getattr(camera, "stream_source", None)
        if callable(get_src):
            src = await get_src()
            if isinstance(src, str) and src.strip():
                return src.strip()
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("entity stream_source resolve failed for %s: %s", camera_id, err)

    return None


def _parse_media_relay_config(
    media_relay: Any,
) -> tuple[str, int, list[dict[str, str]]]:
    """Validate + normalize ``config.media_relay`` into (host, port, streams).

    Returns ``("", 0, [])`` when the block is absent/malformed (relay disabled).
    Each returned stream is a dict with non-empty ``cameraId``, ``streamId``,
    ``passphrase``. Streams missing any field are dropped (logged at debug).
    """
    if not isinstance(media_relay, dict):
        return "", 0, []

    srt_host = media_relay.get("srtHost")
    srt_port = media_relay.get("srtPort")
    if not isinstance(srt_host, str) or not srt_host.strip():
        return "", 0, []
    if not isinstance(srt_port, int) or srt_port <= 0:
        return "", 0, []

    raw_streams = media_relay.get("streams")
    if not isinstance(raw_streams, list):
        return "", 0, []

    streams: list[dict[str, str]] = []
    for item in raw_streams:
        if not isinstance(item, dict):
            continue
        camera_id = item.get("cameraId")
        stream_id = item.get("streamId")
        passphrase = item.get("passphrase")
        if not (
            isinstance(camera_id, str)
            and camera_id.strip()
            and isinstance(stream_id, str)
            and stream_id.strip()
            and isinstance(passphrase, str)
            and passphrase.strip()
        ):
            _LOGGER.debug("media relay: dropping malformed stream entry: %s", item)
            continue
        streams.append(
            {
                "cameraId": camera_id.strip(),
                "streamId": stream_id.strip(),
                "passphrase": passphrase,
            }
        )

    return srt_host.strip(), srt_port, streams


class MediaRelayManager:
    """Reconciles the set of running ffmpeg pushers to match cloud config.

    One instance per branch, owned by the coordinator. ``reconcile`` is called
    each poll cycle with the latest ``config.media_relay``; it starts pushers for
    newly-enabled cameras, stops pushers for removed/changed ones, and restarts
    any that have died (subject to a restart floor). Everything is best-effort —
    a relay hiccup must never fail the coordinator cycle.
    """

    def __init__(self, hass: Any) -> None:
        self._hass = hass
        # camera_id → active _Pusher.
        self._pushers: dict[str, _Pusher] = {}
        # Serialize reconcile so two overlapping cycles can't double-spawn.
        self._lock = asyncio.Lock()
        # Latch: warn about a missing ffmpeg binary only once, not every cycle.
        self._ffmpeg_missing_logged = False

    async def reconcile(self, media_relay: Any) -> None:
        """Converge running pushers onto ``config.media_relay``. Never raises."""
        try:
            async with self._lock:
                await self._reconcile_locked(media_relay)
        except Exception as err:  # noqa: BLE001 - relay must never break the loop
            _LOGGER.debug("media relay reconcile errored (ignored): %s", err)

    async def _reconcile_locked(self, media_relay: Any) -> None:
        srt_host, srt_port, streams = _parse_media_relay_config(media_relay)

        # Relay disabled or no valid streams → stop everything.
        if not streams:
            if self._pushers:
                _LOGGER.info("media relay: no streams in config; stopping all pushers")
                await self.stop_all()
            return

        # A missing ffmpeg binary makes every start fail — check once and bail
        # loudly rather than spawn-failing per camera every cycle.
        if shutil.which("ffmpeg") is None:
            if not self._ffmpeg_missing_logged:
                _LOGGER.error(
                    "media relay: ffmpeg not installed in this image; %s stream(s) "
                    "cannot be relayed. The add-on Dockerfile must `apk add ffmpeg`.",
                    len(streams),
                )
                self._ffmpeg_missing_logged = True
            return
        self._ffmpeg_missing_logged = False

        desired: dict[str, _Pusher] = {}
        for s in streams:
            desired[s["cameraId"]] = _Pusher(
                camera_id=s["cameraId"],
                stream_id=s["streamId"],
                passphrase=s["passphrase"],
                srt_host=srt_host,
                srt_port=srt_port,
            )

        # 1) Stop pushers no longer desired, or whose config drifted.
        for camera_id in list(self._pushers):
            current = self._pushers[camera_id]
            wanted = desired.get(camera_id)
            if wanted is None or wanted.desired_key() != current.desired_key():
                await current.stop()
                self._pushers.pop(camera_id, None)

        # 2) Start new / restart dead pushers.
        now = asyncio.get_running_loop().time()
        for camera_id, pusher in desired.items():
            existing = self._pushers.get(camera_id)
            if existing is not None and existing.is_running:
                continue  # already relaying with the same config
            if existing is not None and not existing.can_restart(now):
                continue  # exited recently; honor the restart floor
            if existing is not None:
                # Dead pusher we're allowed to retry — drop it before respawning.
                await existing.stop()
            started = await pusher.start(self._hass)
            # Track it either way; a failed start becomes a restart candidate
            # next cycle (its _last_start gates the floor).
            self._pushers[camera_id] = pusher
            if not started:
                _LOGGER.debug(
                    "media relay: pusher for %s did not start; will retry", camera_id
                )

    async def stop_all(self) -> None:
        """Stop every running pusher (on config-clear or unload). Never raises."""
        pushers = list(self._pushers.values())
        self._pushers.clear()
        for pusher in pushers:
            try:
                await pusher.stop()
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("media relay: stop errored for %s: %s", pusher.camera_id, err)

    @property
    def active_camera_ids(self) -> list[str]:
        """Camera ids with a currently-running pusher (for diagnostics/tests)."""
        return [cid for cid, p in self._pushers.items() if p.is_running]
