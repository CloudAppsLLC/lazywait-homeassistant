"""Branch-side camera-AI inference supervisor (edge detection on the Pi).

Mirrors mediarelay.py's reconcile pattern, but instead of ffmpeg SRT pushers it
supervises one **inference subprocess per AI-enabled camera**. The heavy ML deps
(onnxruntime, opencv, YOLO models) live in the ADD-ON image on PATH — NOT in HA
core (this integration declares requirements: []). So we spawn the bundled
inference process (`python -m lazywait_inference`, or LW_INFERENCE_CMD) and hand
it, per camera:
  - the LOCAL RTSP url (resolved here via HA's stream helper — HA owns the NVR
    creds; the add-on/inference process never sees them otherwise),
  - the detection config (actions/objects/fps/area) from the cloud cameraAi block,
  - the ingest URL + token to POST events to.

Everything is best-effort + crash-proof: a missing inference binary, a failed
RTSP resolve, or a dead subprocess is logged and retried next cycle. Inference
never breaks the coordinator loop (mirrors mediarelay.py's contract).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from typing import Any

from .mediarelay import _resolve_rtsp_source

_LOGGER = logging.getLogger(__name__)

# Cap concurrent inference subprocesses. The Pi runs 1-8 cameras; each process
# round-robins its own camera at bounded fps, but we still cap total processes so
# a large NVR can't spawn a runaway fleet that starves HA + ffmpeg.
_MAX_INFERENCE_PROCS = 8
# Don't respawn a crashed process faster than this (avoid a tight crash loop).
_RESTART_MIN_INTERVAL_SECONDS = 20.0

# The command that runs the bundled inference package. Overridable so the add-on
# can point at a venv python. Default assumes the module is importable.
_INFERENCE_CMD = os.environ.get("LW_INFERENCE_CMD", "python3 -m lazywait_inference")


def _inference_available() -> bool:
    """True if the inference entrypoint is runnable (python3 + the package)."""
    exe = _INFERENCE_CMD.split()[0]
    return shutil.which(exe) is not None


class _InferenceProc:
    """One inference subprocess for a single camera."""

    def __init__(self, camera_id: str, config_key: str) -> None:
        self.camera_id = camera_id
        # A hash of the per-camera config; a change means "restart with new args".
        self.config_key = config_key
        self._proc: asyncio.subprocess.Process | None = None
        self._last_start = 0.0
        self._stderr_task: asyncio.Task | None = None

    @property
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    def can_restart(self, now: float) -> bool:
        return (now - self._last_start) >= _RESTART_MIN_INTERVAL_SECONDS

    async def start(self, rtsp_url: str, env_extra: dict[str, str]) -> bool:
        """Spawn `python -m lazywait_inference` for this one camera."""
        self._last_start = asyncio.get_running_loop().time()
        env = {**os.environ, **env_extra, "LW_RTSP_URL": rtsp_url, "LW_SINGLE_CAMERA": self.camera_id}
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *_INFERENCE_CMD.split(),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("camera-ai: failed to spawn inference for %s: %s", self.camera_id, err)
            self._proc = None
            return False
        # Drain stderr so a crash reason is visible (bounded) without blocking.
        self._stderr_task = asyncio.ensure_future(self._drain_stderr())
        _LOGGER.info("camera-ai: inference started for %s", self.camera_id)
        return True

    async def _drain_stderr(self) -> None:
        if not self._proc or not self._proc.stderr:
            return
        try:
            while True:
                line = await self._proc.stderr.readline()
                if not line:
                    break
                _LOGGER.debug("camera-ai[%s]: %s", self.camera_id, line.decode(errors="replace").rstrip())
        except Exception:  # noqa: BLE001
            pass

    async def stop(self) -> None:
        proc = self._proc
        self._proc = None
        if self._stderr_task:
            self._stderr_task.cancel()
            self._stderr_task = None
        if proc is None or proc.returncode is not None:
            return
        try:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                proc.kill()
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("camera-ai: stop errored for %s: %s", self.camera_id, err)


def _config_key(cam: dict) -> str:
    """Stable hash of the per-camera detection config (restart on change)."""
    return json.dumps(
        {
            "a": sorted(cam.get("detectActions") or []),
            "o": sorted(cam.get("objectClasses") or []),
            "h": bool(cam.get("detectHumans", True)),
            "f": cam.get("sampleFps"),
            "r": cam.get("areaId"),
        },
        sort_keys=True,
    )


class CameraAiManager:
    """Reconciles inference subprocesses to the cloud cameraAi config. One per
    branch, owned by the coordinator; `reconcile` is called each poll cycle."""

    def __init__(self, hass: Any) -> None:
        self._hass = hass
        self._procs: dict[str, _InferenceProc] = {}
        self._lock = asyncio.Lock()
        self._unavailable_logged = False

    async def reconcile(self, camera_ai: Any) -> None:
        """Converge running inference procs onto config.cameraAi. Never raises."""
        try:
            async with self._lock:
                await self._reconcile_locked(camera_ai)
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("camera-ai reconcile errored (ignored): %s", err, exc_info=True)

    async def _reconcile_locked(self, camera_ai: Any) -> None:
        if not isinstance(camera_ai, dict):
            camera_ai = {}
        cameras = camera_ai.get("cameras") or []
        ingest_url = camera_ai.get("ingestUrl") or ""
        ingest_token = camera_ai.get("ingestToken") or ""

        _LOGGER.info(
            "camera-ai: reconcile cameras=%s",
            [c.get("cameraId") for c in cameras if isinstance(c, dict)],
        )

        # No cameras / no ingest → stop everything.
        if not cameras or not ingest_url or not ingest_token:
            if self._procs:
                _LOGGER.info("camera-ai: no cameras/ingest; stopping all inference")
                await self.stop_all()
            return

        # Inference binary must be present (add-on image installs it). Log once.
        if not _inference_available():
            if not self._unavailable_logged:
                _LOGGER.error(
                    "camera-ai: inference entrypoint '%s' not found; %s camera(s) "
                    "cannot run detection. The add-on image must install "
                    "onnxruntime + the lazywait_inference package.",
                    _INFERENCE_CMD,
                    len(cameras),
                )
                self._unavailable_logged = True
            return
        self._unavailable_logged = False

        cameras = cameras[:_MAX_INFERENCE_PROCS]
        desired = {c["cameraId"]: c for c in cameras if isinstance(c, dict) and c.get("cameraId")}

        # 1) Stop procs no longer desired or whose config changed.
        for cid in list(self._procs):
            want = desired.get(cid)
            if want is None or self._procs[cid].config_key != _config_key(want):
                await self._procs[cid].stop()
                self._procs.pop(cid, None)

        # 2) Start / restart the rest.
        now = asyncio.get_running_loop().time()
        common_env = {
            "LW_INGEST_URL": ingest_url,
            "LW_INGEST_TOKEN": ingest_token,
        }
        for cid, cam in desired.items():
            existing = self._procs.get(cid)
            if existing is not None and existing.is_running:
                continue
            if existing is not None and not existing.can_restart(now):
                continue
            rtsp = await _resolve_rtsp_source(self._hass, cid)
            if not rtsp:
                _LOGGER.debug("camera-ai: no RTSP source for %s; skipping", cid)
                continue
            if existing is not None:
                await existing.stop()
            proc = _InferenceProc(cid, _config_key(cam))
            env_extra = {
                **common_env,
                "LW_CAMERA_CONFIG": json.dumps(cam),
            }
            await proc.start(rtsp, env_extra)
            self._procs[cid] = proc

    async def stop_all(self) -> None:
        procs = list(self._procs.values())
        self._procs.clear()
        for p in procs:
            try:
                await p.stop()
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("camera-ai: stop errored for %s: %s", p.camera_id, err)

    @property
    def active_camera_ids(self) -> list[str]:
        return [cid for cid, p in self._procs.items() if p.is_running]
