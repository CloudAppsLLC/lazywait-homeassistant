"""Branch-side camera-AI: publish inference TARGETS for the add-on to run.

ARCHITECTURE (why this doesn't spawn a subprocess): the heavy ML deps
(onnxruntime, opencv, YOLO models) live in the ADD-ON container, NOT in HA core
where this integration runs. HA core and the add-on are SEPARATE containers, so
a `python3 -m lazywait_inference` spawned from here runs with HA-core's python
and fails ("No module named lazywait_inference"). Instead:

  1. This integration (HA core) resolves each AI-enabled camera's LOCAL NVR RTSP
     via HA's stream helper — HA owns the NVR creds; nothing else can resolve them.
  2. It writes the resolved {rtsp, config, ingest} set to a JSON file in the HA
     config dir (mounted into the add-on at /homeassistant, `homeassistant_config:rw`).
  3. The ADD-ON's inference supervisor (run.sh) reads that file and runs YOLO per
     camera IN the add-on container (where the venv + models exist), posting events.

So this module's job is just: reconcile the cloud `cameraAi` block → resolve RTSP
→ write the targets file. No subprocess. Best-effort + never raises (a write
failure must not break the coordinator loop).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from .mediarelay import _resolve_rtsp_source

_LOGGER = logging.getLogger(__name__)

# Where we write the targets the add-on reads. The HA config dir is mounted into
# the add-on at /homeassistant (homeassistant_config:rw). Inside HA core the same
# dir is /config. Write to /config; the add-on reads /homeassistant/<same file>.
_TARGETS_FILENAME = "lazywait_inference_targets.json"


def _targets_path() -> str:
    # HA core sees its config dir at /config (or /homeassistant on some builds).
    for base in ("/config", "/homeassistant"):
        if os.path.isdir(base):
            return os.path.join(base, _TARGETS_FILENAME)
    return os.path.join("/config", _TARGETS_FILENAME)


class CameraAiManager:
    """Reconciles the inference-targets file to the cloud cameraAi config. One per
    branch, owned by the coordinator; `reconcile` is called each poll cycle."""

    def __init__(self, hass: Any) -> None:
        self._hass = hass
        self._lock = asyncio.Lock()
        self._last_written = ""  # dedupe identical writes

    async def reconcile(self, camera_ai: Any) -> None:
        """Resolve RTSP for each AI camera + write the targets file. Never raises."""
        try:
            async with self._lock:
                await self._reconcile_locked(camera_ai)
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("camera-ai reconcile errored (ignored): %s", err, exc_info=True)

    async def _reconcile_locked(self, camera_ai: Any) -> None:
        if not isinstance(camera_ai, dict):
            camera_ai = {}
        cameras = [c for c in (camera_ai.get("cameras") or []) if isinstance(c, dict)]
        ingest_url = camera_ai.get("ingestUrl") or ""
        ingest_token = camera_ai.get("ingestToken") or ""

        targets = []
        if cameras and ingest_url and ingest_token:
            for cam in cameras:
                cid = cam.get("cameraId")
                if not cid:
                    continue
                rtsp = await _resolve_rtsp_source(self._hass, cid)
                if not rtsp:
                    _LOGGER.debug("camera-ai: no RTSP source for %s; skipping", cid)
                    continue
                targets.append(
                    {
                        "cameraId": cid,
                        "rtsp": rtsp,
                        "detectHumans": cam.get("detectHumans", True),
                        "detectActions": cam.get("detectActions") or [],
                        "objectClasses": cam.get("objectClasses") or [],
                        "sampleFps": cam.get("sampleFps") or 4,
                        "areaId": cam.get("areaId"),
                    }
                )

        payload = {
            "ingestUrl": ingest_url,
            "ingestToken": ingest_token,
            "cameras": targets,
        }
        # Redact the token in logs; write the real thing to the file.
        _LOGGER.info(
            "camera-ai: %d target(s) → %s",
            len(targets),
            [t["cameraId"] for t in targets],
        )

        body = json.dumps(payload)
        if body == self._last_written:
            return  # unchanged; skip the write
        try:
            path = _targets_path()
            tmp = f"{path}.tmp"
            # Write via executor so file IO never blocks the event loop.
            await self._hass.async_add_executor_job(_write_file, tmp, path, body)
            self._last_written = body
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("camera-ai: failed to write targets file: %s", err)

    async def stop_all(self) -> None:
        """On unload, clear the targets file so the add-on stops all inference."""
        try:
            path = _targets_path()
            empty = json.dumps({"ingestUrl": "", "ingestToken": "", "cameras": []})
            await self._hass.async_add_executor_job(_write_file, f"{path}.tmp", path, empty)
            self._last_written = empty
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("camera-ai: failed to clear targets file: %s", err)


def _write_file(tmp: str, path: str, body: str) -> None:
    """Atomic write: temp file + rename (so the add-on never reads a half file)."""
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(body)
    os.replace(tmp, path)
