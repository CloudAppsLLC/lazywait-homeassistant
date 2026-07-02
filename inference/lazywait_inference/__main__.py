"""Entrypoint for one camera's edge inference (spawned by the HA integration).

The `lazywait` integration's CameraAiManager (camerai.py) spawns ONE of these per
AI-enabled camera, mirroring how mediarelay.py spawns one ffmpeg per camera. The
integration resolves the LOCAL NVR RTSP (it owns the creds via HA's stream
helper) and passes everything via env — this process does NO cloud config poll:

  LW_RTSP_URL       local NVR RTSP for this camera (creds embedded by HA)
  LW_SINGLE_CAMERA  the HA camera id (used as camera_id in events)
  LW_CAMERA_CONFIG  JSON: {cameraId, detectHumans, detectActions[], objectClasses[],
                    sampleFps, areaId}
  LW_INGEST_URL     where to POST event batches (/v1/integrations/camera-events/batch)
  LW_INGEST_TOKEN   Bearer (branch media relay token)
  LW_MODELS_DIR     ONNX model dir (default /app/inference/models)

Best-effort + crash-proof: any error logs and the process exits non-zero; the
integration respawns it (subject to the restart floor). A missing model /
onnxruntime disables inference cleanly (exits so the supervisor stops retrying
quickly is fine — snapshot + streaming keep working regardless).
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from typing import List

from .detector import InferenceEngine
from .engine import Engine

logging.basicConfig(level=os.environ.get("LW_LOG_LEVEL", "INFO"))
_LOGGER = logging.getLogger("lazywait_inference")

RTSP_URL = os.environ.get("LW_RTSP_URL", "")
CAMERA_ID = os.environ.get("LW_SINGLE_CAMERA", "")
INGEST_URL = os.environ.get("LW_INGEST_URL", "")
INGEST_TOKEN = os.environ.get("LW_INGEST_TOKEN", "")
try:
    CAMERA_CONFIG = json.loads(os.environ.get("LW_CAMERA_CONFIG", "{}"))
except Exception:  # noqa: BLE001
    CAMERA_CONFIG = {}


def _post_events(events: List[dict]) -> None:
    if not events or not INGEST_URL or not INGEST_TOKEN:
        return
    try:
        data = json.dumps({"events": events[:200]}).encode()
        req = urllib.request.Request(INGEST_URL, data=data, method="POST")
        req.add_header("Authorization", f"Bearer {INGEST_TOKEN}")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("event post failed: %s", err)


def main() -> int:
    if not RTSP_URL or not CAMERA_ID:
        _LOGGER.error("LW_RTSP_URL / LW_SINGLE_CAMERA required")
        return 2

    infer = InferenceEngine()
    if not infer.ready:
        _LOGGER.error("inference engine not ready (models/onnxruntime missing); exiting")
        return 3

    engine = Engine(infer, _post_events)
    # One camera; ensure the config carries this camera's id.
    cam = dict(CAMERA_CONFIG)
    cam["cameraId"] = CAMERA_ID
    engine.set_config([cam])

    # Per-camera fps from config (bounded); the engine processes one camera/tick.
    fps = float(cam.get("sampleFps") or 4)
    tick_interval = 1.0 / max(0.5, min(15.0, fps))
    # This camera's RTSP is fixed; the engine's rtsp resolver just returns it.
    rtsp_for = lambda _cid: RTSP_URL  # noqa: E731

    _LOGGER.info(
        "edge inference: camera=%s provider=%s fps=%.1f actions=%s objects=%s",
        CAMERA_ID,
        infer.provider,
        fps,
        cam.get("detectActions"),
        cam.get("objectClasses"),
    )

    while True:
        now = time.time()
        try:
            engine.tick(rtsp_for)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("tick errored (continuing): %s", err)
        elapsed = time.time() - now
        time.sleep(max(0.0, tick_interval - elapsed))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        pass
