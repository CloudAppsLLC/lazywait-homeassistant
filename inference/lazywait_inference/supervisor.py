"""Add-on inference supervisor — runs YOLO per camera INSIDE the add-on.

Started by the add-on's run.sh (as a background process). Reads the targets file
the `lazywait` integration writes to the HA config dir (mounted here at
/homeassistant) — {ingestUrl, ingestToken, cameras:[{cameraId, rtsp, detect*,
objectClasses, sampleFps, areaId}]} — and runs ONE `python -m lazywait_inference`
child per camera (in this container's venv, where onnxruntime + the models live).
Reconciles on file change: starts new cameras, stops removed ones, restarts dead
children (with a floor). Best-effort + crash-proof: this container has the deps,
so `No module named lazywait_inference` (the HA-core-subprocess bug) can't happen.

Env:
  LW_TARGETS_FILE   path to the targets JSON (default /homeassistant/lazywait_inference_targets.json)
  LW_PYTHON         python to run the per-camera worker (default: this interpreter)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from typing import Dict

logging.basicConfig(level=os.environ.get("LW_LOG_LEVEL", "INFO"))
_LOGGER = logging.getLogger("lazywait_inference.supervisor")

# The integration writes /config/<file>; the add-on sees the SAME dir at
# /homeassistant. Prefer that; fall back to /config.
def _default_targets_file() -> str:
    for base in ("/homeassistant", "/config"):
        p = os.path.join(base, "lazywait_inference_targets.json")
        if os.path.isdir(base):
            return p
    return "/homeassistant/lazywait_inference_targets.json"


TARGETS_FILE = os.environ.get("LW_TARGETS_FILE", _default_targets_file())
PYTHON = os.environ.get("LW_PYTHON", sys.executable)
POLL_SECONDS = float(os.environ.get("LW_SUPERVISOR_POLL_S", "5"))
RESTART_FLOOR_S = 20.0


class _Child:
    __slots__ = ("proc", "key", "last_start")

    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self.key = ""
        self.last_start = 0.0

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None


def _cam_key(cam: dict) -> str:
    """Restart the child when any of these change."""
    return json.dumps(
        {
            "r": cam.get("rtsp"),
            "a": sorted(cam.get("detectActions") or []),
            "o": sorted(cam.get("objectClasses") or []),
            "h": bool(cam.get("detectHumans", True)),
            "f": cam.get("sampleFps"),
        },
        sort_keys=True,
    )


def _spawn(cam: dict, ingest_url: str, ingest_token: str) -> subprocess.Popen:
    env = {
        **os.environ,
        "LW_RTSP_URL": cam["rtsp"],
        "LW_SINGLE_CAMERA": cam["cameraId"],
        "LW_INGEST_URL": ingest_url,
        "LW_INGEST_TOKEN": ingest_token,
        "LW_CAMERA_CONFIG": json.dumps(cam),
    }
    _LOGGER.info("supervisor: starting inference for %s", cam["cameraId"])
    return subprocess.Popen(  # noqa: S603
        [PYTHON, "-m", "lazywait_inference"],
        env=env,
        stdin=subprocess.DEVNULL,
    )


def _read_targets() -> dict:
    try:
        with open(TARGETS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"cameras": [], "ingestUrl": "", "ingestToken": ""}
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("supervisor: bad targets file: %s", err)
        return {"cameras": [], "ingestUrl": "", "ingestToken": ""}


def main() -> None:
    _LOGGER.info("supervisor: watching %s (python=%s)", TARGETS_FILE, PYTHON)
    children: Dict[str, _Child] = {}
    while True:
        data = _read_targets()
        ingest_url = data.get("ingestUrl") or ""
        ingest_token = data.get("ingestToken") or ""
        cams = {c["cameraId"]: c for c in data.get("cameras", []) if c.get("cameraId") and c.get("rtsp")}
        can_run = bool(ingest_url and ingest_token)

        # Stop children not desired / config changed.
        for cid in list(children):
            ch = children[cid]
            want = cams.get(cid) if can_run else None
            if want is None or ch.key != _cam_key(want):
                if ch.running:
                    _LOGGER.info("supervisor: stopping inference for %s", cid)
                    try:
                        ch.proc.terminate()
                        try:
                            ch.proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            ch.proc.kill()
                    except Exception:  # noqa: BLE001
                        pass
                children.pop(cid, None)

        # Start / restart desired children.
        now = time.monotonic()
        if can_run:
            for cid, cam in cams.items():
                ch = children.get(cid)
                if ch and ch.running:
                    continue
                if ch and (now - ch.last_start) < RESTART_FLOOR_S:
                    continue  # honor restart floor for a crash-looping child
                nc = _Child()
                nc.key = _cam_key(cam)
                nc.last_start = now
                try:
                    nc.proc = _spawn(cam, ingest_url, ingest_token)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.warning("supervisor: spawn failed for %s: %s", cid, err)
                children[cid] = nc

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
