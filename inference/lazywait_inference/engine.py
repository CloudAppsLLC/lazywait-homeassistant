"""Edge inference engine — runs on the branch Raspberry Pi.

Reconciles to the cloud `cameraAi` config: for each AI-enabled camera it samples
frames from the LOCAL NVR RTSP (round-robin, bounded fps so it never starves HA +
ffmpeg), runs YOLO detection + (when actions are wanted) pose, tracks people with
a lightweight IoU tracker, classifies actions/falls, and batch-POSTs STATE-CHANGE
events to the cloud ingest endpoint (media-relay-token auth).

Design goals: cheap, bounded, crash-proof. Any per-camera failure (RTSP down,
model error) is isolated and retried next cycle; inference never takes down the
add-on or HA.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional, Tuple

from .actions import TrackHistory, classify_single_frame, classify_temporal
from .detector import InferenceEngine

_LOGGER = logging.getLogger(__name__)

# Emit a repeat of the same (track, action) at most this often (state-change +
# heartbeat). Falls bypass this and emit immediately.
_ACTION_COOLDOWN_S = 10.0
# Object census cadence — count objects at most this often per camera (they don't
# move; per-frame counting is wasteful).
_OBJECT_CENSUS_S = 20.0
# IoU threshold to associate a detection with an existing track.
_IOU_MATCH = 0.3
# Drop a track after this many seconds unseen.
_TRACK_TTL_S = 3.0


def _iou(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> float:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    x1, y1 = max(ax, bx), max(ay, by)
    x2, y2 = min(ax + aw, bx + bw), min(ay + ah, by + bh)
    inter = max(0, x2 - x1) * max(0, y2 - y1)
    if inter <= 0:
        return 0.0
    return inter / (aw * ah + bw * bh - inter)


class _Track:
    __slots__ = ("id", "bbox", "last_seen", "hist", "last_actions", "last_emit")

    def __init__(self, tid: int, bbox, now: float) -> None:
        self.id = tid
        self.bbox = bbox
        self.last_seen = now
        self.hist = TrackHistory()
        self.last_actions: Dict[str, float] = {}
        self.last_emit = 0.0


class _CameraState:
    """Per-camera tracker + object-census timer."""

    def __init__(self) -> None:
        self.tracks: List[_Track] = []
        self._next_id = 1
        self.last_object_census = 0.0

    def assign(self, dets_person: List[Tuple[float, float, float, float]], now: float) -> List[_Track]:
        # Greedy IoU association; unmatched detections spawn new tracks.
        used = set()
        matched: List[_Track] = []
        for bbox in dets_person:
            best, best_iou = None, _IOU_MATCH
            for tr in self.tracks:
                if tr.id in used:
                    continue
                i = _iou(tr.bbox, bbox)
                if i >= best_iou:
                    best, best_iou = tr, i
            if best is None:
                best = _Track(self._next_id, bbox, now)
                self._next_id += 1
                self.tracks.append(best)
            best.bbox = bbox
            best.last_seen = now
            used.add(best.id)
            matched.append(best)
        # Evict stale tracks.
        self.tracks = [t for t in self.tracks if now - t.last_seen <= _TRACK_TTL_S]
        return matched


class Engine:
    def __init__(self, ingest, poster) -> None:
        """`ingest` = InferenceEngine, `poster` = callable(list[event]) → None."""
        self._infer = ingest
        self._post = poster
        self._cams: Dict[str, _CameraState] = {}
        self._cfg: List[dict] = []
        self._rr = 0  # round-robin cursor

    def set_config(self, cameras: List[dict]) -> None:
        """cameras = cameraAi.cameras from cloud /config."""
        self._cfg = cameras or []
        # Drop state for cameras no longer configured.
        ids = {c.get("cameraId") for c in self._cfg}
        for cid in list(self._cams):
            if cid not in ids:
                self._cams.pop(cid, None)

    def tick(self, rtsp_url_for) -> None:
        """Process ONE camera this tick (round-robin) so the loop stays bounded
        regardless of camera count. `rtsp_url_for(cameraId)` resolves the local
        RTSP URL (the integration knows the NVR creds/host)."""
        if not self._cfg or not self._infer.ready:
            return
        cam = self._cfg[self._rr % len(self._cfg)]
        self._rr += 1
        cid = cam.get("cameraId")
        if not cid:
            return
        url = rtsp_url_for(cid)
        if not url:
            return
        try:
            self._process_camera(cam, url)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("camera %s inference cycle errored (ignored): %s", cid, err)

    def _grab_frame(self, url: str):
        import cv2  # noqa: PLC0415

        cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
        try:
            ok, frame = cap.read()
            return frame if ok else None
        finally:
            cap.release()

    def _process_camera(self, cam: dict, url: str) -> None:
        cid = cam["cameraId"]
        area_id = cam.get("areaId")
        want_objects = set(cam.get("objectClasses") or [])
        want_actions = set(cam.get("detectActions") or [])
        detect_humans = cam.get("detectHumans", True)
        now = time.time()
        state = self._cams.setdefault(cid, _CameraState())

        frame = self._grab_frame(url)
        if frame is None:
            return

        dets = self._infer.detect(frame, want_objects or None)
        persons = [d[2] for d in dets if d[0] == "person"]
        events: List[dict] = []

        # ── Objects: census on a slow cadence (they don't move). ──────────────
        if want_objects and (now - state.last_object_census >= _OBJECT_CENSUS_S):
            state.last_object_census = now
            counts: Dict[str, int] = {}
            for cls, _cf, _b in dets:
                if cls in want_objects:
                    counts[cls] = counts.get(cls, 0) + 1
            for cls in want_objects:
                events.append(
                    {
                        "camera_id": cid,
                        "area_id": area_id,
                        "event_class": f"object_{cls}",
                        "count": counts.get(cls, 0),
                    }
                )

        # ── Humans: track + present + actions. ────────────────────────────────
        if detect_humans and persons:
            tracks = state.assign(persons, now)
            poses = self._infer.pose(frame) if want_actions else []
            for tr in tracks:
                # human_present heartbeat per track (deduped by cooldown).
                if now - tr.last_emit >= _ACTION_COOLDOWN_S:
                    tr.last_emit = now
                    events.append(
                        {
                            "camera_id": cid,
                            "area_id": area_id,
                            "event_class": "human_present",
                            "track_id": str(tr.id),
                            "bbox": _norm_bbox(tr.bbox, frame),
                        }
                    )
                if not want_actions or not poses:
                    continue
                pose = _nearest_pose(tr.bbox, poses)
                if not pose:
                    continue
                bbox_wh = (tr.bbox[2], tr.bbox[3])
                acts = classify_single_frame(pose, bbox_wh)
                acts += classify_temporal(tr.hist, _center(tr.bbox), pose, bbox_wh)
                for act in acts:
                    if act not in want_actions and not (act == "falling" and "falling" in want_actions):
                        continue
                    if act == "falling":
                        events.append(
                            {
                                "camera_id": cid,
                                "area_id": area_id,
                                "event_class": "alert_fall",
                                "track_id": str(tr.id),
                                "bbox": _norm_bbox(tr.bbox, frame),
                            }
                        )
                        tr.last_actions[act] = now
                        continue
                    last = tr.last_actions.get(act, 0.0)
                    if now - last >= _ACTION_COOLDOWN_S:
                        tr.last_actions[act] = now
                        events.append(
                            {
                                "camera_id": cid,
                                "area_id": area_id,
                                "event_class": f"action_{act}",
                                "track_id": str(tr.id),
                                "bbox": _norm_bbox(tr.bbox, frame),
                            }
                        )

        if events:
            self._post(events)


def _center(b):
    return (b[0] + b[2] / 2, b[1] + b[3] / 2)


def _norm_bbox(b, frame) -> dict:
    h, w = frame.shape[:2]
    return {
        "x": max(0.0, min(1.0, b[0] / w)),
        "y": max(0.0, min(1.0, b[1] / h)),
        "w": max(0.0, min(1.0, b[2] / w)),
        "h": max(0.0, min(1.0, b[3] / h)),
    }


def _nearest_pose(bbox, poses):
    """Pick the pose whose torso centroid is nearest the bbox center."""
    cx, cy = _center(bbox)
    best, best_d = None, 1e18
    for pose in poses:
        pts = [(x, y) for (x, y, c) in pose if c >= 0.3]
        if not pts:
            continue
        mx = sum(p[0] for p in pts) / len(pts)
        my = sum(p[1] for p in pts) / len(pts)
        d = (mx - cx) ** 2 + (my - cy) ** 2
        if d < best_d:
            best, best_d = pose, d
    return best
