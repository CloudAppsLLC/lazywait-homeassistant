"""Model loading + inference — YOLO11n detection + YOLO11n-pose via ONNX Runtime.

Auto-selects the best available execution provider: a Hailo/Coral accelerator if
present (checked by import), else ONNX Runtime CPU. Models (~6 MB each) are
bundled in the add-on image at MODELS_DIR. Kept deliberately small + dependency-
light so it runs on a Raspberry Pi 5 (quad A76, 8 GB) without a GPU.

This module is import-guarded: if onnxruntime or the models are missing the
engine reports unavailable and the supervisor logs once + skips inference (the
snapshot + streaming paths keep working). Never crashes the add-on.
"""

from __future__ import annotations

import logging
import os
from typing import List, Optional, Tuple

_LOGGER = logging.getLogger(__name__)

MODELS_DIR = os.environ.get("LW_MODELS_DIR", "/app/inference/models")
DET_MODEL = os.path.join(MODELS_DIR, "yolo11n.onnx")
POSE_MODEL = os.path.join(MODELS_DIR, "yolo11n-pose.onnx")

# COCO class id → our object-class catalog name (subset we expose in the UI).
COCO_TO_CLASS = {
    0: "person",
    39: "bottle",
    41: "cup",
    45: "bowl",
    56: "chair",
    57: "couch",
    58: "potted_plant",
    60: "table",  # COCO 'dining table'
    62: "tv",
    63: "laptop",
    73: "book",
    74: "clock",
    72: "refrigerator",
}

Detection = Tuple[str, float, Tuple[float, float, float, float]]  # (cls, conf, xywh px)


class InferenceEngine:
    """Wraps the detection + pose ONNX sessions. Lazy-loaded, best-effort."""

    def __init__(self) -> None:
        self._det = None
        self._pose = None
        self._provider = "none"
        self._ready = False
        self._load()

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def provider(self) -> str:
        return self._provider

    def _load(self) -> None:
        try:
            import onnxruntime as ort  # noqa: PLC0415
        except Exception as err:  # noqa: BLE001
            _LOGGER.error("onnxruntime unavailable; inference disabled: %s", err)
            return

        # Provider preference: Hailo/Coral if their EP is registered, else CPU.
        available = ort.get_available_providers()
        providers: List[str] = []
        for pref in ("HailoExecutionProvider", "CoralExecutionProvider"):
            if pref in available:
                providers.append(pref)
        providers.append("CPUExecutionProvider")
        self._provider = providers[0]

        opts = ort.SessionOptions()
        # Bound threads so inference can't starve HA + ffmpeg on the shared Pi.
        opts.intra_op_num_threads = int(os.environ.get("LW_INFER_THREADS", "2"))

        try:
            if os.path.exists(DET_MODEL):
                self._det = ort.InferenceSession(DET_MODEL, sess_options=opts, providers=providers)
            if os.path.exists(POSE_MODEL):
                self._pose = ort.InferenceSession(POSE_MODEL, sess_options=opts, providers=providers)
        except Exception as err:  # noqa: BLE001
            _LOGGER.error("failed to load ONNX models from %s: %s", MODELS_DIR, err)
            return

        self._ready = self._det is not None
        if self._ready:
            _LOGGER.info(
                "inference engine ready (provider=%s, pose=%s)",
                self._provider,
                self._pose is not None,
            )
        else:
            _LOGGER.error("detection model missing at %s; inference disabled", DET_MODEL)

    def detect(self, frame, want_classes: Optional[set]) -> List[Detection]:
        """Run object detection on a BGR frame. Returns detections whose class is
        'person' or in want_classes. Import numpy lazily (keeps module import-safe)."""
        if not self._det:
            return []
        try:
            import numpy as np  # noqa: PLC0415

            blob, scale, pad = _preprocess(frame, np)
            out = self._det.run(None, {self._det.get_inputs()[0].name: blob})[0]
            return _postprocess_det(out, scale, pad, want_classes, np)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("detect failed: %s", err)
            return []

    def pose(self, frame) -> List[List[Tuple[float, float, float]]]:
        """Run pose estimation; returns one 17-keypoint list per detected person."""
        if not self._pose:
            return []
        try:
            import numpy as np  # noqa: PLC0415

            blob, scale, pad = _preprocess(frame, np)
            out = self._pose.run(None, {self._pose.get_inputs()[0].name: blob})[0]
            return _postprocess_pose(out, scale, pad, np)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("pose failed: %s", err)
            return []


# ── Pre/post-processing (letterbox to 640, standard YOLO decode) ──────────────
_INPUT = 640


def _preprocess(frame, np):
    h, w = frame.shape[:2]
    scale = _INPUT / max(h, w)
    nh, nw = int(round(h * scale)), int(round(w * scale))
    import cv2  # noqa: PLC0415

    resized = cv2.resize(frame, (nw, nh))
    canvas = np.full((_INPUT, _INPUT, 3), 114, dtype=np.uint8)
    pad_y, pad_x = (_INPUT - nh) // 2, (_INPUT - nw) // 2
    canvas[pad_y : pad_y + nh, pad_x : pad_x + nw] = resized
    blob = canvas[:, :, ::-1].transpose(2, 0, 1).astype(np.float32) / 255.0
    return blob[None], scale, (pad_x, pad_y)


def _postprocess_det(out, scale, pad, want_classes, np, conf_th=0.35):
    # YOLO11 export: (1, 84, N) → transpose to (N, 84): 4 bbox + 80 class scores.
    pred = np.squeeze(out)
    if pred.ndim != 2:
        return []
    if pred.shape[0] < pred.shape[1]:
        pred = pred.T
    boxes = pred[:, :4]
    scores = pred[:, 4:]
    cls_ids = scores.argmax(1)
    confs = scores.max(1)
    keep = confs > conf_th
    dets: List[Detection] = []
    px, py = pad
    for (cx, cy, bw, bh), cid, cf in zip(boxes[keep], cls_ids[keep], confs[keep]):
        name = COCO_TO_CLASS.get(int(cid))
        if not name:
            continue
        if name != "person" and (want_classes is not None and name not in want_classes):
            continue
        x = (cx - bw / 2 - px) / scale
        y = (cy - bh / 2 - py) / scale
        dets.append((name, float(cf), (float(x), float(y), float(bw / scale), float(bh / scale))))
    return dets


def _postprocess_pose(out, scale, pad, np, conf_th=0.4):
    # YOLO11-pose export: (1, 56, N) → (N, 56): 4 bbox + 1 conf + 17*3 keypoints.
    pred = np.squeeze(out)
    if pred.ndim != 2:
        return []
    if pred.shape[0] < pred.shape[1]:
        pred = pred.T
    px, py = pad
    poses = []
    for row in pred:
        if row[4] < conf_th:
            continue
        kps = row[5:].reshape(-1, 3)
        pose = [
            ((kx - px) / scale, (ky - py) / scale, float(kc))
            for kx, ky, kc in kps
        ]
        poses.append(pose)
    return poses
