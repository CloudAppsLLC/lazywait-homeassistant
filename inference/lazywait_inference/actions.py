"""Pose → action/fall classification from YOLO keypoints.

Single-frame heuristics (standing, sitting, raising_hand) read one pose; temporal
heuristics (walking, waving, falling) read a short per-track history. All rules
are explainable geometry on the 17 COCO keypoints — no extra model — so they run
cheaply on the Pi CPU and are trivial to tune. A v2 can swap in ST-GCN on the
same history buffer without changing the event contract.

COCO-17 keypoint order:
 0 nose 1 l_eye 2 r_eye 3 l_ear 4 r_ear 5 l_sho 6 r_sho 7 l_elb 8 r_elb
 9 l_wri 10 r_wri 11 l_hip 12 r_hip 13 l_knee 14 r_knee 15 l_ank 16 r_ank
"""

from __future__ import annotations

from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

# Keypoint indices.
NOSE = 0
L_SHO, R_SHO = 5, 6
L_ELB, R_ELB = 7, 8
L_WRI, R_WRI = 9, 10
L_HIP, R_HIP = 11, 12
L_KNEE, R_KNEE = 13, 14
L_ANK, R_ANK = 15, 16

# A keypoint below this confidence is treated as missing.
_KP_CONF = 0.3

Keypoint = Tuple[float, float, float]  # (x, y, conf) — pixels + confidence
Pose = List[Keypoint]


def _pt(pose: Pose, i: int) -> Optional[Tuple[float, float]]:
    if i >= len(pose):
        return None
    x, y, c = pose[i]
    return (x, y) if c >= _KP_CONF else None


def _avg(a: Optional[Tuple[float, float]], b: Optional[Tuple[float, float]]):
    if a and b:
        return ((a[0] + b[0]) / 2, (a[1] + b[1]) / 2)
    return a or b


def classify_single_frame(pose: Pose, bbox_wh: Tuple[float, float]) -> List[str]:
    """Actions derivable from ONE pose. Returns a list (a pose can be e.g. both
    'sitting' and 'raising_hand'). Image y grows DOWNWARD."""
    out: List[str] = []
    sho = _avg(_pt(pose, L_SHO), _pt(pose, R_SHO))
    hip = _avg(_pt(pose, L_HIP), _pt(pose, R_HIP))
    knee = _avg(_pt(pose, L_KNEE), _pt(pose, R_KNEE))

    # Raising hand: either wrist clearly above (smaller y than) the shoulders.
    if sho:
        for wri_i in (L_WRI, R_WRI):
            w = _pt(pose, wri_i)
            if w and w[1] < sho[1] - 0.05 * bbox_wh[1]:
                out.append("raising_hand")
                break

    # Standing vs sitting from the torso/leg geometry. When the knee is close to
    # the hip vertically (thigh folded / seated) → sitting; when hip→knee spans a
    # large share of the body height → standing.
    if sho and hip and knee:
        torso = abs(hip[1] - sho[1]) or 1.0
        thigh = abs(knee[1] - hip[1])
        if thigh < 0.6 * torso:
            out.append("sitting")
        else:
            out.append("standing")
    elif sho and hip:
        # No knees visible (e.g. behind a counter) → default to standing.
        out.append("standing")

    return out


class TrackHistory:
    """Rolling per-track state for temporal actions (walking, waving, falling)."""

    __slots__ = ("centroids", "wrist_y", "aspect", "last_ts")

    def __init__(self) -> None:
        self.centroids: Deque[Tuple[float, float]] = deque(maxlen=12)
        self.wrist_y: Deque[float] = deque(maxlen=12)
        self.aspect: Deque[float] = deque(maxlen=8)
        self.last_ts: float = 0.0


def classify_temporal(
    hist: TrackHistory,
    centroid: Tuple[float, float],
    pose: Pose,
    bbox_wh: Tuple[float, float],
) -> List[str]:
    """Update history with this frame and return temporal actions detected."""
    out: List[str] = []
    w, h = bbox_wh
    hist.centroids.append(centroid)
    hist.aspect.append((w / h) if h else 0.0)

    # Walking: centroid moved a meaningful fraction of body width across the
    # recent window (filters jitter; not just any pixel drift).
    if len(hist.centroids) >= 6:
        dx = hist.centroids[-1][0] - hist.centroids[0][0]
        dy = hist.centroids[-1][1] - hist.centroids[0][1]
        dist = (dx * dx + dy * dy) ** 0.5
        if dist > 0.6 * max(w, 1.0):
            out.append("walking")

    # Waving: wrist y oscillates (up/down) repeatedly near/above the shoulder.
    r = _pt(pose, R_WRI) or _pt(pose, L_WRI)
    if r:
        hist.wrist_y.append(r[1])
        if len(hist.wrist_y) >= 6:
            ys = list(hist.wrist_y)[-6:]
            reversals = sum(
                1
                for i in range(1, len(ys) - 1)
                if (ys[i] - ys[i - 1]) * (ys[i + 1] - ys[i]) < 0
            )
            span = max(ys) - min(ys)
            if reversals >= 2 and span > 0.08 * h:
                out.append("waving")

    # Falling: bbox aspect flips from tall (standing, w/h < 1) to wide
    # (horizontal, w/h > ~1.2) AND the torso is roughly horizontal. Sustained
    # over the window to avoid a fast-sit false positive.
    if len(hist.aspect) >= 4:
        recent = list(hist.aspect)
        was_tall = min(recent[:2]) < 0.9
        now_wide = recent[-1] > 1.2
        sho = _avg(_pt(pose, L_SHO), _pt(pose, R_SHO))
        hip = _avg(_pt(pose, L_HIP), _pt(pose, R_HIP))
        torso_horizontal = False
        if sho and hip:
            torso_horizontal = abs(hip[1] - sho[1]) < abs(hip[0] - sho[0])
        if was_tall and now_wide and torso_horizontal:
            out.append("falling")

    return out
