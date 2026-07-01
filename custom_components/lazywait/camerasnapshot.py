"""In-process camera snapshot capture (near-live path, replaces WebRTC).

The near-live view works by HA capturing a fresh JPEG (~1 fps) for whichever
cameras the dashboard is watching RIGHT NOW and POSTing it to the cloud, which
caches the latest frame per camera and serves it to the dashboard poll. This is
the SIMPLE alternative to WebRTC signaling: no SDP, no TURN, no peer connection —
just a still image refreshed roughly once a second.

Capture is UNIVERSAL and in-process: ``homeassistant.components.camera``'s
``async_get_image(hass, entity_id)`` asks the camera entity itself for a current
frame (works for go2rtc, generic RTSP/ONVIF, Hikvision NVR channels, etc.) and
returns an ``Image`` with ``.content`` (raw JPEG bytes) and ``.content_type``.

This module NEVER raises: a missing camera, a slow NVR, or a moved HA symbol all
degrade to ``None`` so a single bad frame can never break the coordinator loop.
"""

from __future__ import annotations

import base64
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Import only for typing so this module stays importable in the bare
    # (HA-free) test environment — mirrors camera.py's HA-free-import stance.
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

# Per-capture timeout. Snapshots must feel live (~1 fps), so a camera that can't
# produce a frame quickly is skipped this tick rather than stalling the loop.
_SNAPSHOT_TIMEOUT_SECONDS = 8


async def capture_snapshot(
    hass: "HomeAssistant", entity_id: str
) -> tuple[bytes, str] | None:
    """Capture one current frame from a camera entity, in-process.

    Uses ``homeassistant.components.camera.async_get_image`` — the universal,
    provider-agnostic path that asks the entity for a still frame. Returns
    ``(jpeg_bytes, content_type)`` on success, or ``None`` on ANY failure
    (unknown entity, no image, timeout, HA symbol moved). Never raises — the
    caller treats ``None`` as "skip this camera this tick".

    The caller base64-encodes the bytes for upload; we return raw bytes so the
    encoding decision (and any future binary handling) stays with the caller.
    """
    if not entity_id or not entity_id.startswith("camera."):
        return None

    # Lazy, defensive import so this module stays importable in the bare
    # (HA-free) test environment and tolerates the symbol moving between HA
    # releases. Absent → we simply return None and the snapshot is skipped.
    try:
        from homeassistant.components.camera import (  # type: ignore
            async_get_image,
        )
    except Exception as err:  # noqa: BLE001 - missing API → skip
        _LOGGER.debug("camera.async_get_image unavailable: %s", err)
        return None

    try:
        image = await async_get_image(
            hass, entity_id, timeout=_SNAPSHOT_TIMEOUT_SECONDS
        )
    except Exception as err:  # noqa: BLE001 - never break the loop on a bad frame
        _LOGGER.debug("snapshot capture failed for %s (ignored): %s", entity_id, err)
        return None

    content = getattr(image, "content", None)
    if not content:
        _LOGGER.debug("snapshot for %s returned no bytes; skipping", entity_id)
        return None
    content_type = getattr(image, "content_type", None) or "image/jpeg"
    return content, content_type


def encode_snapshot(content: bytes) -> str:
    """Base64-encode raw JPEG bytes to the ASCII string the cloud expects.

    The cloud wants the base64 WITHOUT a ``data:`` prefix, which is exactly what
    ``b64encode`` yields once decoded to ASCII.
    """
    return base64.b64encode(content).decode("ascii")
