"""Hikvision camera → LazyWait face-attendance bridge.

A Hikvision IP camera detects a person/face on-device; this module captures a
JPEG of that face and hands it to the cloud face-checkin endpoint (via
`LazyWaitApiClient.face_checkin`), which recognises the employee and toggles
their clock IN/OUT.

Two capture strategies exist on Hikvision's ISAPI:

1. SNAPSHOT (implemented here, the reliable default)
   A single still off the channel:
       GET http://<host>/ISAPI/Streaming/channels/101/picture
   Digest-authenticated, returns a JPEG. Channel `101` is the main stream of
   the first channel (a NVR exposes `<chan>01`). This works on essentially every
   Hikvision model — DVR, NVR, and standalone IP cams — which is why it's the
   default. The trade-off: it grabs whatever is in frame *now*, so the
   automation should trigger it off the camera's own face/motion event so the
   subject is actually present.

2. SMART-EVENT STREAM (documented, the preferred future path — NOT implemented)
   For true on-device face detection, Hikvision streams events over a long-lived
   multipart connection:
       GET http://<host>/ISAPI/Event/notification/alertStream
   It pushes `<EventNotificationAlert>` XML chunks (eventType `VMD`/`facedetect`/
   `linedetect` etc.). The face-detection variants can embed a cropped face JPEG
   (base64 in `<detectionPicTransType>`/`<facePicData>`), which would let us
   forward the *exact* detected face with no extra round-trip and no "who was in
   frame" ambiguity. Implementing it means holding a persistent aiohttp stream,
   parsing the multipart/mixed boundary, and extracting the embedded picture —
   meaningfully more moving parts than a snapshot, and the face-pic payload is
   gated behind per-model "Smart"/"Face Capture" licensing. We ship the snapshot
   path now (works everywhere) and leave this as the upgrade.

Wiring: an HA automation triggers on the camera's face/motion event and calls a
service that runs `async_handle_face_event` → capture → `api.face_checkin`. See
`async_handle_face_event` below and the README's "Face attendance (Hikvision)".
"""

from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any

import aiohttp

_LOGGER = logging.getLogger(__name__)

# Hikvision snapshot path. 101 = channel 1, main stream — the universally
# supported still-picture endpoint across DVR/NVR/IP models.
_SNAPSHOT_PATH = "/ISAPI/Streaming/channels/101/picture"

# A snapshot is a small still; cap the wait so a slow/offline camera can't hang
# the automation. Generous enough for a 1080p JPEG over LAN.
_SNAPSHOT_TIMEOUT_SECONDS = 10

# Don't forward an empty/garbage body as a "face". A real channel JPEG is well
# over this; a few hundred bytes is an error page or a black frame.
_MIN_JPEG_BYTES = 1024


async def capture_face_jpeg_base64(
    session: aiohttp.ClientSession,
    host: str,
    username: str,
    password: str,
) -> str | None:
    """Capture a snapshot from a Hikvision camera and return it base64-encoded.

    GETs the ISAPI channel-101 picture with Digest auth and returns the JPEG as
    a base64 string with NO data-URL prefix (the cloud face-checkin endpoint
    wants raw base64). Returns None on any failure — a missed frame must never
    raise into the automation; the next detection event simply tries again.

    Args:
        session: the shared HA aiohttp session (caller owns its lifecycle).
        host: camera host or host:port, no scheme (e.g. "192.168.1.64").
        username: ISAPI user (needs picture/preview rights).
        password: that user's password.

    Returns:
        Base64-encoded JPEG bytes, or None if capture failed.
    """
    host = (host or "").strip().rstrip("/")
    if not host:
        _LOGGER.warning("Hikvision capture skipped: no host configured")
        return None

    # Digest is Hikvision's default for ISAPI and what the snapshot URL expects.
    # aiohttp ships DigestAuth only in recent versions; fall back to BasicAuth on
    # older cores (Hikvision accepts Basic when the camera's auth mode allows it).
    auth = _build_digest_auth(username, password) or aiohttp.BasicAuth(
        username, password
    )

    url = f"http://{host}{_SNAPSHOT_PATH}"
    timeout = aiohttp.ClientTimeout(total=_SNAPSHOT_TIMEOUT_SECONDS)

    try:
        async with session.get(url, auth=auth, timeout=timeout) as resp:
            if resp.status != 200:
                _LOGGER.warning(
                    "Hikvision snapshot %s returned HTTP %s", host, resp.status
                )
                return None
            data = await resp.read()
    except asyncio.TimeoutError:
        _LOGGER.warning("Hikvision snapshot %s timed out", host)
        return None
    except aiohttp.ClientError as err:
        _LOGGER.warning("Hikvision snapshot %s failed: %s", host, err)
        return None

    if not data or len(data) < _MIN_JPEG_BYTES:
        _LOGGER.warning(
            "Hikvision snapshot %s returned %s bytes (too small to be a frame)",
            host,
            len(data) if data else 0,
        )
        return None

    return base64.b64encode(data).decode("ascii")


def _build_digest_auth(username: str, password: str) -> Any | None:
    """Return an aiohttp DigestAuth if this aiohttp build has one, else None.

    aiohttp added `DigestAuth` relatively recently; on older cores the name is
    absent. We resolve it dynamically so the module imports everywhere — when
    it's missing we fall back to BasicAuth at the call site (Hikvision accepts
    Basic on the snapshot URL when the camera's auth mode allows it).
    """
    digest_cls = getattr(aiohttp, "DigestAuth", None)
    if digest_cls is None:
        _LOGGER.debug(
            "aiohttp.DigestAuth unavailable; falling back to Basic auth for "
            "Hikvision snapshot"
        )
        return None
    try:
        return digest_cls(username, password)
    except Exception:  # pragma: no cover - defensive; never block capture
        _LOGGER.debug("aiohttp.DigestAuth construction failed; using Basic auth")
        return None


async def async_handle_face_event(
    hass: Any,
    entry: Any,
    *,
    host: str | None = None,
    username: str | None = None,
    password: str | None = None,
    image_base64: str | None = None,
) -> dict[str, Any] | None:
    """Capture a face (or accept a pre-captured one) and post it to the cloud.

    This is the single entry point an HA automation/service or the coordinator
    calls when a Hikvision camera reports a face/person. It either:
      * uses `image_base64` if the caller already has the frame (e.g. extracted
        from a smart-event payload, or grabbed from an HA camera entity), or
      * captures a fresh snapshot from (`host`, `username`, `password`).
    then forwards it via `LazyWaitApiClient.face_checkin`, attributing it to the
    entry's paired branch.

    Returns the cloud's face-checkin result (matched/recorded/action/...), or
    None if nothing could be captured or the post failed. Never raises — a
    camera event must not break the automation engine.

    The LazyWait client + branch id are read from `hass.data[DOMAIN][entry_id]`
    (the coordinator), so the automation only needs to identify the camera.
    """
    # Imported lazily to avoid a circular import at module load (api/const are
    # light, but keep the camera helper standalone-importable).
    from .const import DOMAIN

    coordinator = (hass.data.get(DOMAIN, {}) or {}).get(entry.entry_id)
    if coordinator is None:
        _LOGGER.warning(
            "Hikvision face event ignored: no LazyWait coordinator for entry %s",
            getattr(entry, "entry_id", "?"),
        )
        return None

    client = coordinator._client  # noqa: SLF001 - same package, intentional reuse
    branch_id = coordinator.branch_id

    photo_base64 = image_base64
    if not photo_base64:
        session = client._session  # noqa: SLF001 - reuse the paired session
        photo_base64 = await capture_face_jpeg_base64(
            session, host or "", username or "", password or ""
        )

    if not photo_base64:
        _LOGGER.warning("Hikvision face event: no image captured; nothing posted")
        return None

    try:
        result = await client.face_checkin(photo_base64, branch_id=branch_id)
    except Exception as err:  # noqa: BLE001 - log + drop; never raise into HA
        _LOGGER.warning("LazyWait face-checkin failed: %s", err)
        return None

    if result.get("matched") and result.get("recorded"):
        _LOGGER.info(
            "LazyWait face-checkin: %s %s (similarity %s)",
            result.get("employeeName") or result.get("employeeId"),
            result.get("action"),
            result.get("similarity"),
        )
    elif result.get("matched"):
        _LOGGER.info(
            "LazyWait face-checkin matched %s but not recorded (%s)",
            result.get("employeeName") or result.get("employeeId"),
            result.get("reason"),
        )
    else:
        _LOGGER.debug("LazyWait face-checkin: no employee matched")

    return result
