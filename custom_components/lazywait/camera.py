"""Live camera WebRTC answerer — bridges a cloud SDP offer to a local camera.

The dashboard is one WebRTC peer; HA/go2rtc is the other. The cloud relays
SIGNALING ONLY (SDP offer/answer; ICE candidates bundled non-trickle inside the
SDP). Media flows peer-to-peer over Twilio TURN — it never touches the cloud.
HA is outbound-only behind NAT, so it cannot accept an inbound WebRTC peer
connection directly; instead it POLLS the cloud for a pending offer, produces an
answer for that offer against a named camera/stream, and POSTs the answer back
to the cloud.

PRIMARY path — HA's NATIVE camera WebRTC API (no port guessing)
===============================================================
The integration runs IN-PROCESS and holds the ``hass`` object, so it asks HA's
own camera component to answer the offer via
``Camera.async_handle_async_webrtc_offer(offer, session_id, send_message)``.
HA Core already owns a correctly configured WebRTC provider — the go2rtc
integration registered itself as a ``CameraWebRTCProvider`` at go2rtc's REAL
url during its own setup — so HA routes the offer there with NO loopback port
assumptions and NO HA token. This fixes the historic failure where POSTing the
offer to a guessed go2rtc loopback port (11984 / 1984) returned "Cannot connect
to host" and the dashboard polled forever (status:"offered", answer:null).
See ``_answer_via_ha_native`` below.

go2rtc handshake — FALLBACK ONLY (last resort if the native path can't answer)
==============================================================================
go2rtc exposes a one-shot WebRTC exchange: you POST an SDP *offer* and it
returns an SDP *answer* for a named source stream. This is now a FALLBACK,
tried only when the native camera API is unavailable / the entity has no WebRTC
support / it timed out. The exact route depends on how go2rtc is reached:

  * Standalone go2rtc (default API port 1984):
        POST http://<go2rtc-host>:1984/api/webrtc?src=<stream_id>
        body: { "type": "offer", "sdp": "<offer sdp>" }
        200 : { "type": "answer", "sdp": "<answer sdp>" }

  * HA-bundled go2rtc, proxied through Supervisor / HA:
        POST http://<ha-host>:8123/api/go2rtc/webrtc?src=<stream_id>
    (HA forwards to its internal go2rtc; auth is the HA long-lived token.)

Both shapes are implemented below behind ``_ATTEMPTS`` and tried in order; the
first 200 with an ``sdp`` wins. **The precise path + query + body for the HA
build of go2rtc is the single thing to confirm against a live HA instance** —
go2rtc's API has shifted between releases (some builds want ``/api/webrtc``,
some accept the offer as a base64 ``data=`` query param, some namespace it under
``/api/ws`` for the websocket variant). If none of the attempts answer, we fall
back to returning the stream's RTSP/info so the caller can degrade gracefully
(log it, surface "go2rtc handshake unconfirmed") rather than silently failing.

Stream id resolution
====================
``cameraId`` from the cloud offer identifies which go2rtc stream to answer with.
It may be:
  * a go2rtc stream name already configured in go2rtc.yaml (used as-is), or
  * "" (empty) → fall back to the configured default stream id, or
  * an HA ``camera.*`` entity id → go2rtc in HA registers HA cameras under their
    entity id, so it is also usable as ``src`` directly.

No secrets are committed: the go2rtc base URL + optional HA token come from the
config entry / HA runtime, never hard-coded.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import aiohttp

_LOGGER = logging.getLogger(__name__)

# Cap the go2rtc handshake so a wedged go2rtc can't hang the poll loop.
_HANDSHAKE_TIMEOUT_SECONDS = 15

# HA-bundled go2rtc (HA 2024.11+) binds its HTTP API on 11984 — the standalone
# go2rtc default 1984 prefixed with "1" to avoid a port clash. An in-process
# custom component reaches it DIRECTLY on loopback; there is NO HA proxy path
# (a live probe of /api/go2rtc/* returns 404 on HA's REST API). Overridable per
# entry. 1984 is tried only as a fallback for external/standalone go2rtc.
DEFAULT_GO2RTC_BASE_URL = "http://127.0.0.1:11984"

# Legacy/standalone go2rtc API port — fallback only.
LEGACY_GO2RTC_BASE_URL = "http://127.0.0.1:1984"

# HA base is retained for REST state enumeration (list_cameras) only — NOT for
# the go2rtc handshake (the bundled go2rtc is not proxied through HA).
DEFAULT_HA_BASE_URL = "http://127.0.0.1:8123"


@dataclass(frozen=True)
class Go2RtcTarget:
    """Where to reach go2rtc + how to authenticate the handshake.

    go2rtc_base_url: standalone go2rtc API (e.g. http://127.0.0.1:1984).
    ha_base_url:     HA base for the proxied /api/go2rtc form.
    ha_token:        HA long-lived token for the proxied form (None → omit auth,
                     valid when go2rtc is reached directly on the LAN).
    """

    go2rtc_base_url: str = DEFAULT_GO2RTC_BASE_URL
    ha_base_url: str = DEFAULT_HA_BASE_URL
    ha_token: str | None = None


@dataclass(frozen=True)
class CameraAnswer:
    """Result of an offer→answer attempt.

    answer_sdp: the SDP answer to POST back to the cloud, or None if go2rtc did
                not produce one (then `fallback` carries degraded info).
    fallback:   diagnostic / RTSP info when the handshake could not complete, so
                the caller can log + surface "go2rtc handshake unconfirmed".
    """

    answer_sdp: str | None
    fallback: dict[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return bool(self.answer_sdp)


def _resolve_stream_id(camera_id: str, default_stream_id: str) -> str:
    """Pick the go2rtc ``src`` stream name for this offer."""
    cid = (camera_id or "").strip()
    if cid:
        return cid
    return (default_stream_id or "").strip()


def _build_attempts(
    target: Go2RtcTarget, stream_id: str
) -> list[dict[str, Any]]:
    """Ordered list of go2rtc handshake attempts to try until one answers.

    Each attempt is { url, headers, json }. They cover the standalone go2rtc
    API and the HA-proxied form; the first that returns a 200 with an ``sdp``
    wins. See the module docstring — the HA-proxied shape is the one to confirm
    live.
    """
    attempts: list[dict[str, Any]] = []

    go2rtc_base = target.go2rtc_base_url.rstrip("/")
    # go2rtc expects the offer as an SDP body; src selects the published stream.
    query = f"?src={quote(stream_id, safe='')}" if stream_id else ""

    # 1) HA-bundled go2rtc, DIRECT on its loopback API (default 11984). This is
    #    the real path for an in-process custom component — no auth on loopback.
    attempts.append(
        {
            "url": f"{go2rtc_base}/api/webrtc{query}",
            "headers": {"Content-Type": "application/json"},
            # body filled per-call with the offer SDP (see answer_offer).
        }
    )

    # 2) Legacy/standalone go2rtc on 1984 (only if the configured base wasn't
    #    already 1984) — covers an external go2rtc / add-on deployment.
    legacy_base = LEGACY_GO2RTC_BASE_URL.rstrip("/")
    if legacy_base != go2rtc_base:
        attempts.append(
            {
                "url": f"{legacy_base}/api/webrtc{query}",
                "headers": {"Content-Type": "application/json"},
            }
        )

    return attempts


async def _answer_via_ha_native(
    hass: Any,
    *,
    entity_id: str,
    offer_sdp: str,
) -> str | None:
    """Get an SDP answer from HA's native camera WebRTC API for a camera entity.

    This is the ROBUST path: HA Core's camera component already owns a correctly
    configured WebRTC provider (the go2rtc integration registered itself at
    go2rtc's REAL url during its own setup). We hand the offer to the camera
    entity and let HA route it — no port guessing, no loopback assumptions, no
    HA token. Works for any camera with ``CameraEntityFeature.STREAM`` (Hikvision
    NVR channels through go2rtc qualify).

    ``async_handle_async_webrtc_offer`` is fire-and-forget: it returns ``None``
    and delivers the result later through the ``send_message`` callback. We
    bridge that into an awaitable Future resolved on the first
    ``WebRTCAnswer`` / ``WebRTCError``. Older HA builds (pre-unified provider
    API) instead expose ``async_handle_web_rtc_offer(offer) -> str`` which
    returns the answer SDP directly; we try that as a secondary native shape.

    Returns the answer SDP, or ``None`` if HA has no camera entity / no WebRTC
    provider / errored / timed out — the caller then falls back to the go2rtc
    POST. NEVER raises.
    """
    if hass is None or not entity_id or not entity_id.startswith("camera."):
        return None

    # Lazy, defensive imports — keep camera.py importable in the bare (HA-free)
    # test environment, and tolerate the camera WebRTC symbols moving between HA
    # releases. If they're absent we degrade to the go2rtc-POST fallback.
    try:
        from homeassistant.components.camera import (  # type: ignore
            CameraEntityFeature,
            get_camera_from_entity_id,
        )
    except Exception as err:  # noqa: BLE001 - missing API → use fallback
        _LOGGER.debug("HA native camera API unavailable: %s", err)
        return None

    try:
        camera = get_camera_from_entity_id(hass, entity_id)
    except Exception as err:  # noqa: BLE001 - HomeAssistantError if not found
        _LOGGER.debug("camera entity %s not resolvable for native WebRTC: %s", entity_id, err)
        return None

    # Camera must advertise STREAM to participate in WebRTC.
    try:
        has_stream = bool(
            int(getattr(camera, "supported_features", 0)) & int(CameraEntityFeature.STREAM)
        )
    except Exception:  # noqa: BLE001 - odd feature shape → assume usable, let HA decide
        has_stream = True
    if not has_stream:
        _LOGGER.debug("camera %s has no STREAM feature; native WebRTC skipped", entity_id)
        return None

    # Preferred: the unified async provider API (HA 2024.11+ through 2026.6.x).
    handle_async = getattr(camera, "async_handle_async_webrtc_offer", None)
    if callable(handle_async):
        try:
            from homeassistant.components.camera.webrtc import (  # type: ignore
                WebRTCAnswer,
                WebRTCError,
            )
        except Exception as err:  # noqa: BLE001 - message types moved → try legacy shape
            _LOGGER.debug("HA WebRTC message types unavailable: %s", err)
        else:
            loop = asyncio.get_running_loop()
            answer_future: asyncio.Future[str | None] = loop.create_future()

            def _send_message(message: Any) -> None:
                # Resolve on the FIRST terminal message (answer or error). ICE
                # candidates ride non-trickle inside the offer, so we only need
                # the answer SDP; intermediate WebRTCCandidate messages are
                # ignored here.
                if answer_future.done():
                    return
                if isinstance(message, WebRTCAnswer):
                    answer_future.set_result(message.sdp)
                elif isinstance(message, WebRTCError):
                    _LOGGER.debug(
                        "HA native WebRTC error for %s: %s/%s",
                        entity_id,
                        getattr(message, "code", "?"),
                        getattr(message, "message", "?"),
                    )
                    answer_future.set_result(None)

            session_id = uuid.uuid4().hex
            try:
                # Fire-and-forget: returns None; the answer comes via _send_message.
                await handle_async(offer_sdp, session_id, _send_message)
                answer = await asyncio.wait_for(
                    answer_future, timeout=_HANDSHAKE_TIMEOUT_SECONDS
                )
                if answer:
                    _LOGGER.info(
                        "HA native camera WebRTC answered offer for %s", entity_id
                    )
                return answer
            except (asyncio.TimeoutError, asyncio.CancelledError) as err:
                _LOGGER.debug("HA native WebRTC offer timed out for %s: %s", entity_id, err)
                return None
            except Exception as err:  # noqa: BLE001 - any provider error → fallback
                _LOGGER.debug("HA native WebRTC offer failed for %s: %s", entity_id, err)
                return None
            finally:
                # Best-effort teardown so the provider doesn't hold the session open.
                close = getattr(camera, "close_webrtc_session", None)
                if callable(close):
                    try:
                        close(session_id)
                    except Exception:  # noqa: BLE001 - teardown is best-effort
                        pass

    # Legacy native shape (older HA): returns the answer SDP directly.
    handle_sync = getattr(camera, "async_handle_web_rtc_offer", None)
    if callable(handle_sync):
        try:
            answer = await asyncio.wait_for(
                handle_sync(offer_sdp), timeout=_HANDSHAKE_TIMEOUT_SECONDS
            )
        except Exception as err:  # noqa: BLE001 - any error → fallback
            _LOGGER.debug("HA legacy native WebRTC offer failed for %s: %s", entity_id, err)
            return None
        if isinstance(answer, str) and answer.strip():
            _LOGGER.info(
                "HA native camera WebRTC (legacy) answered offer for %s", entity_id
            )
            return answer
        return None

    _LOGGER.debug("camera %s exposes no native WebRTC offer handler", entity_id)
    return None


async def answer_offer(
    session: aiohttp.ClientSession,
    target: Go2RtcTarget,
    *,
    camera_id: str,
    offer_sdp: str,
    default_stream_id: str = "",
    rtsp_fallback_url: str | None = None,
    hass: Any = None,
) -> CameraAnswer:
    """Answer a cloud WebRTC offer — HA-native first, go2rtc POST as fallback.

    Resolution order:
      1. **HA native** (``_answer_via_ha_native``) when ``hass`` is available and
         ``camera_id`` is a ``camera.*`` entity id — hands the offer to HA's
         camera component, which routes to the correctly configured go2rtc
         provider with NO port guessing. This is the primary, reliable path.
      2. **go2rtc POST fallback** (see ``_build_attempts``) — the historic
         loopback-port handshake, tried ONLY if the native path produced no
         answer. On the first 200 carrying an SDP answer, returns
         ``CameraAnswer(answer_sdp=...)``.

    If neither produces an answer, returns a ``CameraAnswer`` with
    ``answer_sdp=None`` and a ``fallback`` dict describing what was tried
    (native error + go2rtc statuses) + the RTSP/stream info, so the caller can
    log a precise "no answer" reason and (optionally) surface the RTSP URL for a
    non-WebRTC viewer.

    Never raises — a failed handshake must not break the coordinator poll loop.

    Args:
        session: shared HA aiohttp session (used by the go2rtc fallback only).
        target: go2rtc / HA endpoints + optional HA token.
        camera_id: stream id / ``camera.*`` entity id from the cloud offer
            ("" → default_stream_id for the go2rtc fallback).
        offer_sdp: the dashboard's SDP offer (ICE bundled non-trickle).
        default_stream_id: go2rtc stream to use when camera_id is empty.
        rtsp_fallback_url: optional RTSP URL to include in the fallback info.
        hass: in-process HomeAssistant object; enables the native path.
    """
    # 1) NATIVE first — let HA's camera component answer using its correctly
    #    configured WebRTC provider (no loopback port guessing). Requires hass +
    #    a camera.* entity id. Never raises.
    native_error: str | None = None
    if hass is not None and (camera_id or "").startswith("camera."):
        native_answer = await _answer_via_ha_native(
            hass, entity_id=camera_id, offer_sdp=offer_sdp
        )
        if native_answer:
            return CameraAnswer(answer_sdp=native_answer)
        native_error = "native path returned no answer (see debug log)"

    # 2) go2rtc POST fallback (legacy loopback handshake).
    stream_id = _resolve_stream_id(camera_id, default_stream_id)
    if not stream_id:
        _LOGGER.warning(
            "Live camera: no answer — native path: %s; and no go2rtc stream id "
            "(camera_id empty and no default).",
            native_error or "not attempted (no hass / not a camera.* entity)",
        )
        return CameraAnswer(
            answer_sdp=None,
            fallback={
                "reason": "no_stream_id",
                "native_error": native_error,
                "rtsp": rtsp_fallback_url,
            },
        )

    offer_body = {"type": "offer", "sdp": offer_sdp}
    timeout = aiohttp.ClientTimeout(total=_HANDSHAKE_TIMEOUT_SECONDS)
    tried: list[dict[str, Any]] = []

    for attempt in _build_attempts(target, stream_id):
        url = attempt["url"]
        headers = attempt["headers"]
        try:
            async with session.post(
                url, json=offer_body, headers=headers, timeout=timeout
            ) as resp:
                status = resp.status
                payload = await _safe_json(resp)
                answer_sdp = _extract_answer_sdp(payload)
                if status == 200 and answer_sdp:
                    _LOGGER.info(
                        "go2rtc answered offer for stream %s via %s",
                        stream_id,
                        url,
                    )
                    return CameraAnswer(answer_sdp=answer_sdp)
                tried.append({"url": url, "status": status})
                _LOGGER.debug(
                    "go2rtc attempt %s returned status=%s (no usable answer)",
                    url,
                    status,
                )
        except (aiohttp.ClientError, TimeoutError) as err:  # noqa: PERF203
            tried.append({"url": url, "error": str(err)})
            _LOGGER.debug("go2rtc attempt %s failed: %s", url, err)

    # Neither the native path nor the go2rtc fallback produced an answer —
    # degrade gracefully with diagnostics. Log at WARNING (not debug) so a "no
    # signal from camera" report is debuggable straight from the HA log without
    # enabling debug. Show BOTH what the native path did and the go2rtc statuses.
    _LOGGER.warning(
        "Live camera: NO ANSWER for stream %s. "
        "Native HA camera WebRTC path: %s. "
        "go2rtc loopback fallback tried: %s. "
        "If the camera supports WebRTC, the native path should answer; check that "
        "the camera entity exists and exposes CameraEntityFeature.STREAM. The "
        "go2rtc loopback ports (11984/1984) are only a last resort and are not "
        "reachable in many HA OS setups.",
        stream_id,
        native_error or "not attempted (no hass / not a camera.* entity id)",
        tried,
    )
    return CameraAnswer(
        answer_sdp=None,
        fallback={
            "reason": "no_answer",
            "stream_id": stream_id,
            "native_error": native_error,
            "tried": tried,
            "rtsp": rtsp_fallback_url,
        },
    )


def _build_streams_attempts(target: Go2RtcTarget) -> list[dict[str, Any]]:
    """Ordered go2rtc *streams-list* attempts (the discovery counterpart of the
    handshake attempts). Each is { url, headers }; the first 200 with a dict body
    wins. Same two shapes as the handshake:

      1) Standalone go2rtc API:   GET http://127.0.0.1:1984/api/streams
      2) HA-proxied go2rtc:       GET http://127.0.0.1:8123/api/go2rtc/api/streams
                                  (Bearer = HA long-lived token)

    go2rtc returns a JSON object keyed by stream id, e.g.
      { "front_door": { "producers": [...], ... }, "kitchen": { ... } }
    """
    attempts: list[dict[str, Any]] = []

    go2rtc_base = target.go2rtc_base_url.rstrip("/")
    ha_base = target.ha_base_url.rstrip("/")

    attempts.append(
        {
            "url": f"{go2rtc_base}/api/streams",
            "headers": {"Accept": "application/json"},
        }
    )

    ha_headers = {"Accept": "application/json"}
    if target.ha_token:
        ha_headers["Authorization"] = f"Bearer {target.ha_token}"
    attempts.append(
        {
            "url": f"{ha_base}/api/go2rtc/api/streams",
            "headers": ha_headers,
        }
    )

    return attempts


def _parse_go2rtc_streams(payload: Any) -> list[dict[str, Any]]:
    """Turn go2rtc's ``/api/streams`` body into ``[{id, name, online}]``.

    go2rtc keys the dict by stream id; the value is the stream's producer/info
    block (shape varies across builds, so we only use the KEY). Presence in the
    list == online (best-effort). The friendly name defaults to the id; callers
    may upgrade it from an HA camera entity friendly_name when resolvable.
    """
    if not isinstance(payload, dict):
        return []
    cameras: list[dict[str, Any]] = []
    for stream_id in payload:
        sid = str(stream_id).strip()
        if not sid:
            continue
        cameras.append({"id": sid, "name": sid, "online": True})
    return cameras


async def _list_from_ha_entities(
    session: aiohttp.ClientSession, target: Go2RtcTarget
) -> list[dict[str, Any]]:
    """Secondary source: enumerate HA ``camera.*`` entities via the HA REST API.

    Used only when the go2rtc streams list is empty. HA registers its camera
    entities with go2rtc under their entity id, so the entity_id doubles as a
    usable go2rtc ``src``. Requires the HA long-lived token; without one we have
    no way to read HA states, so we return []. Never raises.

    GET {ha_base}/api/states → a list of state objects; we keep entity_ids that
    start with "camera." and lift their friendly_name for a nicer label.
    """
    if not target.ha_token:
        return []
    ha_base = target.ha_base_url.rstrip("/")
    url = f"{ha_base}/api/states"
    headers = {
        "Authorization": f"Bearer {target.ha_token}",
        "Accept": "application/json",
    }
    timeout = aiohttp.ClientTimeout(total=_HANDSHAKE_TIMEOUT_SECONDS)
    try:
        async with session.get(url, headers=headers, timeout=timeout) as resp:
            if resp.status != 200:
                return []
            payload = await _safe_json(resp)
    except (aiohttp.ClientError, TimeoutError) as err:
        _LOGGER.debug("HA camera-entity enumeration failed: %s", err)
        return []

    if not isinstance(payload, list):
        return []

    cameras: list[dict[str, Any]] = []
    for state in payload:
        if not isinstance(state, dict):
            continue
        entity_id = state.get("entity_id")
        if not isinstance(entity_id, str) or not entity_id.startswith("camera."):
            continue
        attrs = state.get("attributes")
        friendly = (
            attrs.get("friendly_name")
            if isinstance(attrs, dict) and isinstance(attrs.get("friendly_name"), str)
            else None
        )
        state_val = state.get("state")
        # "unavailable"/"unknown" → offline; anything else (idle/recording/
        # streaming) means the entity is reachable.
        online = state_val not in ("unavailable", "unknown")
        cameras.append(
            {
                "id": entity_id,
                "name": (friendly or entity_id).strip() or entity_id,
                "online": bool(online),
            }
        )
    return cameras


def list_from_hass_states(hass: Any) -> list[dict[str, Any]]:
    """Discover ``camera.*`` entities directly from the in-process HA object.

    This is the RELIABLE discovery path for Hikvision/NVR channels and any other
    HA camera that is NOT auto-registered as a go2rtc stream: it reads HA's state
    machine synchronously, in-process — no HTTP, no long-lived token. HA registers
    each of its camera entities with the bundled go2rtc under the entity id, so
    the ``camera.*`` entity_id doubles as the go2rtc ``src`` and flows straight
    back as ``cameraId`` into ``answer_offer`` (which already resolves
    ``cameraId`` → ``src``).

    Maps each camera State to ``{ id, name, online }`` where:
      * id     = the ``camera.*`` entity_id,
      * name   = the ``friendly_name`` attribute, falling back to the entity_id,
      * online = the state is not ``unavailable`` / ``unknown``.

    Uses the stable public API ``hass.states.async_all('camera')`` (the domain
    filter is supported across current HA core versions; we still guard the
    per-state shape defensively). Best-effort and NEVER raises — returns ``[]``
    when ``hass`` is missing or the state machine can't be read, so the
    coordinator's report step can't break the poll loop.
    """
    if hass is None:
        return []
    try:
        states = hass.states.async_all("camera")
    except Exception as err:  # noqa: BLE001 - discovery must never raise
        _LOGGER.debug("hass.states camera enumeration failed (ignored): %s", err)
        return []

    cameras: list[dict[str, Any]] = []
    for state in states or []:
        entity_id = getattr(state, "entity_id", None)
        if not isinstance(entity_id, str) or not entity_id.startswith("camera."):
            continue
        attrs = getattr(state, "attributes", None)
        friendly = (
            attrs.get("friendly_name")
            if hasattr(attrs, "get")
            and isinstance(attrs.get("friendly_name"), str)
            else None
        )
        state_val = getattr(state, "state", None)
        # "unavailable"/"unknown" → offline; anything else (idle/recording/
        # streaming) means the entity is reachable.
        online = state_val not in ("unavailable", "unknown")
        cameras.append(
            {
                "id": entity_id,
                "name": (friendly or entity_id).strip() or entity_id,
                "online": bool(online),
            }
        )
    return cameras


async def list_cameras(
    session: aiohttp.ClientSession,
    target: Go2RtcTarget,
    hass: Any = None,
) -> list[dict[str, Any]]:
    """Discover the branch's cameras for the cloud/dashboard picker.

    Returns ``[{ "id": str, "name": str, "online": bool }]`` where ``id`` is the
    go2rtc stream ``src`` (opaque to the cloud; fed straight back as ``cameraId``
    into the offer flow). Strategy, merged + deduped by id in this order:

      1. Enumerate go2rtc streams (``/api/streams``) — for branches that DO use a
         go2rtc.yaml, tried standalone then HA-proxied (see
         ``_build_streams_attempts``).
      2. Enumerate HA ``camera.*`` entities directly from the in-process ``hass``
         object (``list_from_hass_states``) — the reliable path that surfaces
         Hikvision/NVR channels which never auto-register with go2rtc. No token,
         no HTTP.
      3. Last resort ONLY when ``hass`` is unavailable: HA ``camera.*`` entities
         via the HA REST API (needs a long-lived token; usually a no-op).

    Best-effort and NEVER raises — returns ``[]`` on total failure so the
    coordinator's report step can't break the poll loop.
    """
    timeout = aiohttp.ClientTimeout(total=_HANDSHAKE_TIMEOUT_SECONDS)
    cameras: list[dict[str, Any]] = []

    # 1) go2rtc streams (authoritative for go2rtc.yaml-configured branches).
    for attempt in _build_streams_attempts(target):
        url = attempt["url"]
        headers = attempt["headers"]
        try:
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status != 200:
                    continue
                payload = await _safe_json(resp)
        except (aiohttp.ClientError, TimeoutError) as err:  # noqa: PERF203
            _LOGGER.debug("go2rtc streams attempt %s failed: %s", url, err)
            continue
        parsed = _parse_go2rtc_streams(payload)
        if parsed:
            cameras = parsed
            break

    # 2) In-process HA camera entities — the reliable path (Hikvision/NVR). Merged
    #    with the go2rtc list and deduped below, so it works whether or not
    #    go2rtc.yaml is in use.
    if hass is not None:
        try:
            cameras = cameras + list_from_hass_states(hass)
        except Exception as err:  # noqa: BLE001 - discovery must never raise
            _LOGGER.debug("hass camera enumeration errored (ignored): %s", err)
    elif not cameras:
        # 3) Last resort: HA REST states, only when we have no hass object AND
        #    go2rtc gave us nothing. Needs a long-lived token; usually a no-op.
        try:
            cameras = await _list_from_ha_entities(session, target)
        except Exception as err:  # noqa: BLE001 - discovery must never raise
            _LOGGER.debug("HA camera-entity fallback errored (ignored): %s", err)
            cameras = []

    # Dedupe by id, preserving first-seen order (go2rtc wins over HA entity).
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for cam in cameras:
        cid = cam.get("id")
        if not isinstance(cid, str) or cid in seen:
            continue
        seen.add(cid)
        deduped.append(cam)
    return deduped


def _extract_answer_sdp(payload: Any) -> str | None:
    """Pull the SDP answer string out of go2rtc's response.

    go2rtc replies (across builds) as one of:
      { "type": "answer", "sdp": "<sdp>" }   — the common shape
      { "sdp": "<sdp>" }                       — type omitted
      "<raw sdp string>"                        — some builds return bare SDP
    """
    if isinstance(payload, dict):
        sdp = payload.get("sdp")
        if isinstance(sdp, str) and sdp.strip():
            return sdp
        return None
    if isinstance(payload, str) and payload.strip().startswith("v="):
        return payload
    return None


async def _safe_json(resp: aiohttp.ClientResponse) -> Any:
    """Parse a response body as JSON, else return the raw text (for bare SDP)."""
    try:
        return await resp.json(content_type=None)
    except (aiohttp.ContentTypeError, ValueError):
        try:
            return await resp.text()
        except Exception:  # pragma: no cover - defensive
            return None
