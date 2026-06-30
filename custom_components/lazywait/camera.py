"""Live camera WebRTC answerer — bridges a cloud SDP offer to go2rtc.

The dashboard is one WebRTC peer; HA/go2rtc is the other. The cloud relays
SIGNALING ONLY (SDP offer/answer; ICE candidates bundled non-trickle inside the
SDP). Media flows peer-to-peer over Twilio TURN — it never touches the cloud.
HA is outbound-only behind NAT, so it cannot accept an inbound WebRTC peer
connection directly; instead it POLLS the cloud for a pending offer, asks the
LOCAL go2rtc (bundled in HA) to produce an answer for that offer against a named
stream, and POSTs the answer back to the cloud.

go2rtc handshake — THE ONE CALL THAT NEEDS LIVE VERIFICATION
============================================================
go2rtc exposes a one-shot WebRTC exchange: you POST an SDP *offer* and it
returns an SDP *answer* for a named source stream. The exact route depends on
how go2rtc is reached:

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

import logging
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


async def answer_offer(
    session: aiohttp.ClientSession,
    target: Go2RtcTarget,
    *,
    camera_id: str,
    offer_sdp: str,
    default_stream_id: str = "",
    rtsp_fallback_url: str | None = None,
) -> CameraAnswer:
    """Ask the local go2rtc to answer a cloud WebRTC offer.

    Tries each go2rtc handshake shape (see ``_build_attempts``) in order. On the
    first 200 carrying an SDP answer, returns ``CameraAnswer(answer_sdp=...)``.
    If no attempt produces an answer, returns a ``CameraAnswer`` with
    ``answer_sdp=None`` and a ``fallback`` dict describing what was tried + the
    RTSP/stream info, so the caller can log a precise "go2rtc handshake
    unconfirmed" and (optionally) surface the RTSP URL for a non-WebRTC viewer.

    Never raises — a failed handshake must not break the coordinator poll loop.

    Args:
        session: shared HA aiohttp session.
        target: go2rtc / HA endpoints + optional HA token.
        camera_id: stream id from the cloud offer ("" → default_stream_id).
        offer_sdp: the dashboard's SDP offer (ICE bundled non-trickle).
        default_stream_id: go2rtc stream to use when camera_id is empty.
        rtsp_fallback_url: optional RTSP URL to include in the fallback info.
    """
    stream_id = _resolve_stream_id(camera_id, default_stream_id)
    if not stream_id:
        _LOGGER.warning(
            "go2rtc answer skipped: no stream id (camera_id empty and no default)"
        )
        return CameraAnswer(
            answer_sdp=None,
            fallback={
                "reason": "no_stream_id",
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

    # No attempt produced an answer — degrade gracefully with diagnostics. Log
    # the exact endpoints + statuses at WARNING (not debug) so a "no signal"
    # report is debuggable straight from the HA log without enabling debug.
    _LOGGER.warning(
        "go2rtc handshake unconfirmed for stream %s; tried: %s. "
        "Bundled HA go2rtc listens on 127.0.0.1:11984 — confirm it's running and "
        "that the camera entity is registered there as a stream.",
        stream_id,
        tried,
    )
    return CameraAnswer(
        answer_sdp=None,
        fallback={
            "reason": "go2rtc_handshake_unconfirmed",
            "stream_id": stream_id,
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
