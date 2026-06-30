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

# Default go2rtc API base when reached standalone. The HA-bundled go2rtc is
# reached via the HA proxy form (see _ATTEMPTS). Overridable per entry.
DEFAULT_GO2RTC_BASE_URL = "http://127.0.0.1:1984"

# Default HA base for the proxied go2rtc form. 127.0.0.1:8123 is HA's local API.
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
    ha_base = target.ha_base_url.rstrip("/")
    # go2rtc expects the offer as an SDP body; src selects the published stream.
    query = f"?src={quote(stream_id, safe='')}" if stream_id else ""

    # 1) Standalone go2rtc API (default port 1984). No auth on the LAN.
    attempts.append(
        {
            "url": f"{go2rtc_base}/api/webrtc{query}",
            "headers": {"Content-Type": "application/json"},
            # body filled per-call with the offer SDP (see answer_offer).
        }
    )

    # 2) HA-bundled go2rtc proxied through HA. Auth with the HA long-lived token
    #    when supplied. THIS PATH/SHAPE is the one to verify against the live HA
    #    build of go2rtc.
    ha_headers = {"Content-Type": "application/json"}
    if target.ha_token:
        ha_headers["Authorization"] = f"Bearer {target.ha_token}"
    attempts.append(
        {
            "url": f"{ha_base}/api/go2rtc/webrtc{query}",
            "headers": ha_headers,
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

    # No attempt produced an answer — degrade gracefully with diagnostics.
    _LOGGER.warning(
        "go2rtc handshake unconfirmed for stream %s; tried %s endpoint(s). "
        "Verify the go2rtc WebRTC route for this HA build (see camera.py docstring).",
        stream_id,
        len(tried),
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
