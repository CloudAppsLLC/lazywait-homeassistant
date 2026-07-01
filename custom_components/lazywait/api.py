"""HTTP client for the LazyWait cloud Home Assistant surface.

All calls are OUTBOUND from HA to the cloud — the cloud never connects in. The
only unauthenticated call is `redeem_pairing_code` (the pairing code itself is
the auth, since HA has no bearer token yet). Every other call carries the
long-lived enrollment bearer minted by /pair.

Endpoints (relative to the configured base URL, e.g.
https://apiv2.lazywait.com/v1):
  POST /integrations/home-assistant/pair      — redeem a pairing code → token
  GET  /integrations/home-assistant/config    — poll branch config
  POST /integrations/home-assistant/events    — push a batch of events
  GET  /integrations/home-assistant/ping       — liveness / token check
  POST /integrations/home-assistant/status     — self-reported health heartbeat
  GET  /integrations/home-assistant/camera/poll   — claim a pending WebRTC offer
  POST /integrations/home-assistant/camera/answer — return the SDP answer
  POST /integrations/home-assistant/camera/cameras — report the discovered list
  GET  /integrations/home-assistant/camera/snapshot/requests — cameras viewed now
  POST /integrations/home-assistant/camera/snapshot — upload a near-live snapshot
"""

from __future__ import annotations

from typing import Any

import aiohttp

_HA_PREFIX = "/integrations/home-assistant"

# Bounded total timeout for a near-live snapshot upload. Kept short: at a
# sub-second capture cadence a hung POST would otherwise stall that camera's
# frame far longer than one tick. A dropped frame just retries next tick.
_SNAPSHOT_UPLOAD_TIMEOUT_SECONDS = 3.0


class LazyWaitApiError(Exception):
    """Base error for cloud API failures."""


class LazyWaitAuthError(LazyWaitApiError):
    """The bearer token was rejected (401). Triggers HA's reauth flow."""


class LazyWaitPairingError(LazyWaitApiError):
    """A pairing code was rejected. `error_key` distinguishes the reason.

    error_key is one of: HA_CODE_EXPIRED | HA_CODE_USED | HA_CODE_INVALID |
    HA_PAIR_BODY_INVALID | HA_PAIR_FAILED.
    """

    def __init__(self, error_key: str, message: str | None = None) -> None:
        self.error_key = error_key
        super().__init__(message or error_key)


class LazyWaitApiClient:
    """Thin async wrapper over the LazyWait cloud HA endpoints."""

    def __init__(
        self,
        base_url: str,
        session: aiohttp.ClientSession,
        token: str | None = None,
    ) -> None:
        # Normalize: strip a trailing slash so f"{base}{path}" never doubles up.
        self._base_url = base_url.rstrip("/")
        self._session = session
        self._token = token

    @property
    def token(self) -> str | None:
        """The stored bearer, if paired."""
        return self._token

    def _url(self, path: str) -> str:
        return f"{self._base_url}{_HA_PREFIX}{path}"

    def _auth_headers(self) -> dict[str, str]:
        if not self._token:
            raise LazyWaitAuthError("no enrollment token stored")
        return {"Authorization": f"Bearer {self._token}"}

    # ── Pairing (unauthenticated — the code is the auth) ────────────────────

    async def redeem_pairing_code(
        self, pairing_code: str, ha_instance_name: str
    ) -> dict[str, Any]:
        """Exchange a pairing code for a long-lived bearer token.

        Returns the cloud's /pair response:
          { branchId, enrollmentToken, baseUrl, config }

        Raises LazyWaitPairingError with the cloud's errorKey on a 400 so the
        config flow can show a precise "expired / already used / invalid"
        message. The token is NOT stored on the client here — the caller saves
        it into the config entry and constructs an authenticated client.
        """
        url = self._url("/pair")
        body = {"pairingCode": pairing_code, "haInstanceName": ha_instance_name}
        try:
            async with self._session.post(url, json=body) as resp:
                payload = await self._safe_json(resp)
                if resp.status == 200:
                    return payload
                error_key = (
                    payload.get("errorKey")
                    if isinstance(payload, dict)
                    else None
                ) or "HA_PAIR_FAILED"
                raise LazyWaitPairingError(error_key)
        except aiohttp.ClientError as err:
            raise LazyWaitApiError(f"pair request failed: {err}") from err

    # ── Authenticated calls ─────────────────────────────────────────────────

    async def get_config(self) -> dict[str, Any]:
        """Fetch the branch config HA applies (thresholds, entities, version)."""
        url = self._url("/config")
        return await self._authed_request("GET", url)

    async def ping(self) -> dict[str, Any]:
        """Liveness probe; also proves the stored token still resolves a branch."""
        url = self._url("/ping")
        return await self._authed_request("GET", url)

    async def push_events(
        self, events: list[dict[str, Any]], idempotency_key: str | None = None
    ) -> dict[str, Any]:
        """Push a batch of events. Returns { accepted, rejected, results }.

        An optional Idempotency-Key lets the cloud de-dupe a re-flushed batch
        after a reconnect, so an at-least-once flush never double-counts.
        """
        url = self._url("/events")
        headers = self._auth_headers()
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        return await self._authed_request(
            "POST", url, json={"events": events}, headers=headers
        )

    async def report_status(
        self,
        ha_version: str,
        integration_version: str,
        online: bool,
        config_version: int,
        last_event_at: str | None,
    ) -> dict[str, Any]:
        """Self-reported health heartbeat the dashboard renders."""
        url = self._url("/status")
        body = {
            "haVersion": ha_version,
            "integrationVersion": integration_version,
            "online": online,
            "configVersion": config_version,
            "lastEventAt": last_event_at,
        }
        return await self._authed_request("POST", url, json=body)

    # ── Live camera WebRTC signaling ────────────────────────────────────────

    async def camera_poll(self) -> dict[str, Any]:
        """Poll the cloud for a pending WebRTC offer for this branch.

        GET /integrations/home-assistant/camera/poll (bearer-authed; the cloud
        resolves the branch from the token, never from us).

        Returns the cloud's response verbatim:
          { "pending": false }
          { "pending": true, "sessionId": str, "offer": <sdp>, "cameraId": str }

        The dashboard posts an SDP offer for a branch camera; the cloud holds it
        for ~60s and hands it to whichever HA loop polls first. `offer` is a full
        SDP with ICE candidates bundled non-trickle. `cameraId` identifies which
        go2rtc stream / HA camera entity to answer with (may be ""→ default).

        Raises LazyWaitAuthError on 401 (token rotated) so the coordinator can
        surface reauth; LazyWaitApiError on other failures.
        """
        url = self._url("/camera/poll")
        return await self._authed_request("GET", url)

    async def camera_answer(
        self, session_id: str, answer_sdp: str
    ) -> dict[str, Any]:
        """Post HA/go2rtc's SDP answer for a signaling session.

        POST /integrations/home-assistant/camera/answer (bearer-authed). The
        branch is resolved cloud-side from the token; we only send the session id
        and the SDP answer (ICE bundled non-trickle).

        Returns { "ok": true } on success. A 404
        { "errorKey": "HA_CAMERA_SESSION_NOT_FOUND" } means the session expired
        (the dashboard waited too long) — the caller logs + drops.
        """
        url = self._url("/camera/answer")
        body = {"sessionId": session_id, "answer": answer_sdp}
        return await self._authed_request("POST", url, json=body)

    async def snapshot_requests(self) -> dict[str, Any]:
        """Poll which cameras the dashboard is viewing NOW (near-live snapshot).

        GET /integrations/home-assistant/camera/snapshot/requests (bearer-authed;
        the cloud resolves the branch from the token). Returns the cloud's
        response verbatim:
          {
            "primary":   ["camera.main"],        — grid MAIN, capture every tick
            "secondary": ["camera.thumb1", ...], — thumbnails, round-robin
            "cameraIds": [...],                   — flat union (back-compat)
          }

        The dashboard registers a camera id while its live view is open (the grid
        marks the main camera primary, thumbnails secondary); the cloud aggregates
        the currently-viewed set for this branch and hands it back so HA captures
        only what someone is watching. Empty primary+secondary → capture nothing.

        Raises LazyWaitAuthError on 401 (token rotated); LazyWaitApiError else.
        """
        url = self._url("/camera/snapshot/requests")
        return await self._authed_request("GET", url)

    async def post_snapshot(
        self,
        camera_id: str,
        image_b64: str,
        content_type: str = "image/jpeg",
    ) -> dict[str, Any]:
        """Upload one freshly-captured JPEG snapshot for a viewed camera.

        POST /integrations/home-assistant/camera/snapshot (bearer-authed; branch
        resolved cloud-side from the token). `image_b64` is the base64 of the raw
        JPEG bytes with NO ``data:`` prefix. The cloud caches the latest frame
        per (branch, camera) and serves it to the dashboard poll, giving a
        near-live (~1 fps) view without WebRTC.

        Returns { "ok": true }. Raises LazyWaitAuthError on 401; LazyWaitApiError
        on other non-2xx (the caller swallows it — a dropped frame is fine).
        """
        url = self._url("/camera/snapshot")
        body = {
            "cameraId": camera_id,
            "image": image_b64,
            "contentType": content_type,
        }
        # Bounded upload: at a sub-second cadence a hung POST must not stall a
        # camera's frame indefinitely (unlike the capture side, which already has
        # its own timeout). A dropped frame is fine — the next tick retries.
        return await self._authed_request(
            "POST", url, json=body, timeout=_SNAPSHOT_UPLOAD_TIMEOUT_SECONDS
        )

    async def report_cameras(
        self, cameras: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Push HA's discovered camera list to the cloud for the picker.

        POST /integrations/home-assistant/camera/cameras (bearer-authed). The
        cloud resolves the branch from the token and caches the list in memory
        (ephemeral — HA re-reports each cycle), serving it to the dashboard so an
        admin can pick a camera before opening the live view.

        Each camera is { id, name, online? }; `id` is the go2rtc stream src that
        flows straight back as `cameraId` in the offer. Returns the cloud's
        response verbatim (e.g. { "ok": true, "count": n }).
        """
        url = self._url("/camera/cameras")
        return await self._authed_request("POST", url, json={"cameras": cameras})

    # ── Admin control (persistent WS + HTTP-poll fallback) ──────────────────

    def ws_url(self, admin_ws_path: str) -> str:
        """The admin control WebSocket URL: the ws/wss form of the base URL.

        http://…  → ws://…   ;  https://… → wss://…  . The path is appended to
        the base (which already includes the /v1 prefix), so a white-label host
        that terminates TLS works without extra config.
        """
        base = self._base_url
        if base.startswith("https://"):
            ws_base = "wss://" + base[len("https://"):]
        elif base.startswith("http://"):
            ws_base = "ws://" + base[len("http://"):]
        else:
            ws_base = base
        return f"{ws_base}{admin_ws_path}"

    def auth_headers(self) -> dict[str, str]:
        """Public accessor for the bearer headers (the WS client needs them on
        the upgrade handshake). Raises LazyWaitAuthError when unpaired."""
        return self._auth_headers()

    @property
    def session(self) -> aiohttp.ClientSession:
        """The shared aiohttp session (reused for the WS connect)."""
        return self._session

    async def poll_commands(self) -> dict[str, Any]:
        """HTTP fallback: claim the next queued admin command for this branch.

        GET /integrations/home-assistant/admin/commands/poll (bearer-authed).
        Used ONLY when the persistent control WebSocket is down. Returns the
        cloud's response verbatim:
          { "pending": false }
          { "pending": true, "command": { commandId, kind, domain, service, … } }

        Raises LazyWaitAuthError on 401 (token rotated); LazyWaitApiError else.
        """
        url = self._url("/admin/commands/poll")
        return await self._authed_request("GET", url)

    async def post_command_result(
        self, command_id: str, result: dict[str, Any]
    ) -> dict[str, Any]:
        """HTTP fallback: post a command's result.

        POST /integrations/home-assistant/admin/commands/<id>/result. `result`
        is { status: 'ok'|'error', errorKey?, result? }. Returns { ok: true } or
        a 404 when the command expired.
        """
        url = self._url(f"/admin/commands/{command_id}/result")
        return await self._authed_request("POST", url, json=result)

    # ── Face attendance (Hikvision) ─────────────────────────────────────────

    async def face_checkin(
        self, photo_base64: str, branch_id: str | None = None
    ) -> dict[str, Any]:
        """Forward a detected face to the cloud face-checkin endpoint.

        POSTs to {base_url}/hrm/attendance/face-checkin (NOTE: this lives
        OUTSIDE the /integrations/home-assistant prefix — it's the shared,
        device-facing HRMS route). The cloud runs AWS Rekognition, toggles the
        matched employee's clock IN/OUT, and writes an hrms_attendance row with
        a 5-min per-employee cooldown.

        This route is PUBLIC (device-facing, no JWT) — the body is the only
        input it needs. We still attach the bearer when we have one (harmless;
        the route ignores it), so a future move to an authenticated variant is a
        one-line change. `branch_id` defaults to the entry's paired branch so a
        camera check-in is attributed to the right store.

        Returns the cloud's response verbatim:
          { matched, employeeId?, employeeName?, clientId?, similarity?,
            action?: 'clock_in' | 'clock_out', recorded, reason?, attendance? }

        Raises LazyWaitApiError on a non-2xx (the caller logs + drops — a missed
        camera frame is not worth failing the whole automation over).
        """
        # /hrm/... hangs directly off the base URL, not the HA prefix.
        url = f"{self._base_url}/hrm/attendance/face-checkin"
        body: dict[str, Any] = {
            "photo_base64": photo_base64,
            "source": "hikvision",
        }
        if branch_id:
            body["branch_id"] = branch_id
        # Bearer is optional here; send it only if paired. No _auth_headers()
        # call (that raises when unpaired) — face-checkin must work token-less.
        headers = (
            {"Authorization": f"Bearer {self._token}"} if self._token else {}
        )
        return await self._authed_request("POST", url, json=body, headers=headers)

    # ── internals ───────────────────────────────────────────────────────────

    async def _authed_request(
        self,
        method: str,
        url: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        req_headers = headers if headers is not None else self._auth_headers()
        # A per-request total timeout when the caller wants a bounded call (the
        # near-live upload does); otherwise omit the kwarg entirely so the call
        # inherits the shared session default (passing timeout=None would DISABLE
        # the timeout instead of inheriting it).
        extra: dict[str, Any] = {}
        if timeout is not None:
            extra["timeout"] = aiohttp.ClientTimeout(total=timeout)
        try:
            async with self._session.request(
                method, url, json=json, headers=req_headers, **extra
            ) as resp:
                if resp.status == 401:
                    # Token rotated/revoked on the cloud — surface a distinct
                    # error so __init__ can kick off HA's reauth flow.
                    raise LazyWaitAuthError("HA_TOKEN_INVALID")
                payload = await self._safe_json(resp)
                if resp.status >= 400:
                    error_key = (
                        payload.get("errorKey")
                        if isinstance(payload, dict)
                        else None
                    ) or f"http_{resp.status}"
                    raise LazyWaitApiError(error_key)
                return payload if isinstance(payload, dict) else {}
        except aiohttp.ClientError as err:
            raise LazyWaitApiError(f"{method} {url} failed: {err}") from err

    @staticmethod
    async def _safe_json(resp: aiohttp.ClientResponse) -> Any:
        """Parse JSON without raising on an empty/non-JSON body."""
        try:
            return await resp.json(content_type=None)
        except (aiohttp.ContentTypeError, ValueError):
            return {}
