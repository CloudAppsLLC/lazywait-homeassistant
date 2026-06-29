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
"""

from __future__ import annotations

from typing import Any

import aiohttp

_HA_PREFIX = "/integrations/home-assistant"


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

    # ── internals ───────────────────────────────────────────────────────────

    async def _authed_request(
        self,
        method: str,
        url: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        req_headers = headers if headers is not None else self._auth_headers()
        try:
            async with self._session.request(
                method, url, json=json, headers=req_headers
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
