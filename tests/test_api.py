"""Tests for LazyWaitApiClient pure + async logic.

api.py imports only aiohttp (no homeassistant), so it imports cleanly here. The
aiohttp ClientSession is mocked: both `session.post(...)` and
`session.request(...)` return an async context manager whose `__aenter__`
yields a fake response with a `status` and an async `json()` — exactly the shape
the client uses (`async with self._session.<x>(...) as resp:`).
"""

from typing import Any

import aiohttp
import pytest
from unittest.mock import MagicMock

from custom_components.lazywait.api import (
    LazyWaitApiClient,
    LazyWaitApiError,
    LazyWaitAuthError,
    LazyWaitPairingError,
)

BASE = "https://apiv2.lazywait.com/v1"
PREFIX = "/integrations/home-assistant"


# ── Helpers ─────────────────────────────────────────────────────────────────


class _FakeResponse:
    """Minimal stand-in for aiohttp.ClientResponse used inside `async with`."""

    def __init__(self, status: int, payload: Any) -> None:
        self.status = status
        self._payload = payload

    async def json(self, content_type: Any = None) -> Any:
        return self._payload


class _FakeRequestCtx:
    """Async context manager returned by the mocked session.post/.request."""

    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    async def __aenter__(self) -> _FakeResponse:
        return self._response

    async def __aexit__(self, *exc: Any) -> bool:
        return False


def _session_returning(response: _FakeResponse) -> MagicMock:
    """A mock session whose post() and request() both yield `response`.

    `post`/`request` are plain (non-async) callables that return an async
    context manager — matching aiohttp, where `session.post(...)` returns a
    context manager you `async with`, not a coroutine.
    """
    session = MagicMock(spec=aiohttp.ClientSession)
    ctx = _FakeRequestCtx(response)
    session.post = MagicMock(return_value=ctx)
    session.request = MagicMock(return_value=ctx)
    return session


# ── _url ────────────────────────────────────────────────────────────────────


def test_url_builds_prefix_and_path() -> None:
    client = LazyWaitApiClient(BASE, MagicMock(spec=aiohttp.ClientSession))
    assert client._url("/pair") == f"{BASE}{PREFIX}/pair"
    assert client._url("/config") == f"{BASE}{PREFIX}/config"


def test_url_strips_trailing_slash_from_base() -> None:
    client = LazyWaitApiClient(
        BASE + "/", MagicMock(spec=aiohttp.ClientSession)
    )
    # Trailing slash stripped in __init__, so the URL never doubles up.
    assert client._url("/ping") == f"{BASE}{PREFIX}/ping"


def test_url_strips_multiple_trailing_slashes() -> None:
    client = LazyWaitApiClient(
        BASE + "///", MagicMock(spec=aiohttp.ClientSession)
    )
    assert client._url("/events") == f"{BASE}{PREFIX}/events"


# ── _auth_headers ───────────────────────────────────────────────────────────


def test_auth_headers_with_token() -> None:
    client = LazyWaitApiClient(
        BASE, MagicMock(spec=aiohttp.ClientSession), token="tok-123"
    )
    assert client._auth_headers() == {"Authorization": "Bearer tok-123"}


def test_auth_headers_without_token_raises() -> None:
    client = LazyWaitApiClient(BASE, MagicMock(spec=aiohttp.ClientSession))
    with pytest.raises(LazyWaitAuthError):
        client._auth_headers()


def test_token_property() -> None:
    client = LazyWaitApiClient(
        BASE, MagicMock(spec=aiohttp.ClientSession), token="abc"
    )
    assert client.token == "abc"
    assert LazyWaitApiClient(
        BASE, MagicMock(spec=aiohttp.ClientSession)
    ).token is None


# ── Exception hierarchy ─────────────────────────────────────────────────────


def test_auth_error_subclasses_api_error() -> None:
    assert issubclass(LazyWaitAuthError, LazyWaitApiError)


def test_pairing_error_subclasses_api_error() -> None:
    assert issubclass(LazyWaitPairingError, LazyWaitApiError)


def test_pairing_error_carries_error_key() -> None:
    err = LazyWaitPairingError("HA_CODE_EXPIRED")
    assert err.error_key == "HA_CODE_EXPIRED"
    # message defaults to the error_key when none given
    assert str(err) == "HA_CODE_EXPIRED"


def test_pairing_error_custom_message() -> None:
    err = LazyWaitPairingError("HA_CODE_USED", "already used")
    assert err.error_key == "HA_CODE_USED"
    assert str(err) == "already used"


# ── redeem_pairing_code (async) ─────────────────────────────────────────────


async def test_redeem_pairing_code_success() -> None:
    payload = {
        "branchId": "b1",
        "enrollmentToken": "tok",
        "baseUrl": BASE,
        "config": {},
    }
    session = _session_returning(_FakeResponse(200, payload))
    client = LazyWaitApiClient(BASE, session)

    result = await client.redeem_pairing_code("CODE-1", "Living Room HA")

    assert result == payload
    # Posted to the /pair URL with the expected body.
    session.post.assert_called_once()
    args, kwargs = session.post.call_args
    assert args[0] == f"{BASE}{PREFIX}/pair"
    assert kwargs["json"] == {
        "pairingCode": "CODE-1",
        "haInstanceName": "Living Room HA",
    }


async def test_redeem_pairing_code_expired_raises_pairing_error() -> None:
    session = _session_returning(
        _FakeResponse(400, {"errorKey": "HA_CODE_EXPIRED"})
    )
    client = LazyWaitApiClient(BASE, session)

    with pytest.raises(LazyWaitPairingError) as exc_info:
        await client.redeem_pairing_code("CODE-1", "HA")

    assert exc_info.value.error_key == "HA_CODE_EXPIRED"


async def test_redeem_pairing_code_400_no_error_key_falls_back() -> None:
    session = _session_returning(_FakeResponse(400, {}))
    client = LazyWaitApiClient(BASE, session)

    with pytest.raises(LazyWaitPairingError) as exc_info:
        await client.redeem_pairing_code("CODE-1", "HA")

    assert exc_info.value.error_key == "HA_PAIR_FAILED"


async def test_redeem_pairing_code_client_error_wrapped() -> None:
    session = MagicMock(spec=aiohttp.ClientSession)
    session.post = MagicMock(side_effect=aiohttp.ClientError("boom"))
    client = LazyWaitApiClient(BASE, session)

    with pytest.raises(LazyWaitApiError):
        await client.redeem_pairing_code("CODE-1", "HA")


# ── _authed_request (async) ─────────────────────────────────────────────────


async def test_authed_request_success_returns_dict() -> None:
    session = _session_returning(_FakeResponse(200, {"ok": True}))
    client = LazyWaitApiClient(BASE, session, token="tok")

    result = await client.get_config()

    assert result == {"ok": True}
    args, kwargs = session.request.call_args
    assert args[0] == "GET"
    assert args[1] == f"{BASE}{PREFIX}/config"
    assert kwargs["headers"] == {"Authorization": "Bearer tok"}


async def test_authed_request_401_raises_auth_error() -> None:
    session = _session_returning(_FakeResponse(401, {}))
    client = LazyWaitApiClient(BASE, session, token="tok")

    with pytest.raises(LazyWaitAuthError):
        await client.ping()


async def test_authed_request_4xx_pulls_error_key() -> None:
    session = _session_returning(
        _FakeResponse(403, {"errorKey": "HA_FORBIDDEN"})
    )
    client = LazyWaitApiClient(BASE, session, token="tok")

    with pytest.raises(LazyWaitApiError) as exc_info:
        await client.get_config()

    assert str(exc_info.value) == "HA_FORBIDDEN"
    # 403 is not auth-specific, so it stays the base type, not LazyWaitAuthError.
    assert not isinstance(exc_info.value, LazyWaitAuthError)


async def test_authed_request_4xx_http_fallback() -> None:
    session = _session_returning(_FakeResponse(500, {}))
    client = LazyWaitApiClient(BASE, session, token="tok")

    with pytest.raises(LazyWaitApiError) as exc_info:
        await client.get_config()

    assert str(exc_info.value) == "http_500"


async def test_authed_request_success_non_dict_payload_returns_empty() -> None:
    session = _session_returning(_FakeResponse(200, ["not", "a", "dict"]))
    client = LazyWaitApiClient(BASE, session, token="tok")

    result = await client.ping()

    assert result == {}


async def test_push_events_sets_idempotency_key_header() -> None:
    session = _session_returning(_FakeResponse(200, {"accepted": 1}))
    client = LazyWaitApiClient(BASE, session, token="tok")

    result = await client.push_events([{"type": "presence"}], idempotency_key="k1")

    assert result == {"accepted": 1}
    args, kwargs = session.request.call_args
    assert args[0] == "POST"
    assert args[1] == f"{BASE}{PREFIX}/events"
    assert kwargs["headers"]["Idempotency-Key"] == "k1"
    assert kwargs["headers"]["Authorization"] == "Bearer tok"
    assert kwargs["json"] == {"events": [{"type": "presence"}]}


# ── Near-live snapshot relay ─────────────────────────────────────────────────


async def test_snapshot_requests_gets_camera_ids() -> None:
    session = _session_returning(
        _FakeResponse(200, {"cameraIds": ["camera.front", "camera.back"]})
    )
    client = LazyWaitApiClient(BASE, session, token="tok")

    result = await client.snapshot_requests()

    assert result == {"cameraIds": ["camera.front", "camera.back"]}
    args, kwargs = session.request.call_args
    assert args[0] == "GET"
    assert args[1] == f"{BASE}{PREFIX}/camera/snapshot/requests"
    assert kwargs["headers"] == {"Authorization": "Bearer tok"}


async def test_post_snapshot_body_and_url() -> None:
    session = _session_returning(_FakeResponse(200, {"ok": True}))
    client = LazyWaitApiClient(BASE, session, token="tok")

    result = await client.post_snapshot("camera.front", "Zm9v", "image/jpeg")

    assert result == {"ok": True}
    args, kwargs = session.request.call_args
    assert args[0] == "POST"
    assert args[1] == f"{BASE}{PREFIX}/camera/snapshot"
    assert kwargs["json"] == {
        "cameraId": "camera.front",
        "image": "Zm9v",
        "contentType": "image/jpeg",
    }
    assert kwargs["headers"] == {"Authorization": "Bearer tok"}


async def test_post_snapshot_defaults_content_type_jpeg() -> None:
    session = _session_returning(_FakeResponse(200, {"ok": True}))
    client = LazyWaitApiClient(BASE, session, token="tok")

    await client.post_snapshot("camera.front", "Zm9v")

    _, kwargs = session.request.call_args
    assert kwargs["json"]["contentType"] == "image/jpeg"
