"""Native HA camera WebRTC answer-path tests for ``answer_offer``.

``answer_offer`` now answers a cloud SDP offer via HA's NATIVE camera WebRTC API
first (``Camera.async_handle_async_webrtc_offer``), routing through HA's
correctly configured go2rtc provider with NO loopback port guessing. The legacy
go2rtc-POST handshake is a last-resort fallback only.

``camera.py`` imports the HA camera symbols LAZILY inside ``_answer_via_ha_native``
so the module stays importable in this bare (HA-free) environment. To exercise
the native path here we inject fake ``homeassistant.components.camera`` /
``...camera.webrtc`` modules into ``sys.modules`` for the duration of a test, so
the lazy imports resolve to our fakes — no real Home Assistant install needed.
"""

import sys
import types

import aiohttp
import pytest

from custom_components.lazywait.camera import Go2RtcTarget, answer_offer


# ── Fakes mirroring the HA camera WebRTC surface ────────────────────────────


class _FakeCameraEntityFeature:
    STREAM = 2  # mirrors CameraEntityFeature.STREAM bit


class _WebRTCAnswer:
    def __init__(self, sdp: str) -> None:
        self.sdp = sdp


class _WebRTCError:
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message


class _FakeCamera:
    """Camera entity that answers via the unified async provider API."""

    def __init__(self, *, supported_features: int, answer_sdp=None, error=None) -> None:
        self.supported_features = supported_features
        self._answer_sdp = answer_sdp
        self._error = error
        self.closed_session: str | None = None
        self.seen_offer: str | None = None

    async def async_handle_async_webrtc_offer(self, offer_sdp, session_id, send_message):
        # Fire-and-forget: deliver the result through the callback, return None.
        self.seen_offer = offer_sdp
        if self._error is not None:
            send_message(self._error)
        elif self._answer_sdp is not None:
            send_message(_WebRTCAnswer(self._answer_sdp))
        return None

    def close_webrtc_session(self, session_id) -> None:
        self.closed_session = session_id


def _install_fake_camera_modules(camera_obj_or_exc):
    """Inject fake homeassistant.components.camera[.webrtc] into sys.modules.

    ``camera_obj_or_exc`` is the entity ``get_camera_from_entity_id`` returns
    (or an Exception instance it should raise). Returns the list of dotted
    module names installed so the caller can clean them up.
    """
    cam_mod = types.ModuleType("homeassistant.components.camera")
    webrtc_mod = types.ModuleType("homeassistant.components.camera.webrtc")

    def _get_camera_from_entity_id(hass, entity_id):
        if isinstance(camera_obj_or_exc, Exception):
            raise camera_obj_or_exc
        return camera_obj_or_exc

    cam_mod.CameraEntityFeature = _FakeCameraEntityFeature  # type: ignore[attr-defined]
    cam_mod.get_camera_from_entity_id = _get_camera_from_entity_id  # type: ignore[attr-defined]
    webrtc_mod.WebRTCAnswer = _WebRTCAnswer  # type: ignore[attr-defined]
    webrtc_mod.WebRTCError = _WebRTCError  # type: ignore[attr-defined]

    # The parent package must exist for the dotted submodule import to resolve.
    ha_pkg = sys.modules.get("homeassistant") or types.ModuleType("homeassistant")
    comp_pkg = sys.modules.get("homeassistant.components") or types.ModuleType(
        "homeassistant.components"
    )

    installed = {
        "homeassistant": ha_pkg,
        "homeassistant.components": comp_pkg,
        "homeassistant.components.camera": cam_mod,
        "homeassistant.components.camera.webrtc": webrtc_mod,
    }
    return installed


@pytest.fixture
def fake_ha_camera(request):
    """Install fake HA camera modules for one test, then remove them."""
    target = request.param
    installed = _install_fake_camera_modules(target)
    saved = {name: sys.modules.get(name) for name in installed}
    sys.modules.update(installed)
    try:
        yield target
    finally:
        for name, prev in saved.items():
            if prev is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prev


class _BoomSession:
    """aiohttp-session stand-in that fails if the go2rtc fallback is reached."""

    def post(self, *args, **kwargs):  # pragma: no cover - asserts via raise
        raise AssertionError("go2rtc fallback POST must not run when native answers")


class _UnreachableGo2RtcSession:
    """aiohttp-session whose POST mirrors the real bug: go2rtc loopback ports
    refuse the connection ("Cannot connect to host"). ``answer_offer`` catches
    ``aiohttp.ClientError`` and degrades to a no-answer CameraAnswer."""

    def post(self, *args, **kwargs):
        raise aiohttp.ClientConnectionError("Cannot connect to host 127.0.0.1:11984")


# ── Tests ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "fake_ha_camera",
    [_FakeCamera(supported_features=_FakeCameraEntityFeature.STREAM, answer_sdp="v=0\nANSWER")],
    indirect=True,
)
async def test_native_path_answers_and_skips_go2rtc(fake_ha_camera) -> None:
    result = await answer_offer(
        _BoomSession(),
        Go2RtcTarget(),
        camera_id="camera.nvr_channel_1",
        offer_sdp="v=0\nOFFER",
        hass=object(),
    )
    assert result.ok is True
    assert result.answer_sdp == "v=0\nANSWER"
    # The offer reached the camera and the session was torn down.
    assert fake_ha_camera.seen_offer == "v=0\nOFFER"
    assert fake_ha_camera.closed_session is not None


@pytest.mark.parametrize(
    "fake_ha_camera",
    [_FakeCamera(supported_features=0, answer_sdp="v=0\nANSWER")],
    indirect=True,
)
async def test_native_skipped_without_stream_feature_falls_back(fake_ha_camera) -> None:
    # No STREAM feature → native skipped; the go2rtc fallback then fails to
    # connect (the real bug), so we degrade to a no-answer CameraAnswer that
    # records the native_error — never raising.
    result = await answer_offer(
        _UnreachableGo2RtcSession(),
        Go2RtcTarget(),
        camera_id="camera.no_stream",
        offer_sdp="v=0\nOFFER",
        hass=object(),
    )
    assert result.ok is False
    assert result.answer_sdp is None
    assert result.fallback is not None
    assert result.fallback.get("reason") == "no_answer"
    assert result.fallback.get("native_error") is not None
    # The fallback was actually attempted and recorded the connection failure.
    assert result.fallback.get("tried")


@pytest.mark.parametrize(
    "fake_ha_camera",
    [
        _FakeCamera(
            supported_features=_FakeCameraEntityFeature.STREAM,
            error=_WebRTCError("WEBRTC_PROVIDER", "no provider"),
        )
    ],
    indirect=True,
)
async def test_native_error_message_resolves_to_no_answer(fake_ha_camera) -> None:
    result = await answer_offer(
        _UnreachableGo2RtcSession(),
        Go2RtcTarget(),
        camera_id="camera.errors",
        offer_sdp="v=0\nOFFER",
        hass=object(),
    )
    assert result.ok is False
    assert result.answer_sdp is None
    # Session torn down even on a WebRTCError.
    assert fake_ha_camera.closed_session is not None


async def test_no_hass_skips_native_entirely() -> None:
    # Without hass, the native path is never attempted; the go2rtc fallback then
    # fails to connect and the call degrades gracefully (no raise).
    result = await answer_offer(
        _UnreachableGo2RtcSession(),
        Go2RtcTarget(),
        camera_id="camera.x",
        offer_sdp="v=0\nOFFER",
        hass=None,
    )
    assert result.ok is False
    assert result.answer_sdp is None
    # native_error stays None because the native path was never even attempted.
    assert result.fallback is not None
    assert result.fallback.get("native_error") is None
