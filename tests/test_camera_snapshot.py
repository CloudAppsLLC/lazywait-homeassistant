"""Tests for the HA-free surface of camerasnapshot.py.

The homeassistant import is behind TYPE_CHECKING and the real ``async_get_image``
is imported lazily inside ``capture_snapshot`` (absent in this bare env → the
function degrades to ``None``). So we can exercise the pure encoder and the
guard/degrade branches without homeassistant installed.
"""

import base64
import io

import pytest

from custom_components.lazywait.camerasnapshot import (
    _SNAPSHOT_MAX_DIMENSION,
    _downscale_jpeg,
    capture_snapshot,
    encode_snapshot,
)

# Pillow ships with Home Assistant but not necessarily this bare test env; skip
# the pixel-level downscale assertions when it's absent (the helper itself is
# written to degrade to None in that case, which we assert separately).
try:
    from PIL import Image as _PILImage  # noqa: F401

    _HAS_PIL = True
except Exception:  # noqa: BLE001
    _HAS_PIL = False


def test_encode_snapshot_is_prefixless_base64() -> None:
    raw = b"\xff\xd8\xff\xe0jpegbytes"
    encoded = encode_snapshot(raw)
    # No data: prefix; round-trips back to the original bytes.
    assert not encoded.startswith("data:")
    assert base64.b64decode(encoded) == raw
    assert encoded == base64.b64encode(raw).decode("ascii")


async def test_capture_snapshot_rejects_non_camera_entity() -> None:
    # Guard rejects anything that isn't a camera.* entity before touching HA.
    assert await capture_snapshot(object(), "light.kitchen") is None
    assert await capture_snapshot(object(), "") is None


async def test_capture_snapshot_returns_none_without_ha() -> None:
    # homeassistant isn't installed here, so the lazy async_get_image import
    # fails and capture_snapshot degrades to None instead of raising.
    result = await capture_snapshot(object(), "camera.front")
    assert result is None


def _make_jpeg(width: int, height: int) -> bytes:
    img = _PILImage.new("RGB", (width, height), (120, 90, 200))
    out = io.BytesIO()
    img.save(out, format="JPEG", quality=95)
    return out.getvalue()


@pytest.mark.skipif(not _HAS_PIL, reason="Pillow not installed in this env")
def test_downscale_jpeg_shrinks_oversized_frame() -> None:
    # A frame larger than the max dimension is resized down (longest side capped)
    # and re-encoded smaller.
    big = _make_jpeg(1920, 1080)
    shrunk = _downscale_jpeg(big)
    assert shrunk is not None
    with _PILImage.open(io.BytesIO(shrunk)) as img:
        assert max(img.width, img.height) == _SNAPSHOT_MAX_DIMENSION
    # The re-encode at a lower quality must actually reduce bytes.
    assert len(shrunk) < len(big)


@pytest.mark.skipif(not _HAS_PIL, reason="Pillow not installed in this env")
def test_downscale_jpeg_never_upscales_small_frame() -> None:
    # A frame already under the cap keeps its dimensions (only shrink, never grow).
    small = _make_jpeg(640, 480)
    shrunk = _downscale_jpeg(small)
    assert shrunk is not None
    with _PILImage.open(io.BytesIO(shrunk)) as img:
        assert (img.width, img.height) == (640, 480)


def test_downscale_jpeg_degrades_on_undecodable_input() -> None:
    # Garbage bytes must not raise — the helper returns None so the caller falls
    # back to the native frame. (Holds whether or not Pillow is installed.)
    assert _downscale_jpeg(b"not a jpeg at all") is None
