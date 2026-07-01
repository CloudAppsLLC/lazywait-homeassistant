"""Tests for the HA-free surface of camerasnapshot.py.

The homeassistant import is behind TYPE_CHECKING and the real ``async_get_image``
is imported lazily inside ``capture_snapshot`` (absent in this bare env → the
function degrades to ``None``). So we can exercise the pure encoder and the
guard/degrade branches without homeassistant installed.
"""

import base64

import pytest

from custom_components.lazywait.camerasnapshot import (
    capture_snapshot,
    encode_snapshot,
)


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
