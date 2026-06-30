"""Pytest bootstrap for the HA-free surface of the integration.

We deliberately only test `api.py` + `const.py`. The rest of the package
(`coordinator`, `config_flow`, `sensor`, `binary_sensor`, `__init__`) imports
homeassistant at module top level, and so does the package's own
`custom_components/lazywait/__init__.py`. That means a normal
`from custom_components.lazywait.api import ...` would execute the package
`__init__` (importing homeassistant) and fail in a bare environment.

To stay HA-free we load `api.py` and `const.py` directly from their file paths
via importlib and register them under the dotted names tests use, WITHOUT
executing the package `__init__`. We register `const` first because `api` does
not import it, but doing so keeps the module namespace consistent.
"""

import importlib.util
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))

_PKG_DIR = _REPO_ROOT / "custom_components" / "lazywait"


def _load(module_name: str, file_name: str) -> None:
    spec = importlib.util.spec_from_file_location(
        module_name, _PKG_DIR / file_name
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)


# Register the dotted names the tests import, sourced straight from the files
# so the HA-importing package __init__ never runs.
_load("custom_components.lazywait.const", "const.py")
_load("custom_components.lazywait.api", "api.py")
# camera.py imports only aiohttp + stdlib (no homeassistant), so it loads HA-free.
_load("custom_components.lazywait.camera", "camera.py")
