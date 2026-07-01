"""The LazyWait integration.

Sets up one config entry (one paired branch) by constructing an authenticated
cloud client from the stored token, doing an initial /ping to confirm the token
still works, and starting the coordinator that polls config / pushes events /
heartbeats. A rejected token surfaces as a reauth flow.
"""

from __future__ import annotations

import asyncio
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import LazyWaitApiClient, LazyWaitApiError, LazyWaitAuthError
from .const import CONF_BASE_URL, CONF_BRANCH_ID, CONF_TOKEN, DOMAIN
from .coordinator import LazyWaitCoordinator
from .ws_client import LazyWaitAdminSocket

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.BINARY_SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up LazyWait from a config entry."""
    base_url = entry.data[CONF_BASE_URL]
    token = entry.data[CONF_TOKEN]
    branch_id = entry.data[CONF_BRANCH_ID]

    session = async_get_clientsession(hass)
    client = LazyWaitApiClient(base_url, session, token=token)

    # Confirm the stored token still resolves a branch before we go further.
    try:
        await client.ping()
    except LazyWaitAuthError as err:
        # Rotated/revoked from the dashboard → ask for a fresh pairing code.
        raise ConfigEntryAuthFailed(str(err)) from err
    except LazyWaitApiError as err:
        # Transient cloud/network issue → HA retries setup later.
        raise ConfigEntryNotReady(f"LazyWait ping failed: {err}") from err

    # INFO (not debug) — ping passed, so the token is good. Its presence in the
    # HA log proves setup got past auth; its absence means the ping/token failed.
    _LOGGER.info("LazyWait setup: token OK for branch %s, starting coordinator", branch_id)

    coordinator = LazyWaitCoordinator(hass, client, branch_id)
    await coordinator.async_config_entry_first_refresh()

    # Start the persistent admin-control WebSocket (near-instant device/
    # automation control from the cloud dashboard). It's a long-lived task that
    # reconnects on its own; the coordinator's 30s poll is the degraded fallback
    # when the socket is down. Stashed ON the coordinator (not the data dict) so
    # the existing platform reads of hass.data[DOMAIN][entry_id] — which expect
    # the coordinator directly — keep working. Cancelled cleanly on unload.
    admin_socket = LazyWaitAdminSocket(hass, entry, client, branch_id)
    admin_task = hass.loop.create_task(admin_socket.run())
    coordinator.attach_admin_socket(admin_socket, admin_task)
    # INFO — confirms the admin-WS task was actually created. If this appears but
    # "Admin WS connecting to ..." never does, the task died before its first
    # connect (import error at module load, ws_url build, etc.).
    _LOGGER.info("LazyWait setup: admin-WS task created for branch %s", branch_id)

    # Start the near-live camera snapshot loop (~1s): captures a JPEG for each
    # camera the dashboard is viewing now and posts it. This is the SIMPLE
    # near-live path replacing WebRTC. Isolated + best-effort; cancelled on
    # unload inside shutdown_admin_socket alongside the admin socket.
    coordinator.start_snapshot_loop()

    # Store the coordinator directly (unchanged contract for sensor /
    # binary_sensor / hikvision platforms).
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        coordinator = hass.data[DOMAIN].pop(entry.entry_id, None)
        if coordinator is not None:
            await coordinator.shutdown_admin_socket()
    return unloaded
