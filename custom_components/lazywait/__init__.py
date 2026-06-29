"""The LazyWait integration.

Sets up one config entry (one paired branch) by constructing an authenticated
cloud client from the stored token, doing an initial /ping to confirm the token
still works, and starting the coordinator that polls config / pushes events /
heartbeats. A rejected token surfaces as a reauth flow.
"""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import LazyWaitApiClient, LazyWaitApiError, LazyWaitAuthError
from .const import CONF_BASE_URL, CONF_BRANCH_ID, CONF_TOKEN, DOMAIN
from .coordinator import LazyWaitCoordinator

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

    coordinator = LazyWaitCoordinator(hass, client, branch_id)
    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unloaded
