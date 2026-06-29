"""Coordinator: polls cloud config, pushes events, sends heartbeats.

The coordinator is the single owner of the cloud connection for one paired
branch. On each refresh it:
  1. GET /config — applies the latest thresholds/entity-map/version.
  2. Flushes any buffered events to POST /events (with an Idempotency-Key so a
     re-flush after a reconnect is de-duped cloud-side).
  3. POST /status — reports HA version, online state, current config version.

A 401 anywhere raises ConfigEntryAuthFailed, which makes HA start the reauth
flow (re-prompt for a fresh pairing code) instead of silently going stale.

Local presence/absence DECISIONS are made by HA automations (or a future local
helper) and handed to `queue_event`; the cloud only stores thresholds and fans
out notifications. The coordinator never decides absence itself.
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import timedelta
from typing import Any

from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import LazyWaitApiClient, LazyWaitApiError, LazyWaitAuthError
from .const import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    DOMAIN,
    INTEGRATION_VERSION,
)

_LOGGER = logging.getLogger(__name__)

# Cap the in-memory buffer so an extended cloud outage can't grow it unbounded.
# Oldest events are dropped first (a stale absence is less useful than a fresh
# one); the drop is logged so the gap is visible.
_MAX_BUFFER = 1000


class LazyWaitCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Owns the cloud poll/push loop for one branch."""

    def __init__(
        self, hass: HomeAssistant, client: LazyWaitApiClient, branch_id: str
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{branch_id}",
            update_interval=timedelta(seconds=DEFAULT_POLL_INTERVAL_SECONDS),
        )
        self._client = client
        self._branch_id = branch_id
        self._buffer: deque[dict[str, Any]] = deque(maxlen=_MAX_BUFFER)
        self._config_version = 0
        self._last_event_at: str | None = None
        # Monotonic counter → a stable Idempotency-Key per flush attempt so a
        # retried flush of the same batch is recognized cloud-side.
        self._flush_seq = 0

    @property
    def branch_id(self) -> str:
        return self._branch_id

    @property
    def config_version(self) -> int:
        return self._config_version

    def queue_event(
        self, event_type: str, entity_id: str, occurred_at: str, payload: dict[str, Any]
    ) -> None:
        """Buffer an event for the next flush. Called by automations/helpers."""
        if len(self._buffer) == self._buffer.maxlen:
            _LOGGER.warning(
                "LazyWait event buffer full (%s); dropping oldest event",
                self._buffer.maxlen,
            )
        self._buffer.append(
            {
                "type": event_type,
                "entityId": entity_id,
                "occurredAt": occurred_at,
                "payload": payload or {},
            }
        )
        self._last_event_at = occurred_at

    async def _async_update_data(self) -> dict[str, Any]:
        """One poll cycle: config → flush events → heartbeat."""
        try:
            config = await self._client.get_config()
            new_version = config.get("version")
            if isinstance(new_version, int):
                self._config_version = new_version
                # Apply the cloud's poll interval if it changed.
                interval = config.get("pollIntervalSeconds")
                if isinstance(interval, int) and interval > 0:
                    self.update_interval = timedelta(seconds=interval)

            await self._flush_events()

            await self._client.report_status(
                ha_version=HA_VERSION,
                integration_version=INTEGRATION_VERSION,
                online=True,
                config_version=self._config_version,
                last_event_at=self._last_event_at,
            )
            return config
        except LazyWaitAuthError as err:
            # Token rotated/revoked → HA opens the reauth flow.
            raise ConfigEntryAuthFailed(str(err)) from err
        except LazyWaitApiError as err:
            raise UpdateFailed(f"LazyWait cloud poll failed: {err}") from err

    async def _flush_events(self) -> None:
        """Drain the buffer to the cloud; re-buffer on failure for next cycle."""
        if not self._buffer:
            return
        batch = list(self._buffer)
        self._flush_seq += 1
        idempotency_key = f"{self._branch_id}:{self._flush_seq}"
        try:
            await self._client.push_events(batch, idempotency_key=idempotency_key)
            # Only clear what we successfully sent — events queued DURING the
            # await stay buffered for the next flush.
            for _ in range(len(batch)):
                if self._buffer:
                    self._buffer.popleft()
        except LazyWaitAuthError:
            raise
        except LazyWaitApiError as err:
            _LOGGER.warning(
                "LazyWait event flush failed (%s buffered): %s", len(batch), err
            )
            # Leave the buffer intact; next cycle retries with the SAME seq+1
            # only after a success, so dedup holds.
            self._flush_seq -= 1
