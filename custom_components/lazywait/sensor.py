"""Sensor entities for the LazyWait integration.

Exposes the cloud connection state inside Home Assistant so a branch admin can
see, from HA itself, that the pairing is live and which config version is in
effect. These mirror what the LazyWait dashboard shows on its side.
"""

from __future__ import annotations

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import LazyWaitCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the LazyWait sensors from a config entry."""
    coordinator: LazyWaitCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([LazyWaitConfigVersionSensor(coordinator)])


class LazyWaitConfigVersionSensor(CoordinatorEntity[LazyWaitCoordinator], SensorEntity):
    """The current cloud config version applied for this branch."""

    _attr_has_entity_name = True
    _attr_name = "Config version"
    _attr_icon = "mdi:cloud-sync"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, coordinator: LazyWaitCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.branch_id}_config_version"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, coordinator.branch_id)},
            "name": f"LazyWait Branch {coordinator.branch_id}",
            "manufacturer": "LazyWait",
        }

    @property
    def native_value(self) -> int:
        return self.coordinator.config_version
