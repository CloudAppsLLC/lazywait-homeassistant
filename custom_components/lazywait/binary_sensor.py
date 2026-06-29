"""Binary sensor entities for the LazyWait integration.

A single connectivity sensor that reflects whether the last cloud poll
succeeded — the HA-side mirror of the dashboard's online/offline dot.
"""

from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
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
    """Set up the LazyWait connectivity binary sensor."""
    coordinator: LazyWaitCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([LazyWaitConnectivitySensor(coordinator)])


class LazyWaitConnectivitySensor(
    CoordinatorEntity[LazyWaitCoordinator], BinarySensorEntity
):
    """True while the cloud connection is healthy (last poll succeeded)."""

    _attr_has_entity_name = True
    _attr_name = "Cloud connection"
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, coordinator: LazyWaitCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.branch_id}_cloud_connection"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, coordinator.branch_id)},
            "name": f"LazyWait Branch {coordinator.branch_id}",
            "manufacturer": "LazyWait",
        }

    @property
    def is_on(self) -> bool:
        # CoordinatorEntity tracks last_update_success; a connectivity sensor is
        # "on" when connected.
        return self.coordinator.last_update_success
