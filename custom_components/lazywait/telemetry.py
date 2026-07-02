"""Smart Branch telemetry sender (spec §3.3, integration ≥ 26.8.0).

The cloud curates which entities matter per branch (`ha_branch_entities`) and
ships the set on /config as `monitored_entities`, plus the reporting cadence
(`report_interval_seconds`), the per-entity staleness floor
(`heartbeat_interval_seconds`) and per-NUMERIC-device_class significant-change
hints (`significant_change`). This module:

  * subscribes to state_changed for EXACTLY the monitored set (re-subscribing
    only when the set actually changes — a config_version bump that doesn't
    touch the set doesn't churn the listener),
  * buffers readings, COALESCING numeric readings per entity (latest wins) so
    a chatty sensor costs one event per flush, matching the cloud's per-entity
    storage cap,
  * flushes every report_interval OR early when a significant change arrives
    (numeric delta since the last COMMITTED reading >= the device_class hint;
    binary/non-numeric entities: only on a state transition, debounced to one
    per TELEMETRY_BINARY_MIN_INTERVAL_SECONDS per entity — raw flip streams
    never leave the branch),
  * forces a per-entity heartbeat reading when an entity has been quiet past
    heartbeat_interval, so the cloud can tell "quiet but healthy" from stale,
  * POSTs batches of ≤ TELEMETRY_MAX_EVENTS_PER_BATCH to the existing /events
    endpoint with the existing bearer + a FRESH Idempotency-Key per batch. On
    failure the batch stays buffered and is retried with the SAME eventIds —
    the cloud dedups durably per event (ha_event_receipts), so ids are NEVER
    regenerated on retry (regenerating them would store duplicates).

Wire contract per event (mirrors the cloud's sensorReadingEventSchema):

    { "type": "sensor_reading",
      "eventId":  "<uuid4>",           # REQUIRED — the durable dedup key
      "entityId": "sensor.kitchen_temp",
      "occurredAt": "<UTC ISO, offset>",
      "payload": { "value": 23.5,       # OMITTED (not null) when non-numeric —
                                        # the cloud schema rejects null
                   "unit": "°C",
                   "state": "23.5",
                   "attributes": { ...allowlist... } } }

The decision logic (config parsing, coalescing, significance, debounce,
heartbeat, batching/retry accounting) lives in HA-free pure code
(`parse_telemetry_config`, `build_reading`, `TelemetryBuffer`) so it is unit
tested without a Home Assistant install; only `TelemetrySender` touches hass.
Like every other cloud-facing loop in this integration it is best-effort and
self-healing — nothing here may ever break the coordinator or die silently.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from homeassistant.core import HomeAssistant, callback

from .api import LazyWaitApiClient, LazyWaitApiError, LazyWaitAuthError
from .const import (
    EVENT_SENSOR_READING,
    TELEMETRY_ATTRIBUTE_ALLOWLIST,
    TELEMETRY_BINARY_MIN_INTERVAL_SECONDS,
    TELEMETRY_DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    TELEMETRY_DEFAULT_REPORT_INTERVAL_SECONDS,
    TELEMETRY_MAX_BUFFERED_EVENTS,
    TELEMETRY_MAX_EVENTS_PER_BATCH,
    TELEMETRY_WAKE_COALESCE_SECONDS,
)

_LOGGER = logging.getLogger(__name__)

# Cloud-side string caps (sensorReadingEventSchema) — trim, never fail.
_STATE_MAX_CHARS = 255
_UNIT_MAX_CHARS = 64
_ATTRIBUTE_MAX_CHARS = 255


def _utc_now_iso() -> str:
    """UTC ISO-8601 with an explicit offset (the cloud requires offset:true)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_numeric(state: Any) -> float | None:
    """A finite float for a numeric state string, else None.

    'on'/'open'/'unavailable' → None (transition semantics). inf/nan parse as
    floats but the cloud schema rejects non-finite — treat them as non-numeric.
    """
    try:
        value = float(state)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def filter_attributes(attributes: Any) -> dict[str, str]:
    """The SMALL telemetry attribute allowlist (friendly_name / device_class /
    unit_of_measurement only). Raw HA attributes can carry stream creds, GPS,
    tokens — and they enter LLM contexts cloud-side — so this is deliberately
    tighter than the admin-snapshot allowlist. Non-string values are dropped
    (nothing in the allowlist is legitimately non-string)."""
    if not isinstance(attributes, dict):
        return {}
    out: dict[str, str] = {}
    for key in TELEMETRY_ATTRIBUTE_ALLOWLIST:
        value = attributes.get(key)
        if isinstance(value, str) and value:
            out[key] = value[:_ATTRIBUTE_MAX_CHARS]
    return out


def build_reading(
    entity_id: str, state: Any, attributes: Any, occurred_at: str
) -> dict[str, Any]:
    """One sensor_reading event. eventId is minted HERE, once per reading —
    a retried flush re-sends the same dict, so the id survives retries."""
    state_str = str(state)
    payload: dict[str, Any] = {
        "state": state_str[:_STATE_MAX_CHARS],
        "attributes": filter_attributes(attributes),
    }
    value = parse_numeric(state_str)
    if value is not None:
        # Omitted entirely when non-numeric — the cloud schema rejects null.
        payload["value"] = value
    unit = attributes.get("unit_of_measurement") if isinstance(attributes, dict) else None
    if isinstance(unit, str) and unit:
        payload["unit"] = unit[:_UNIT_MAX_CHARS]
    return {
        "type": EVENT_SENSOR_READING,
        "eventId": str(uuid.uuid4()),
        "entityId": entity_id,
        "occurredAt": occurred_at,
        "payload": payload,
    }


@dataclass(frozen=True)
class TelemetryConfig:
    """The parsed telemetry block of /config. Defaults mirror the cloud's."""

    monitored_entities: tuple[str, ...] = ()
    report_interval_seconds: int = TELEMETRY_DEFAULT_REPORT_INTERVAL_SECONDS
    heartbeat_interval_seconds: int = TELEMETRY_DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    significant_change: dict[str, float] = field(default_factory=dict)


def _clean_interval(value: Any, default: int) -> int:
    """A positive int interval, else the default (the cloud is untrusted)."""
    if isinstance(value, bool):  # bool is an int subclass — reject explicitly
        return default
    if isinstance(value, (int, float)) and math.isfinite(value) and value > 0:
        return int(value)
    return default


def parse_telemetry_config(config: Any) -> TelemetryConfig:
    """Parse the §3.3 fields off a /config response, tolerating their absence
    (an older cloud simply yields the defaults + an empty monitored set) and
    any junk types (the cloud response is untrusted input)."""
    if not isinstance(config, dict):
        return TelemetryConfig()

    raw_entities = config.get("monitored_entities")
    seen: set[str] = set()
    monitored: list[str] = []
    if isinstance(raw_entities, list):
        for eid in raw_entities:
            if isinstance(eid, str) and eid and eid not in seen:
                seen.add(eid)
                monitored.append(eid)

    hints: dict[str, float] = {}
    raw_hints = config.get("significant_change")
    if isinstance(raw_hints, dict):
        for key, value in raw_hints.items():
            if (
                isinstance(key, str)
                and key
                and not isinstance(value, bool)
                and isinstance(value, (int, float))
                and math.isfinite(value)
                and value > 0
            ):
                hints[key] = float(value)

    return TelemetryConfig(
        monitored_entities=tuple(monitored),
        report_interval_seconds=_clean_interval(
            config.get("report_interval_seconds"),
            TELEMETRY_DEFAULT_REPORT_INTERVAL_SECONDS,
        ),
        heartbeat_interval_seconds=_clean_interval(
            config.get("heartbeat_interval_seconds"),
            TELEMETRY_DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        ),
        significant_change=hints,
    )


class TelemetryBuffer:
    """HA-free buffering + flush accounting. All clock inputs are explicit
    monotonic seconds so the logic is deterministic under test.

    Two stages:
      * ``_pending`` — the latest UNCOMMITTED numeric reading per entity
        (newest replaces oldest; the replaced event was never sent, so
        discarding its eventId is safe).
      * ``_outbox`` — committed events in FIFO order awaiting POST. Batches
        are ``peek``ed (not popped) so a failed POST leaves them — with their
        eventIds — intact for the retry; ``confirm`` removes them only after
        the cloud accepted the batch.

    Binary/non-numeric transitions and heartbeats append straight to the
    outbox (they are discrete facts, not coalescible samples).
    """

    def __init__(self) -> None:
        self._config = TelemetryConfig()
        self._monitored: set[str] = set()
        self._pending: dict[str, dict[str, Any]] = {}
        self._outbox: list[dict[str, Any]] = []
        # Last seen raw state per entity (transition detection).
        self._last_state: dict[str, str] = {}
        # Last COMMITTED numeric value per entity — the significance baseline,
        # so small drifts ACCUMULATE toward the hint instead of resetting on
        # every sample.
        self._last_value: dict[str, float] = {}
        # Monotonic time a reading was last committed per entity (heartbeat).
        self._last_recorded_mono: dict[str, float] = {}
        # Monotonic time of the last RECORDED binary transition (debounce).
        self._last_transition_mono: dict[str, float] = {}
        # Observability: readings dropped to the buffer cap.
        self.dropped_to_cap = 0

    @property
    def report_interval_seconds(self) -> int:
        return self._config.report_interval_seconds

    @property
    def buffered_count(self) -> int:
        return len(self._outbox) + len(self._pending)

    @property
    def monitored_entities(self) -> tuple[str, ...]:
        return self._config.monitored_entities

    def apply_config(self, config: TelemetryConfig) -> None:
        """Adopt a new config; prune per-entity tracking for entities that
        left the monitored set (already-buffered events stay — they were
        curated when recorded; the cloud drops uncurated on ingest anyway)."""
        self._config = config
        self._monitored = set(config.monitored_entities)
        for tracker in (
            self._pending,
            self._last_state,
            self._last_value,
            self._last_recorded_mono,
            self._last_transition_mono,
        ):
            for eid in [k for k in tracker if k not in self._monitored]:
                del tracker[eid]

    # ── Recording ────────────────────────────────────────────────────────────

    def record(
        self,
        entity_id: str,
        state: Any,
        attributes: Any,
        occurred_at: str,
        now: float,
    ) -> bool:
        """Ingest one state_changed. Returns True when the reading warrants an
        EARLY flush (significant numeric delta / recorded binary transition)."""
        if entity_id not in self._monitored:
            return False
        state_str = str(state)
        value = parse_numeric(state_str)
        if value is None:
            return self._record_transition(
                entity_id, state_str, attributes, occurred_at, now
            )
        return self._record_numeric(
            entity_id, state_str, value, attributes, occurred_at, now
        )

    def _record_numeric(
        self,
        entity_id: str,
        state_str: str,
        value: float,
        attributes: Any,
        occurred_at: str,
        now: float,  # noqa: ARG002 - kept symmetrical; numeric commits stamp at flush
    ) -> bool:
        """Coalesce into pending (latest wins); significant when the delta
        since the last COMMITTED value reaches the device_class hint."""
        self._pending[entity_id] = build_reading(
            entity_id, state_str, attributes, occurred_at
        )
        self._last_state[entity_id] = state_str
        baseline = self._last_value.get(entity_id)
        if baseline is None:
            # First reading since startup/curation — it rides the next
            # interval flush (or the startup heartbeat primes it sooner).
            return False
        device_class = (
            attributes.get("device_class") if isinstance(attributes, dict) else None
        )
        hint = (
            self._config.significant_change.get(device_class)
            if isinstance(device_class, str)
            else None
        )
        return hint is not None and abs(value - baseline) >= hint

    def _record_transition(
        self,
        entity_id: str,
        state_str: str,
        attributes: Any,
        occurred_at: str,
        now: float,
    ) -> bool:
        """Binary/non-numeric path: record ONLY state transitions, at most one
        per TELEMETRY_BINARY_MIN_INTERVAL_SECONDS per entity. Debounced flips
        are dropped entirely (raw per-flip streams never leave the branch —
        the heartbeat reconciles the final state within heartbeat_interval)."""
        last = self._last_state.get(entity_id)
        self._last_state[entity_id] = state_str
        if last == state_str:
            return False  # attribute-only change, not a transition
        last_recorded = self._last_transition_mono.get(entity_id)
        if (
            last is not None
            and last_recorded is not None
            and now - last_recorded < TELEMETRY_BINARY_MIN_INTERVAL_SECONDS
        ):
            return False
        self._last_transition_mono[entity_id] = now
        self._append_outbox(
            build_reading(entity_id, state_str, attributes, occurred_at), now
        )
        return True

    # ── Heartbeat ────────────────────────────────────────────────────────────

    def heartbeat_due(self, now: float) -> list[str]:
        """Monitored entities with no committed reading within
        heartbeat_interval (never-recorded counts as due, which primes every
        entity's first value on the first flush after startup/curation).
        Call AFTER commit_pending so fresh samples satisfy the heartbeat."""
        interval = self._config.heartbeat_interval_seconds
        due: list[str] = []
        for entity_id in self._config.monitored_entities:
            last = self._last_recorded_mono.get(entity_id)
            if last is None or now - last >= interval:
                due.append(entity_id)
        return due

    def add_heartbeat(self, event: dict[str, Any], now: float) -> None:
        """Commit a heartbeat reading (built by the caller from live state)."""
        entity_id = event.get("entityId")
        payload = event.get("payload")
        if isinstance(entity_id, str) and isinstance(payload, dict):
            state = payload.get("state")
            if isinstance(state, str):
                self._last_state[entity_id] = state
        self._append_outbox(event, now)

    # ── Flush accounting ─────────────────────────────────────────────────────

    def commit_pending(self, now: float) -> None:
        """Move coalesced numeric readings into the outbox (stamping the
        significance baseline + heartbeat clock). Insertion order preserved."""
        if not self._pending:
            return
        pending, self._pending = self._pending, {}
        for event in pending.values():
            self._append_outbox(event, now)

    def peek_batch(self, max_events: int) -> list[dict[str, Any]]:
        """The next batch WITHOUT removing it — a failed POST retries the very
        same event dicts (same eventIds) next flush."""
        return self._outbox[:max_events]

    def confirm_batch(self, count: int) -> None:
        """Drop the first `count` events after the cloud accepted them."""
        del self._outbox[:count]

    def _append_outbox(self, event: dict[str, Any], now: float) -> None:
        if len(self._outbox) >= TELEMETRY_MAX_BUFFERED_EVENTS:
            # Extended outage: drop OLDEST (a stale reading is the least
            # valuable thing in the buffer) and count it for observability.
            self._outbox.pop(0)
            self.dropped_to_cap += 1
            _LOGGER.warning(
                "LazyWait telemetry buffer full (%s); dropped oldest reading",
                TELEMETRY_MAX_BUFFERED_EVENTS,
            )
        self._outbox.append(event)
        entity_id = event.get("entityId")
        if isinstance(entity_id, str):
            self._last_recorded_mono[entity_id] = now
            payload = event.get("payload")
            value = payload.get("value") if isinstance(payload, dict) else None
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                self._last_value[entity_id] = float(value)


class TelemetrySender:
    """The HA-bound half: state_changed subscription + the flush loop.

    Owned by the coordinator (constructed alongside the media relay / camera
    AI managers), fed each /config via ``apply_config`` on the 30s poll, and
    started/stopped with the entry. Mirrors the snapshot-loop pattern: its own
    asyncio task, everything best-effort, a dead token surfaces NOWHERE here
    (the 30s poll already owns reauth) — the buffer just holds until the next
    flush succeeds.
    """

    def __init__(
        self, hass: HomeAssistant, client: LazyWaitApiClient, branch_id: str
    ) -> None:
        self._hass = hass
        self._client = client
        self._branch_id = branch_id
        self._buffer = TelemetryBuffer()
        self._task: asyncio.Task | None = None
        # Set by a significant change to wake the flush loop early.
        self._wake = asyncio.Event()
        self._unsub: Callable[[], None] | None = None
        # The entity set the live listener was built for — re-subscribe ONLY
        # when this changes (a config_version bump alone doesn't churn it).
        self._subscribed: tuple[str, ...] = ()

    def start(self) -> None:
        """Start the flush loop (called from async_setup_entry)."""
        if self._task is not None:
            return
        self._task = self._hass.loop.create_task(self._flush_loop())
        _LOGGER.info(
            "LazyWait telemetry flush loop started (branch %s)", self._branch_id
        )

    async def stop(self) -> None:
        """Unsubscribe + cancel the flush task (entry unload)."""
        if self._unsub is not None:
            try:
                self._unsub()
            except Exception:  # noqa: BLE001 - teardown must never raise
                pass
            self._unsub = None
        self._subscribed = ()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None

    def apply_config(self, config: Any) -> None:
        """Adopt the telemetry block of a fresh /config poll. Never raises —
        a malformed block must not break the coordinator cycle."""
        cfg = parse_telemetry_config(config)
        self._buffer.apply_config(cfg)
        if cfg.monitored_entities != self._subscribed:
            self._subscribed = cfg.monitored_entities
            self._resubscribe()

    def _resubscribe(self) -> None:
        """(Re)build the state_changed listener for exactly the monitored set."""
        if self._unsub is not None:
            try:
                self._unsub()
            except Exception:  # noqa: BLE001 - teardown must never raise
                pass
            self._unsub = None
        if not self._subscribed:
            _LOGGER.debug("LazyWait telemetry: no monitored entities; not listening")
            return
        try:
            # Lazy import (test harness loads this module HA-free); the helper
            # filters at HA's core event index — cheaper than a bus-wide
            # listener on busy installs.
            from homeassistant.helpers.event import (  # noqa: PLC0415
                async_track_state_change_event,
            )

            self._unsub = async_track_state_change_event(
                self._hass, list(self._subscribed), self._on_state_changed
            )
            _LOGGER.info(
                "LazyWait telemetry: watching %s monitored entities",
                len(self._subscribed),
            )
        except Exception as err:  # noqa: BLE001 - never break the poll cycle
            _LOGGER.warning("LazyWait telemetry subscribe failed: %s", err)

    @callback
    def _on_state_changed(self, event: Any) -> None:
        """One monitored entity changed state — buffer it; wake the flush loop
        when the buffer says the change is significant."""
        data = getattr(event, "data", None)
        new_state = data.get("new_state") if isinstance(data, dict) else None
        if new_state is None:
            return  # entity removed — the heartbeat/orphan path owns this
        try:
            significant = self._buffer.record(
                new_state.entity_id,
                new_state.state,
                dict(new_state.attributes or {}),
                _utc_now_iso(),
                time.monotonic(),
            )
        except Exception as err:  # noqa: BLE001 - listener must never raise
            _LOGGER.debug("telemetry record errored (ignored): %s", err)
            return
        if significant:
            self._wake.set()

    async def _flush_loop(self) -> None:
        """Flush every report_interval, or early on a significant change.

        MUST NEVER raise out (a raised exception kills the bare task silently
        and telemetry stops until reload) — every tick is wrapped; only
        CancelledError propagates so unload stops it cleanly.
        """
        while True:
            try:
                interval = max(1, self._buffer.report_interval_seconds)
                woke = False
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=interval)
                    woke = True
                except asyncio.TimeoutError:
                    pass
                if woke:
                    # Brief coalesce so co-occurring significant changes
                    # (temp + humidity in the same second) share one batch.
                    await asyncio.sleep(TELEMETRY_WAKE_COALESCE_SECONDS)
                self._wake.clear()
                await self._flush()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - loop must never die
                _LOGGER.debug("telemetry flush tick errored (ignored): %s", err)

    async def _flush(self) -> None:
        """One flush: commit coalesced readings → add due heartbeats → POST
        the outbox in ≤ TELEMETRY_MAX_EVENTS_PER_BATCH chunks."""
        now = time.monotonic()
        self._buffer.commit_pending(now)
        for entity_id in self._buffer.heartbeat_due(now):
            state = self._hass.states.get(entity_id)
            if state is None:
                continue  # curated id no longer exists in HA — cloud shows orphan
            self._buffer.add_heartbeat(
                build_reading(
                    entity_id,
                    state.state,
                    dict(state.attributes or {}),
                    _utc_now_iso(),
                ),
                now,
            )

        while True:
            batch = self._buffer.peek_batch(TELEMETRY_MAX_EVENTS_PER_BATCH)
            if not batch:
                return
            try:
                # Fresh Idempotency-Key per batch ATTEMPT (batch composition
                # changes between retries as new readings join); the durable
                # per-event dedup is the eventId, which never changes.
                await self._client.push_events(
                    batch, idempotency_key=str(uuid.uuid4())
                )
            except LazyWaitAuthError as err:
                # The 30s coordinator poll owns reauth; hold the buffer.
                _LOGGER.debug("telemetry flush auth-rejected (ignored): %s", err)
                return
            except LazyWaitApiError as err:
                _LOGGER.warning(
                    "LazyWait telemetry flush failed (%s buffered, will retry "
                    "with the same eventIds): %s",
                    self._buffer.buffered_count,
                    err,
                )
                return
            except Exception as err:  # noqa: BLE001 - never break the loop
                _LOGGER.warning(
                    "LazyWait telemetry flush errored (%s buffered): %s",
                    self._buffer.buffered_count,
                    err,
                )
                return
            self._buffer.confirm_batch(len(batch))
