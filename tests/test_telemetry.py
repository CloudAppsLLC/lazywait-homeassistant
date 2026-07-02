"""Unit tests for the Smart Branch telemetry sender's pure core (telemetry.py):

  * parse_telemetry_config — defaults, tolerance of an older cloud (absent
    fields) and junk types,
  * build_reading — the sensor_reading wire shape (value OMITTED when
    non-numeric, unit from attributes, the 3-key attribute allowlist),
  * TelemetryBuffer — numeric coalescing, significant-change wake, binary
    transition + 60s debounce, per-entity heartbeat, 400-cap batching, and
    the retry-keeps-the-same-eventIds invariant.

All clock inputs are explicit monotonic seconds, so no sleeping/mocking.
"""

import uuid

from custom_components.lazywait.const import (
    TELEMETRY_BINARY_MIN_INTERVAL_SECONDS,
    TELEMETRY_DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    TELEMETRY_DEFAULT_REPORT_INTERVAL_SECONDS,
    TELEMETRY_MAX_BUFFERED_EVENTS,
    TELEMETRY_MAX_EVENTS_PER_BATCH,
)
from custom_components.lazywait.telemetry import (
    TelemetryBuffer,
    TelemetryConfig,
    build_reading,
    filter_attributes,
    parse_numeric,
    parse_telemetry_config,
)

OCCURRED_AT = "2026-07-02T10:00:00.000+00:00"

TEMP_ATTRS = {
    "friendly_name": "Kitchen temp",
    "device_class": "temperature",
    "unit_of_measurement": "°C",
}


def make_buffer(
    entities=("sensor.kitchen_temp", "binary_sensor.front_door"),
    hints=None,
    heartbeat=TELEMETRY_DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
) -> TelemetryBuffer:
    buf = TelemetryBuffer()
    buf.apply_config(
        TelemetryConfig(
            monitored_entities=tuple(entities),
            heartbeat_interval_seconds=heartbeat,
            significant_change=hints if hints is not None else {"temperature": 0.5},
        )
    )
    return buf


def flush_all(buf: TelemetryBuffer, now: float) -> list:
    """Commit pending + drain the outbox the way a successful flush would."""
    buf.commit_pending(now)
    drained = []
    while True:
        batch = buf.peek_batch(TELEMETRY_MAX_EVENTS_PER_BATCH)
        if not batch:
            return drained
        drained.extend(batch)
        buf.confirm_batch(len(batch))


# ── parse_telemetry_config ────────────────────────────────────────────────────


def test_parse_config_defaults_when_fields_absent() -> None:
    # An older cloud ships none of the §3.3 fields — defaults + empty set.
    cfg = parse_telemetry_config({"branchId": "b1", "version": 3})
    assert cfg.monitored_entities == ()
    assert cfg.report_interval_seconds == TELEMETRY_DEFAULT_REPORT_INTERVAL_SECONDS
    assert cfg.heartbeat_interval_seconds == TELEMETRY_DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    assert cfg.significant_change == {}


def test_parse_config_non_dict_yields_defaults() -> None:
    assert parse_telemetry_config(None).monitored_entities == ()
    assert parse_telemetry_config([1, 2]).report_interval_seconds == (
        TELEMETRY_DEFAULT_REPORT_INTERVAL_SECONDS
    )


def test_parse_config_reads_the_wire_fields() -> None:
    cfg = parse_telemetry_config(
        {
            "monitored_entities": ["sensor.a", "sensor.b", "sensor.a", "", 7],
            "report_interval_seconds": 120,
            "heartbeat_interval_seconds": 600,
            "significant_change": {"temperature": 0.5, "humidity": 2},
        }
    )
    # Deduped, non-strings dropped, order preserved.
    assert cfg.monitored_entities == ("sensor.a", "sensor.b")
    assert cfg.report_interval_seconds == 120
    assert cfg.heartbeat_interval_seconds == 600
    assert cfg.significant_change == {"temperature": 0.5, "humidity": 2.0}


def test_parse_config_rejects_junk_values() -> None:
    cfg = parse_telemetry_config(
        {
            "monitored_entities": "sensor.a",  # not a list
            "report_interval_seconds": -5,
            "heartbeat_interval_seconds": True,  # bool is not an interval
            "significant_change": {"temperature": "hot", "humidity": -2, "": 1},
        }
    )
    assert cfg.monitored_entities == ()
    assert cfg.report_interval_seconds == TELEMETRY_DEFAULT_REPORT_INTERVAL_SECONDS
    assert cfg.heartbeat_interval_seconds == TELEMETRY_DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    assert cfg.significant_change == {}


# ── parse_numeric / filter_attributes / build_reading ────────────────────────


def test_parse_numeric() -> None:
    assert parse_numeric("23.5") == 23.5
    assert parse_numeric("-3") == -3.0
    assert parse_numeric("on") is None
    assert parse_numeric("unavailable") is None
    assert parse_numeric("inf") is None  # cloud schema rejects non-finite
    assert parse_numeric("nan") is None


def test_filter_attributes_allowlist_only() -> None:
    filtered = filter_attributes(
        {
            "friendly_name": "Kitchen temp",
            "device_class": "temperature",
            "unit_of_measurement": "°C",
            "access_token": "SECRET",  # must never cross the wire
            "gps": [1, 2],
            "brightness": 200,
        }
    )
    assert filtered == {
        "friendly_name": "Kitchen temp",
        "device_class": "temperature",
        "unit_of_measurement": "°C",
    }
    assert filter_attributes(None) == {}


def test_build_reading_numeric_shape() -> None:
    event = build_reading("sensor.kitchen_temp", "23.5", TEMP_ATTRS, OCCURRED_AT)
    assert event["type"] == "sensor_reading"
    assert event["entityId"] == "sensor.kitchen_temp"
    assert event["occurredAt"] == OCCURRED_AT
    uuid.UUID(event["eventId"])  # a valid uuid — the cloud requires z.string().uuid()
    assert event["payload"]["value"] == 23.5
    assert event["payload"]["state"] == "23.5"
    assert event["payload"]["unit"] == "°C"
    assert set(event["payload"]["attributes"]) <= {
        "friendly_name",
        "device_class",
        "unit_of_measurement",
    }


def test_build_reading_omits_value_for_non_numeric() -> None:
    # The cloud schema is value: z.number().finite().optional() — null FAILS
    # parse, so a non-numeric state must OMIT the key entirely.
    event = build_reading("binary_sensor.front_door", "on", {"device_class": "door"}, OCCURRED_AT)
    assert "value" not in event["payload"]
    assert event["payload"]["state"] == "on"
    assert "unit" not in event["payload"]


# ── numeric coalescing + significant change ──────────────────────────────────


def test_numeric_readings_coalesce_to_latest_per_entity() -> None:
    buf = make_buffer()
    buf.record("sensor.kitchen_temp", "23.1", TEMP_ATTRS, OCCURRED_AT, now=0.0)
    buf.record("sensor.kitchen_temp", "23.2", TEMP_ATTRS, OCCURRED_AT, now=1.0)
    buf.record("sensor.kitchen_temp", "23.3", TEMP_ATTRS, OCCURRED_AT, now=2.0)
    events = flush_all(buf, now=3.0)
    # One coalesced reading, carrying the LATEST value.
    assert len(events) == 1
    assert events[0]["payload"]["value"] == 23.3


def test_first_reading_is_not_significant() -> None:
    buf = make_buffer()
    # No committed baseline yet — first reading rides the interval flush.
    assert buf.record("sensor.kitchen_temp", "23.0", TEMP_ATTRS, OCCURRED_AT, 0.0) is False


def test_significant_change_wakes_after_a_committed_baseline() -> None:
    buf = make_buffer(hints={"temperature": 0.5})
    buf.record("sensor.kitchen_temp", "23.0", TEMP_ATTRS, OCCURRED_AT, 0.0)
    flush_all(buf, now=1.0)  # commits the 23.0 baseline
    # Below the 0.5 hint → buffered but not significant.
    assert buf.record("sensor.kitchen_temp", "23.3", TEMP_ATTRS, OCCURRED_AT, 2.0) is False
    # Drift ACCUMULATES against the committed baseline: 23.6 - 23.0 >= 0.5.
    assert buf.record("sensor.kitchen_temp", "23.6", TEMP_ATTRS, OCCURRED_AT, 3.0) is True


def test_no_hint_for_device_class_means_never_significant() -> None:
    buf = make_buffer(hints={})
    buf.record("sensor.kitchen_temp", "23.0", TEMP_ATTRS, OCCURRED_AT, 0.0)
    flush_all(buf, now=1.0)
    assert buf.record("sensor.kitchen_temp", "99.0", TEMP_ATTRS, OCCURRED_AT, 2.0) is False
    # Still buffered for the interval flush though.
    assert buf.buffered_count == 1


def test_unmonitored_entity_is_ignored() -> None:
    buf = make_buffer(entities=("sensor.kitchen_temp",))
    assert buf.record("sensor.other", "1", {}, OCCURRED_AT, 0.0) is False
    assert buf.buffered_count == 0


# ── binary transitions + debounce ────────────────────────────────────────────


def test_binary_transition_records_and_wakes() -> None:
    buf = make_buffer()
    # First sighting counts as a transition (primes the cloud's state).
    assert buf.record("binary_sensor.front_door", "off", {}, OCCURRED_AT, 0.0) is True
    # A real transition after the debounce window records + wakes.
    now = TELEMETRY_BINARY_MIN_INTERVAL_SECONDS + 1.0
    assert buf.record("binary_sensor.front_door", "on", {}, OCCURRED_AT, now) is True
    events = flush_all(buf, now=now + 1.0)
    assert [e["payload"]["state"] for e in events] == ["off", "on"]


def test_binary_flip_within_debounce_is_dropped() -> None:
    buf = make_buffer()
    buf.record("binary_sensor.front_door", "off", {}, OCCURRED_AT, 0.0)
    flush_all(buf, now=0.5)
    # Rapid flips inside the 60s window: dropped, not deferred.
    assert buf.record("binary_sensor.front_door", "on", {}, OCCURRED_AT, 10.0) is False
    assert buf.record("binary_sensor.front_door", "off", {}, OCCURRED_AT, 20.0) is False
    assert buf.buffered_count == 0
    # After the window a fresh transition records again.
    now = TELEMETRY_BINARY_MIN_INTERVAL_SECONDS + 5.0
    assert buf.record("binary_sensor.front_door", "on", {}, OCCURRED_AT, now) is True


def test_binary_same_state_is_not_a_transition() -> None:
    buf = make_buffer()
    buf.record("binary_sensor.front_door", "on", {}, OCCURRED_AT, 0.0)
    flush_all(buf, now=1.0)
    # Attribute-only change → same state string → nothing recorded, even
    # though the debounce window has long passed.
    now = TELEMETRY_BINARY_MIN_INTERVAL_SECONDS * 3.0
    assert buf.record("binary_sensor.front_door", "on", {}, OCCURRED_AT, now) is False
    assert buf.buffered_count == 0


# ── heartbeat ────────────────────────────────────────────────────────────────


def test_heartbeat_due_for_never_recorded_entities() -> None:
    buf = make_buffer(entities=("sensor.a", "sensor.b"))
    # Nothing recorded yet — both due (this primes first values at startup).
    assert buf.heartbeat_due(0.0) == ["sensor.a", "sensor.b"]


def test_heartbeat_not_due_after_a_fresh_reading() -> None:
    buf = make_buffer(entities=("sensor.a",), heartbeat=300)
    buf.record("sensor.a", "1.0", {}, OCCURRED_AT, 0.0)
    flush_all(buf, now=0.0)  # commit stamps the heartbeat clock
    assert buf.heartbeat_due(299.0) == []
    assert buf.heartbeat_due(300.0) == ["sensor.a"]


def test_add_heartbeat_satisfies_the_heartbeat_and_updates_state() -> None:
    buf = make_buffer(entities=("binary_sensor.front_door",), heartbeat=300)
    event = build_reading("binary_sensor.front_door", "on", {}, OCCURRED_AT)
    buf.add_heartbeat(event, now=1000.0)
    assert buf.heartbeat_due(1100.0) == []
    # The heartbeat updated the last-seen state, so the same state later is
    # NOT a transition.
    assert (
        buf.record("binary_sensor.front_door", "on", {}, OCCURRED_AT, 2000.0) is False
    )


# ── batching + retry semantics ───────────────────────────────────────────────


def test_batches_cap_at_400() -> None:
    entities = tuple(f"sensor.s{i}" for i in range(450))
    buf = make_buffer(entities=entities, hints={})
    for i, eid in enumerate(entities):
        buf.record(eid, str(i), {}, OCCURRED_AT, 0.0)
    buf.commit_pending(1.0)
    first = buf.peek_batch(TELEMETRY_MAX_EVENTS_PER_BATCH)
    assert len(first) == 400
    buf.confirm_batch(len(first))
    second = buf.peek_batch(TELEMETRY_MAX_EVENTS_PER_BATCH)
    assert len(second) == 50
    # No overlap between confirmed batches.
    assert {e["eventId"] for e in first}.isdisjoint({e["eventId"] for e in second})


def test_failed_flush_retries_with_the_same_event_ids() -> None:
    buf = make_buffer(entities=("sensor.a", "sensor.b"), hints={})
    buf.record("sensor.a", "1", {}, OCCURRED_AT, 0.0)
    buf.record("sensor.b", "2", {}, OCCURRED_AT, 0.0)
    buf.commit_pending(1.0)
    attempt1 = buf.peek_batch(TELEMETRY_MAX_EVENTS_PER_BATCH)
    ids1 = [e["eventId"] for e in attempt1]
    # POST failed → nothing confirmed. The retry must carry the SAME ids
    # (the cloud dedups durably per eventId — regenerating would duplicate).
    attempt2 = buf.peek_batch(TELEMETRY_MAX_EVENTS_PER_BATCH)
    assert [e["eventId"] for e in attempt2] == ids1
    # New readings joining the retry batch don't disturb the retried ids.
    buf.record("sensor.a", "3", {}, OCCURRED_AT, 2.0)
    buf.commit_pending(3.0)
    attempt3 = buf.peek_batch(TELEMETRY_MAX_EVENTS_PER_BATCH)
    assert [e["eventId"] for e in attempt3[:2]] == ids1
    assert len(attempt3) == 3


def test_outbox_cap_drops_oldest() -> None:
    buf = make_buffer(entities=("binary_sensor.d",), hints={})
    # Overfill via add_heartbeat (uncapped path input, capped on append).
    for i in range(TELEMETRY_MAX_BUFFERED_EVENTS + 5):
        buf.add_heartbeat(
            build_reading("binary_sensor.d", f"state{i}", {}, OCCURRED_AT), float(i)
        )
    assert buf.buffered_count == TELEMETRY_MAX_BUFFERED_EVENTS
    assert buf.dropped_to_cap == 5
    first = buf.peek_batch(1)[0]
    # The oldest survivors start at state5 (0..4 dropped).
    assert first["payload"]["state"] == "state5"


# ── config re-application ────────────────────────────────────────────────────


def test_apply_config_prunes_tracking_for_removed_entities() -> None:
    buf = make_buffer(entities=("sensor.a", "sensor.b"), hints={"temperature": 0.5})
    buf.record("sensor.a", "1.0", TEMP_ATTRS, OCCURRED_AT, 0.0)
    flush_all(buf, now=1.0)
    # sensor.a leaves the curated set …
    buf.apply_config(
        TelemetryConfig(
            monitored_entities=("sensor.b",),
            significant_change={"temperature": 0.5},
        )
    )
    # … so it is no longer recorded, and no longer heartbeat-due.
    assert buf.record("sensor.a", "9.9", TEMP_ATTRS, OCCURRED_AT, 2.0) is False
    assert buf.heartbeat_due(1e9) == ["sensor.b"]
