from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from src.collector.storage import (
    ACTIVE_SCENARIO_GRACE_SECONDS,
    ObservedMessage,
    ObservationStore,
)
from src.common.messages import utc_now

DEVICE_ID = "iot-001"
SESSION_ID = "29ab4b25-c9ad-4f11-86b3-dabfb06a02cc"
RUN_ID = "71fdc845-3be7-4711-a673-46894844274a"


def observed(
    topic: str,
    payload: dict[str, object] | bytes,
    at: float,
    *,
    retain: bool = False,
    duplicate: bool = False,
) -> ObservedMessage:
    encoded = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return ObservedMessage(
        topic=topic,
        payload=encoded,
        qos=1,
        retain=retain,
        duplicate=duplicate,
        received_at_unix=at,
    )


def status(state: str) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "device_id": DEVICE_ID,
        "session_id": SESSION_ID,
        "state": state,
        "sent_at_utc": "2026-11-08T09:00:00Z",
    }


def experiment(event: str, *, run_id: str = RUN_ID) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "event_id": (
            "cb8df2e0-3488-4c44-91f8-239b8016675b"
            if event == "scenario_started"
            else "9f9f448a-aa2d-4272-ad3a-b47d43f67fd5"
        ),
        "device_id": DEVICE_ID,
        "session_id": SESSION_ID,
        "event_at_utc": utc_now(),
        "event": event,
        "scenario": "message_burst",
        "experiment_run_id": run_id,
        "request_id": "c410f117-93c7-40ca-9147-e57fa4088e83",
        "seed": 42,
        "duration_seconds": 30,
        "details": {},
    }


def telemetry(sequence: int, *, session_id: str = SESSION_ID) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "device_id": DEVICE_ID,
        "device_type": "environment_sensor",
        "session_id": session_id,
        "sequence_no": sequence,
        "sent_at_utc": "2026-11-08T09:00:01Z",
        "measurements": {
            "temperature_c": 23.0,
            "relative_humidity_pct": 50.0,
        },
    }


def test_store_labels_scenario_and_calculates_inter_arrival(tmp_path: Path) -> None:
    database = tmp_path / "observations.db"
    store = ObservationStore(str(database))
    event_topic = f"experiments/v1/devices/{DEVICE_ID}/events"
    telemetry_topic = f"iot/v1/devices/{DEVICE_ID}/telemetry"
    base_time = time.time()
    store.process(observed(event_topic, experiment("scenario_started"), base_time))
    store.process(observed(telemetry_topic, telemetry(1), base_time + 1.0))
    store.process(observed(telemetry_topic, telemetry(2), base_time + 1.25))
    store.process(observed(event_topic, experiment("scenario_ended"), base_time + 2.0))
    store.process(observed(telemetry_topic, telemetry(3), base_time + 3.0))
    store.close()

    connection = sqlite3.connect(database)
    rows = connection.execute(
        """SELECT ground_truth_label, scenario_run_id, inter_arrival_seconds
           FROM mqtt_observations WHERE message_type = 'telemetry' ORDER BY id"""
    ).fetchall()
    assert rows == [
        ("message_burst", RUN_ID, None),
        ("message_burst", RUN_ID, 0.25),
        ("normal", None, 1.75),
    ]
    assert connection.execute("SELECT COUNT(*) FROM scenario_events").fetchone()[0] == 2
    assert connection.execute("SELECT last_sequence FROM devices").fetchone()[0] == 3
    connection.close()


def test_store_preserves_invalid_payload_with_validation_error(tmp_path: Path) -> None:
    database = tmp_path / "invalid.db"
    store = ObservationStore(str(database))
    store.process(
        observed("iot/v1/devices/iot-002/telemetry", b"not-json", 100.0)
    )
    row = store.connection.execute(
        """SELECT payload_valid, validation_error, payload_text
           FROM mqtt_observations"""
    ).fetchone()
    assert tuple(row) == (0, "payload is not valid JSON: Expecting value", "not-json")
    assert store.connection.execute("SELECT COUNT(*) FROM devices").fetchone()[0] == 0
    store.close()


def test_store_records_out_of_range_number_as_invalid_metadata(tmp_path: Path) -> None:
    store = ObservationStore(str(tmp_path / "invalid-number.db"))
    payload = telemetry(1)
    payload["measurements"] = {"unsafe_integer": 10**1000}
    store.process(
        observed(f"iot/v1/devices/{DEVICE_ID}/telemetry", payload, 100.0)
    )
    shallow = telemetry(2)
    shallow.pop("measurements")
    deep_json = (
        json.dumps(shallow)[:-1]
        + ',"measurements":{"nested":'
        + "[" * 1100
        + "0"
        + "]" * 1100
        + "}}"
    ).encode()
    assert len(deep_json) < 4096
    store.process(
        observed(f"iot/v1/devices/{DEVICE_ID}/telemetry", deep_json, 101.0)
    )
    rows = store.connection.execute(
        "SELECT payload_valid, validation_error FROM mqtt_observations ORDER BY id"
    ).fetchall()
    assert rows[0][0] == 0
    assert "number outside the supported range" in rows[0][1]
    assert rows[1][0] == 0
    assert "supported" in rows[1][1] and "depth" in rows[1][1]
    store.close()


def test_application_duplicate_is_kept_but_excluded_from_timing(tmp_path: Path) -> None:
    database = tmp_path / "duplicate.db"
    store = ObservationStore(str(database))
    topic = f"iot/v1/devices/{DEVICE_ID}/telemetry"
    store.process(observed(topic, telemetry(7), 100.0))
    store.process(observed(topic, telemetry(7), 101.0))
    store.process(observed(topic, telemetry(8), 102.0))
    rows = store.connection.execute(
        """SELECT sequence, duplicate, inter_arrival_seconds
           FROM mqtt_observations ORDER BY id"""
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (7, 0, None),
        (7, 1, None),
        (8, 0, 2.0),
    ]
    assert store.connection.execute("SELECT last_sequence FROM devices").fetchone()[0] == 8
    store.close()


def test_mqtt_dup_flag_does_not_discard_first_application_key(
    tmp_path: Path,
) -> None:
    store = ObservationStore(str(tmp_path / "mqtt-dup.db"))
    topic = f"iot/v1/devices/{DEVICE_ID}/telemetry"
    store.process(observed(topic, telemetry(9), 100.0, duplicate=True))
    store.process(observed(topic, telemetry(9), 101.0))
    rows = store.connection.execute(
        """SELECT mqtt_duplicate, duplicate, inter_arrival_seconds
           FROM mqtt_observations ORDER BY id"""
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (1, 0, None),
        (0, 1, None),
    ]
    assert store.connection.execute("SELECT last_sequence FROM devices").fetchone()[0] == 9
    store.close()


def test_payload_over_limit_is_not_retained_or_registered_as_device(
    tmp_path: Path,
) -> None:
    database = tmp_path / "oversized.db"
    store = ObservationStore(str(database), max_payload_bytes=512)
    oversized = json.dumps(telemetry(1) | {"padding": "x" * 800}).encode()
    store.process(
        observed(f"iot/v1/devices/{DEVICE_ID}/telemetry", oversized, 100.0)
    )
    row = store.connection.execute(
        """SELECT payload_valid, validation_error, payload_text, payload_size_bytes
           FROM mqtt_observations"""
    ).fetchone()
    assert row[0] == 0
    assert "exceeds 512-byte" in row[1]
    assert row[2].startswith("[payload omitted:")
    assert row[3] == len(oversized)
    assert store.connection.execute("SELECT COUNT(*) FROM devices").fetchone()[0] == 0
    store.close()


def test_new_session_can_restart_sequence_numbers(tmp_path: Path) -> None:
    store = ObservationStore(str(tmp_path / "sessions.db"))
    topic = f"iot/v1/devices/{DEVICE_ID}/telemetry"
    store.process(observed(topic, telemetry(0), 100.0))
    store.process(
        observed(
            topic,
            telemetry(0, session_id="c08d1328-0c7a-4fc5-8ccd-34c195bb2aa5"),
            101.0,
        )
    )
    duplicates = store.connection.execute(
        "SELECT duplicate FROM mqtt_observations ORDER BY id"
    ).fetchall()
    assert [row[0] for row in duplicates] == [0, 0]
    store.close()


def test_schema_v1_database_is_migrated_without_dropping_rows(tmp_path: Path) -> None:
    database = tmp_path / "v1.db"
    partial_database = tmp_path / "partial-v1.db"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE schema_metadata (
            schema_version INTEGER NOT NULL,
            initialized_at TEXT NOT NULL
        );
        INSERT INTO schema_metadata VALUES (1, '2026-11-08T00:00:00Z');
        CREATE TABLE mqtt_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            received_at_unix REAL NOT NULL,
            device_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            message_type TEXT NOT NULL CHECK(message_type IN ('telemetry', 'status')),
            message_id TEXT,
            publisher_timestamp TEXT,
            sequence INTEGER,
            payload_size_bytes INTEGER NOT NULL,
            payload_text TEXT NOT NULL,
            qos INTEGER NOT NULL,
            retain INTEGER NOT NULL,
            duplicate INTEGER NOT NULL,
            payload_valid INTEGER NOT NULL,
            validation_error TEXT,
            inter_arrival_seconds REAL,
            ground_truth_label TEXT NOT NULL,
            scenario_run_id TEXT
        );
        INSERT INTO mqtt_observations (
            received_at, received_at_unix, device_id, topic, message_type,
            payload_size_bytes, payload_text, qos, retain, duplicate,
            payload_valid, ground_truth_label
        ) VALUES (
            '2026-11-08T00:00:00Z', 1.0, 'legacy-001',
            'iot/legacy-001/telemetry', 'telemetry', 2, '{}', 1, 0, 1, 1,
            'normal'
        );
        """
    )
    connection.commit()
    partial = sqlite3.connect(partial_database)
    connection.backup(partial)
    partial.execute("ALTER TABLE mqtt_observations ADD COLUMN session_id TEXT")
    partial.execute(
        """ALTER TABLE mqtt_observations
           ADD COLUMN mqtt_duplicate INTEGER NOT NULL DEFAULT 0"""
    )
    partial.commit()
    partial.close()
    connection.close()

    store = ObservationStore(str(database))
    assert store.connection.execute(
        "SELECT schema_version FROM schema_metadata"
    ).fetchone()[0] == 3
    columns = {
        row[1]
        for row in store.connection.execute(
            "PRAGMA table_info(mqtt_observations)"
        ).fetchall()
    }
    assert "session_id" in columns
    assert "mqtt_duplicate" in columns
    assert store.connection.execute(
        "SELECT COUNT(*) FROM mqtt_observations"
    ).fetchone()[0] == 1
    assert tuple(
        store.connection.execute(
            "SELECT mqtt_duplicate, duplicate FROM mqtt_observations"
        ).fetchone()
    ) == (1, 0)
    store.close()

    partial_store = ObservationStore(str(partial_database))
    assert tuple(
        partial_store.connection.execute(
            """SELECT schema_version, maintenance_revision
               FROM schema_metadata"""
        ).fetchone()
    ) == (3, 1)
    assert tuple(
        partial_store.connection.execute(
            "SELECT mqtt_duplicate, duplicate FROM mqtt_observations"
        ).fetchone()
    ) == (1, 0)
    partial_store.close()


def test_schema_v2_database_is_migrated_to_separate_mqtt_dup_flag(
    tmp_path: Path,
) -> None:
    database = tmp_path / "v2.db"
    partial_database = tmp_path / "partial-v2.db"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE schema_metadata (
            schema_version INTEGER NOT NULL,
            initialized_at TEXT NOT NULL
        );
        INSERT INTO schema_metadata VALUES (2, '2026-11-08T00:00:00Z');
        CREATE TABLE mqtt_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            received_at_unix REAL NOT NULL,
            device_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            message_type TEXT NOT NULL CHECK(message_type IN ('telemetry', 'status')),
            message_id TEXT,
            publisher_timestamp TEXT,
            sequence INTEGER,
            payload_size_bytes INTEGER NOT NULL,
            payload_text TEXT NOT NULL,
            qos INTEGER NOT NULL,
            retain INTEGER NOT NULL,
            duplicate INTEGER NOT NULL,
            payload_valid INTEGER NOT NULL,
            validation_error TEXT,
            inter_arrival_seconds REAL,
            ground_truth_label TEXT NOT NULL,
            scenario_run_id TEXT,
            session_id TEXT
        );
        INSERT INTO mqtt_observations (
            received_at, received_at_unix, device_id, topic, message_type,
            session_id, sequence, payload_size_bytes, payload_text, qos,
            retain, duplicate, payload_valid, ground_truth_label
        ) VALUES
            ('2026-11-08T00:00:00Z', 1.0, 'iot-001',
             'iot/v1/devices/iot-001/telemetry', 'telemetry',
             '29ab4b25-c9ad-4f11-86b3-dabfb06a02cc', 1, 2, '{}', 1, 0, 1, 1,
             'normal'),
            ('2026-11-08T00:00:00.500000Z', 1.5, 'iot-001',
             'iot/v1/devices/iot-001/telemetry', 'telemetry',
             '29ab4b25-c9ad-4f11-86b3-dabfb06a02cc', 1, 2, '{}', 1, 0, 0, 1,
             'normal'),
            ('2026-11-08T00:00:01Z', 2.0, 'iot-001',
             'iot/v1/devices/iot-001/telemetry', 'telemetry',
             '29ab4b25-c9ad-4f11-86b3-dabfb06a02cc', 2, 2, '{}', 1, 0, 0, 1,
             'normal'),
            ('2026-11-08T00:00:02Z', 3.0, 'legacy-001',
             'iot/legacy-001/telemetry', 'telemetry', NULL, 1, 2, '{}', 1, 0,
             1, 1, 'normal');
        """
    )
    connection.commit()
    partial = sqlite3.connect(partial_database)
    connection.backup(partial)
    partial.execute(
        """ALTER TABLE mqtt_observations
           ADD COLUMN mqtt_duplicate INTEGER NOT NULL DEFAULT 0"""
    )
    partial.commit()
    partial.close()
    connection.close()

    store = ObservationStore(str(database))
    assert store.connection.execute(
        "SELECT schema_version FROM schema_metadata"
    ).fetchone()[0] == 3
    rows = store.connection.execute(
        "SELECT session_id, mqtt_duplicate, duplicate FROM mqtt_observations ORDER BY id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (SESSION_ID, 0, 0),
        (SESSION_ID, 0, 1),
        (SESSION_ID, 0, 0),
        (None, 1, 0),
    ]
    derived = store.connection.execute(
        """SELECT sequence, inter_arrival_seconds
           FROM mqtt_observations
           WHERE device_id = 'iot-001' ORDER BY id"""
    ).fetchall()
    assert [tuple(row) for row in derived] == [(1, None), (1, None), (2, 1.0)]
    device = store.connection.execute(
        """SELECT last_sequence, last_telemetry_at
           FROM devices WHERE device_id = 'iot-001'"""
    ).fetchone()
    assert tuple(device) == (2, "2026-11-08T00:00:01Z")
    assert store.connection.execute(
        "SELECT maintenance_revision FROM schema_metadata"
    ).fetchone()[0] == 1
    with store.connection:
        store.connection.execute(
            """UPDATE mqtt_observations SET inter_arrival_seconds = 99
               WHERE device_id = 'iot-001' AND sequence = 2"""
        )
        store.connection.execute(
            "UPDATE devices SET last_sequence = 999 WHERE device_id = 'iot-001'"
        )
        store.connection.execute(
            "UPDATE schema_metadata SET maintenance_revision = 0"
        )
    store.close()

    repaired = ObservationStore(str(database))
    assert repaired.connection.execute(
        """SELECT inter_arrival_seconds FROM mqtt_observations
           WHERE device_id = 'iot-001' AND sequence = 2"""
    ).fetchone()[0] == 1.0
    assert repaired.connection.execute(
        "SELECT last_sequence FROM devices WHERE device_id = 'iot-001'"
    ).fetchone()[0] == 2
    repaired.close()

    partial_store = ObservationStore(str(partial_database))
    assert tuple(
        partial_store.connection.execute(
            """SELECT schema_version, maintenance_revision
               FROM schema_metadata"""
        ).fetchone()
    ) == (3, 1)
    partial_rows = partial_store.connection.execute(
        """SELECT mqtt_duplicate, duplicate
           FROM mqtt_observations ORDER BY id"""
    ).fetchall()
    assert [tuple(row) for row in partial_rows] == [
        (0, 0),
        (0, 1),
        (0, 0),
        (1, 0),
    ]
    assert partial_store.connection.execute(
        "SELECT last_sequence FROM devices WHERE device_id = 'iot-001'"
    ).fetchone()[0] == 2
    partial_store.close()


def test_expired_start_event_is_cleared_by_receipt_time_and_not_restored(
    tmp_path: Path,
) -> None:
    database = tmp_path / "expired-scenario.db"
    store = ObservationStore(str(database))
    stale_event = experiment("scenario_started")
    stale_event["event_at_utc"] = "2020-01-01T00:00:00Z"
    store.process(
        observed(
            f"experiments/v1/devices/{DEVICE_ID}/events",
            stale_event,
            100.0,
        )
    )
    assert DEVICE_ID in store._active_scenarios
    store.process(
        observed(
            f"iot/v1/devices/{DEVICE_ID}/telemetry",
            telemetry(1),
            time.time(),
        )
    )
    assert store._active_scenarios == {}
    row = store.connection.execute(
        "SELECT ground_truth_label, scenario_run_id FROM mqtt_observations"
    ).fetchone()
    assert tuple(row) == ("normal", None)
    store.close()

    reopened = ObservationStore(str(database))
    assert reopened._active_scenarios == {}
    reopened.close()


def test_scenario_expiry_uses_collector_receipt_time_not_device_clock(
    tmp_path: Path,
) -> None:
    store = ObservationStore(str(tmp_path / "clock-skew.db"))
    event_topic = f"experiments/v1/devices/{DEVICE_ID}/events"
    start = experiment("scenario_started")
    start["event_at_utc"] = "2099-01-01T00:00:00Z"
    received_at = time.time()
    store.process(observed(event_topic, start, received_at))
    assert store._active_scenarios[DEVICE_ID][2] == (
        received_at + 30 + ACTIVE_SCENARIO_GRACE_SECONDS
    )
    store.close()


def test_retained_status_does_not_regress_newer_device_timestamp(
    tmp_path: Path,
) -> None:
    store = ObservationStore(str(tmp_path / "retained-time.db"))
    telemetry_time = datetime.fromisoformat(
        "2026-11-08T09:00:10+00:00"
    ).timestamp()
    store.process(
        observed(
            f"iot/v1/devices/{DEVICE_ID}/telemetry",
            telemetry(1),
            telemetry_time,
        )
    )
    retained_online = status("online")
    retained_online["sent_at_utc"] = "20261108T090000Z"
    store.process(
        observed(
            f"iot/v1/devices/{DEVICE_ID}/status",
            retained_online,
            telemetry_time + 30,
            retain=True,
        )
    )
    row = store.connection.execute(
        "SELECT last_seen_at, last_status_at FROM devices"
    ).fetchone()
    assert tuple(row) == (
        "2026-11-08T09:00:10.000000Z",
        "2026-11-08T09:00:00.000000Z",
    )

    future_status = status("online")
    future_status["sent_at_utc"] = "2099-01-01T00:00:00Z"
    status_topic = f"iot/v1/devices/{DEVICE_ID}/status"
    store.process(
        observed(
            status_topic,
            future_status,
            telemetry_time + 31,
            retain=True,
        )
    )
    store.process(
        observed(
            status_topic,
            status("offline"),
            telemetry_time + 32,
        )
    )
    state = store.connection.execute(
        """SELECT last_status_at, last_status_event, is_online
           FROM devices WHERE device_id = ?""",
        (DEVICE_ID,),
    ).fetchone()
    assert tuple(state) == ("2026-11-08T09:00:42.000000Z", "offline", 0)
    with store.connection:
        store.connection.execute(
            "UPDATE devices SET last_status_event = 'online', is_online = 1"
        )
        store.connection.execute(
            "UPDATE schema_metadata SET maintenance_revision = 0"
        )
    store.close()

    rebuilt = ObservationStore(str(tmp_path / "retained-time.db"))
    rebuilt_state = rebuilt.connection.execute(
        """SELECT last_status_at, last_status_event, is_online
           FROM devices WHERE device_id = ?""",
        (DEVICE_ID,),
    ).fetchone()
    assert tuple(rebuilt_state) == (
        "2026-11-08T09:00:42.000000Z",
        "offline",
        0,
    )
    rebuilt.close()


def test_missing_end_event_expires_during_same_collector_process(
    tmp_path: Path,
) -> None:
    store = ObservationStore(str(tmp_path / "missing-end.db"))
    event_topic = f"experiments/v1/devices/{DEVICE_ID}/events"
    telemetry_topic = f"iot/v1/devices/{DEVICE_ID}/telemetry"
    start = experiment("scenario_started")
    start["duration_seconds"] = 1
    started_unix = time.time()
    store.process(observed(event_topic, start, started_unix))
    store.process(observed(telemetry_topic, telemetry(1), started_unix + 0.5))
    store.process(
        observed(
            telemetry_topic,
            telemetry(2),
            started_unix + 1 + ACTIVE_SCENARIO_GRACE_SECONDS + 0.1,
        )
    )
    rows = store.connection.execute(
        """SELECT ground_truth_label, scenario_run_id
           FROM mqtt_observations WHERE message_type = 'telemetry' ORDER BY id"""
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("message_burst", RUN_ID),
        ("normal", None),
    ]
    assert store._active_scenarios == {}
    store.close()
