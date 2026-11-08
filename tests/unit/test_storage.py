from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from src.collector.storage import ObservedMessage, ObservationStore


def observed(topic: str, payload: dict[str, object] | bytes, at: float) -> ObservedMessage:
    encoded = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return ObservedMessage(
        topic=topic,
        payload=encoded,
        qos=1,
        retain=False,
        duplicate=False,
        received_at_unix=at,
    )


def status(event: str, *, run_id: str = "run-1") -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "1.0",
        "message_id": f"message-{event}",
        "device_id": "device-001",
        "timestamp": "2026-11-08T09:00:00Z",
        "event": event,
        "details": {},
    }
    if event.startswith("scenario_"):
        payload.update(
            {
                "scenario": "message_burst",
                "scenario_run_id": run_id,
                "request_id": "request-1",
                "seed": 42,
                "duration_seconds": 30,
            }
        )
    return payload


def telemetry(sequence: int) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "message_id": f"telemetry-{sequence}",
        "device_id": "device-001",
        "timestamp": "2026-11-08T09:00:01Z",
        "sequence": sequence,
        "sensor": {"temperature_c": 23.0, "humidity_percent": 50.0},
        "device": {"firmware": "test", "uptime_seconds": 1.0},
    }


def test_store_labels_scenario_and_calculates_inter_arrival(tmp_path: Path) -> None:
    database = tmp_path / "observations.db"
    store = ObservationStore(str(database))
    store.process(observed("iot/device-001/status", status("scenario_started"), 100.0))
    store.process(observed("iot/device-001/telemetry", telemetry(1), 101.0))
    store.process(observed("iot/device-001/telemetry", telemetry(2), 101.25))
    store.process(observed("iot/device-001/status", status("scenario_ended"), 102.0))
    store.process(observed("iot/device-001/telemetry", telemetry(3), 103.0))
    store.close()

    connection = sqlite3.connect(database)
    rows = connection.execute(
        """SELECT ground_truth_label, scenario_run_id, inter_arrival_seconds
           FROM mqtt_observations WHERE message_type = 'telemetry' ORDER BY id"""
    ).fetchall()
    assert rows == [
        ("message_burst", "run-1", None),
        ("message_burst", "run-1", 0.25),
        ("normal", None, 1.75),
    ]
    assert connection.execute("SELECT COUNT(*) FROM scenario_events").fetchone()[0] == 2
    assert connection.execute("SELECT last_sequence FROM devices").fetchone()[0] == 3
    connection.close()


def test_store_preserves_invalid_payload_with_validation_error(tmp_path: Path) -> None:
    database = tmp_path / "invalid.db"
    store = ObservationStore(str(database))
    store.process(observed("iot/device-002/telemetry", b"not-json", 100.0))
    row = store.connection.execute(
        """SELECT payload_valid, validation_error, payload_text
           FROM mqtt_observations"""
    ).fetchone()
    assert tuple(row) == (0, "payload is not valid JSON: Expecting value", "not-json")
    store.close()
