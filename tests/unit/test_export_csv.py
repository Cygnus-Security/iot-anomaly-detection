from __future__ import annotations

import csv
import json
import sqlite3
import time
from pathlib import Path

from src.collector.export_csv import TABLES, export_table
from src.collector.storage import ObservedMessage, ObservationStore
from src.common.messages import utc_now

DEVICE_ID = "iot-001"
SESSION_ID = "29ab4b25-c9ad-4f11-86b3-dabfb06a02cc"


def observation(topic: str, payload: dict[str, object]) -> ObservedMessage:
    return ObservedMessage(
        topic=topic,
        payload=json.dumps(payload).encode(),
        qos=1,
        retain=False,
        duplicate=False,
        received_at_unix=time.time(),
    )


def test_phase2_tables_export_with_headers_and_matching_row_counts(
    tmp_path: Path,
) -> None:
    database = tmp_path / "export.db"
    store = ObservationStore(str(database))
    store.process(
        observation(
            f"iot/v1/devices/{DEVICE_ID}/status",
            {
                "schema_version": "1.0",
                "device_id": DEVICE_ID,
                "session_id": SESSION_ID,
                "state": "online",
                "sent_at_utc": utc_now(),
            },
        )
    )
    store.process(
        observation(
            f"experiments/v1/devices/{DEVICE_ID}/events",
            {
                "schema_version": "1.0",
                "event_id": "cb8df2e0-3488-4c44-91f8-239b8016675b",
                "device_id": DEVICE_ID,
                "session_id": SESSION_ID,
                "event_at_utc": utc_now(),
                "event": "scenario_started",
                "scenario": "normal",
                "experiment_run_id": "71fdc845-3be7-4711-a673-46894844274a",
                "request_id": "c410f117-93c7-40ca-9147-e57fa4088e83",
                "seed": 42,
                "duration_seconds": 30,
                "details": {},
            },
        )
    )
    store.close()

    connection = sqlite3.connect(database)
    try:
        expected = {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in TABLES}
        for table in TABLES:
            output = tmp_path / f"{table}.csv"
            assert export_table(connection, table, output) == expected[table]
            with output.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.reader(handle))
            assert rows[0]
            assert len(rows) - 1 == expected[table]
    finally:
        connection.close()
