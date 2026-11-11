from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.analytics.service import AnalyticsConfig, AnalyticsService
from src.collector.runtime_state import (
    CollectorRuntimeState,
    load_runtime_state,
    runtime_state_path,
)
from src.collector.storage import ObservedMessage, ObservationStore


DEVICE_ID = "iot-001"
SESSION_ID = "83dd2c12-a9c3-43fd-a39f-3e3657f14a71"
TOPIC = f"iot/v1/devices/{DEVICE_ID}/telemetry"


def _utc(value: float) -> str:
    return (
        datetime.fromtimestamp(value, tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _telemetry(sequence: int, at: float) -> ObservedMessage:
    payload = {
        "schema_version": "1.0",
        "device_id": DEVICE_ID,
        "device_type": "environment_sensor",
        "session_id": SESSION_ID,
        "sequence_no": sequence,
        "sent_at_utc": _utc(at),
        "measurements": {
            "temperature_c": 23.0,
            "relative_humidity_pct": 50.0,
        },
    }
    return ObservedMessage(
        topic=TOPIC,
        payload=json.dumps(payload).encode("utf-8"),
        qos=1,
        retain=False,
        duplicate=False,
        received_at_unix=at,
    )


def _seed(path: Path, start: float) -> None:
    store = ObservationStore(str(path))
    try:
        for sequence, offset in enumerate((1.0, 6.0, 11.0), start=1):
            store.process(_telemetry(sequence, start + offset))
    finally:
        store.close()


def _config(path: Path, start: float) -> AnalyticsConfig:
    return AnalyticsConfig(
        database_path=path,
        run_id="functional-t09",
        collection_session_id="collection-t09",
        device_ids=(DEVICE_ID,),
        run_start_unix=start,
        run_end_unix=start + 20.0,
    )


def test_pending_pre_boundary_message_blocks_window_until_persisted(
    tmp_path: Path,
) -> None:
    start = 1_800_000_000.0
    database = tmp_path / "run.db"
    _seed(database, start)
    state = CollectorRuntimeState(runtime_state_path(database))
    pending = _telemetry(4, start + 19.0)
    state.reserve(received_at_unix=pending.received_at_unix, topic=pending.topic)

    service = AnalyticsService(_config(database, start))
    try:
        early = service.process_until(start + 20.0)
        assert early.cutoff_unix == start + 19.0
        assert early.scheduled_windows == 0
        assert early.feature_windows == 0
        assert early.collector_queue_depth == 1

        writer = ObservationStore(str(database))
        try:
            writer.process(pending)
        finally:
            writer.close()
        state.persisted(
            received_at_unix=pending.received_at_unix, topic=pending.topic
        )

        complete = service.process_until(start + 20.0)
        assert complete.scheduled_windows == 1
        assert complete.feature_windows == 1
        assert complete.collector_queue_depth == 0
        row = service.store.connection.execute(
            "SELECT observed_message_count, message_count FROM feature_windows"
        ).fetchone()
        assert tuple(row) == (4, 4)
    finally:
        service.close()


def test_dropped_telemetry_marks_intersecting_window_as_observation_gap(
    tmp_path: Path,
) -> None:
    start = 1_800_000_000.0
    database = tmp_path / "run.db"
    _seed(database, start)
    state = CollectorRuntimeState(runtime_state_path(database))
    lost_at = start + 12.0
    state.reserve(received_at_unix=lost_at, topic=TOPIC)
    state.dropped(
        received_at_unix=lost_at,
        topic=TOPIC,
        reason="queue_full",
    )

    service = AnalyticsService(_config(database, start))
    try:
        summary = service.process_until(start + 20.0)
        row = service.store.connection.execute(
            """
            SELECT feature_status, exclusion_reason, message_count
            FROM feature_windows
            """
        ).fetchone()
    finally:
        service.close()

    assert summary.collector_dropped_messages == 1
    assert tuple(row) == ("observation_gap", "collector_queue_full", None)


def test_runtime_state_is_atomic_and_stopped_state_remains_auditable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "collector-state.json"
    state = CollectorRuntimeState(path)
    state.reserve(received_at_unix=100.0, topic=TOPIC)
    pending = load_runtime_state(path)
    assert pending is not None
    assert pending.queue_depth == 1
    assert pending.oldest_pending_received_at_unix == 100.0

    state.persisted(received_at_unix=100.0, topic=TOPIC)
    state.stop()
    stopped = load_runtime_state(path)
    assert stopped is not None
    assert stopped.queue_depth == 0
    assert stopped.last_persisted_received_at_unix == 100.0
    assert stopped.accepting_messages is False
    assert not list(tmp_path.glob("*.tmp"))
