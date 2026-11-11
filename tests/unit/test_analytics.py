from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.analytics.service import AnalyticsConfig, AnalyticsService, main
from src.analytics.storage import AnalyticsStore
from src.collector.storage import ObservedMessage, ObservationStore
from src.ml import IsolationForestConfig, ModelMetadata, TrainedArtifact


RUN_ID = "test-k2-01"
COLLECTION_SESSION_ID = "53dc2ff4-4c91-45ff-bfca-55a3feea5d5f"
DEVICE_SESSION_ID = "1b54df51-7ca8-41fd-9ccd-b25bbdc83a64"
SCENARIO_EVENT_RUN_ID = "5e4a70bc-1f79-4bd9-b26e-f4d9c6fa24a8"


class FixedEstimator:
    """Deterministic test double that flags high message counts."""

    def decision_function(self, matrix: list[list[float]]) -> list[float]:
        return [-0.5 if row[0] >= 4 else 0.5 for row in matrix]


def artifact() -> TrainedArtifact:
    metadata = ModelMetadata(
        model_version="iforest-test-v1",
        trained_at_utc="2026-11-11T00:00:00.000000Z",
        feature_schema_version="1.0",
        feature_names=(
            "message_count",
            "message_rate_hz",
            "payload_mean_bytes",
            "payload_std_bytes",
            "interarrival_mean_ms",
            "interarrival_std_ms",
            "peak_messages_per_second",
        ),
        window_seconds=20,
        stride_seconds=5,
        min_messages=3,
        threshold=0.0,
        threshold_quantile=0.01,
        score_direction="lower_is_more_anomalous",
        train_run_ids=("train-normal-01",),
        validation_run_ids=("validation-normal-01",),
        training_row_count=100,
        validation_row_count=50,
        config=IsolationForestConfig(),
        library_versions={"python": "test", "scikit-learn": "test"},
    )
    return TrainedArtifact(estimator=FixedEstimator(), metadata=metadata)


def utc(value: float) -> str:
    return (
        datetime.fromtimestamp(value, tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def observed(topic: str, payload: dict[str, object], at: float) -> ObservedMessage:
    return ObservedMessage(
        topic=topic,
        payload=json.dumps(payload).encode("utf-8"),
        qos=1,
        retain=False,
        duplicate=False,
        received_at_unix=at,
    )


def telemetry(device_id: str, sequence: int, at: float) -> ObservedMessage:
    return observed(
        f"iot/v1/devices/{device_id}/telemetry",
        {
            "schema_version": "1.0",
            "device_id": device_id,
            "device_type": "environment_sensor",
            "session_id": DEVICE_SESSION_ID,
            "sequence_no": sequence,
            "sent_at_utc": utc(at),
            "measurements": {
                "temperature_c": 23.0,
                "relative_humidity_pct": 50.0,
            },
        },
        at,
    )


def scenario_event(event: str, at: float) -> ObservedMessage:
    return observed(
        "experiments/v1/devices/iot-001/events",
        {
            "schema_version": "1.0",
            "event_id": (
                "83c92a5a-1a56-4717-8cd7-9ae33f98ec00"
                if event == "scenario_started"
                else "ae7f7490-0303-4d9e-b302-b4a82d2c7876"
            ),
            "device_id": "iot-001",
            "session_id": DEVICE_SESSION_ID,
            "event_at_utc": utc(at),
            "event": event,
            "scenario": "message_burst",
            "experiment_run_id": SCENARIO_EVENT_RUN_ID,
            "request_id": "b89173ec-f066-47eb-8479-011bb81a317f",
            "seed": 401,
            "duration_seconds": 10,
            "details": {},
        },
        at,
    )


def seed_database(path: Path, start: float, *, with_phase: bool = False) -> None:
    store = ObservationStore(str(path))
    try:
        for device_id, offsets in {
            "iot-001": (1.0, 5.0, 9.0, 13.0),
            "iot-002": (2.0, 8.0, 14.0),
        }.items():
            for sequence, offset in enumerate(offsets):
                store.process(telemetry(device_id, sequence, start + offset))
        if with_phase:
            store.process(scenario_event("scenario_started", start + 5.0))
            store.process(scenario_event("scenario_ended", start + 15.0))
    finally:
        store.close()


def config(path: Path, start: float, *, scenario: str = "normal") -> AnalyticsConfig:
    return AnalyticsConfig(
        database_path=path,
        run_id=RUN_ID,
        collection_session_id=COLLECTION_SESSION_ID,
        device_ids=("iot-001", "iot-002"),
        run_start_unix=start,
        run_end_unix=start + 20.0,
        scenario=scenario,
    )


def test_schema_extends_but_does_not_change_raw_schema_version(tmp_path: Path) -> None:
    path = tmp_path / "run.db"
    seed_database(path, 1_800_000_000.0)

    with AnalyticsStore(path) as store:
        assert store.raw_schema_version() == 3

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT schema_version FROM schema_metadata"
        ).fetchone() == (3,)
        names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert {
        "feature_windows",
        "inference_calls",
        "window_predictions",
        "anomaly_alerts",
    } <= names


def test_schema_migrates_predictions_created_before_timing_contract(
    tmp_path: Path,
) -> None:
    path = tmp_path / "legacy-derived.db"
    seed_database(path, 1_800_000_000.0)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE window_predictions (
                window_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                collection_session_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                window_end_utc TEXT NOT NULL,
                predicted_at_utc TEXT NOT NULL,
                anomaly_score REAL NOT NULL,
                predicted_label TEXT NOT NULL,
                decision_threshold REAL NOT NULL,
                model_version TEXT NOT NULL,
                feature_schema_version TEXT NOT NULL,
                inference_latency_ms REAL NOT NULL,
                processing_delay_ms REAL NOT NULL
            )
            """
        )

    with AnalyticsStore(path) as store:
        columns = {
            str(row[1])
            for row in store.connection.execute(
                "PRAGMA table_info(window_predictions)"
            ).fetchall()
        }
        tables = {
            str(row[0])
            for row in store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert {"inference_call_id", "inference_status"} <= columns
    assert "inference_calls" in tables


def test_persists_all_windows_predictions_and_dashboard_alert_contract(
    tmp_path: Path,
) -> None:
    start = 1_800_000_000.0
    path = tmp_path / "run.db"
    seed_database(path, start)

    service = AnalyticsService(config(path, start), artifact=artifact())
    try:
        summary = service.process_until(start + 20.0)
    finally:
        service.close()

    assert summary.scheduled_windows == 2
    assert summary.scoreable_windows == 2
    assert summary.new_predictions == 2
    assert summary.feature_windows == 2
    assert summary.window_predictions == 2
    assert summary.inference_calls == 1
    assert summary.anomaly_alerts == 1

    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        predictions = connection.execute(
            """
            SELECT predicted_label, inference_call_id, inference_status
            FROM window_predictions ORDER BY device_id
            """
        ).fetchall()
        assert [row[0] for row in predictions] == ["anomaly", "normal"]
        assert len({row[1] for row in predictions}) == 1
        assert {row[2] for row in predictions} == {"scored"}
        call = connection.execute(
            """
            SELECT inference_call_id, run_id, model_version, window_ids_json,
                   batch_size, inference_duration_ms, measured_at_utc
            FROM inference_calls
            """
        ).fetchone()
        alert = connection.execute(
            """
            SELECT detected_at_utc, device_id, anomaly_score,
                   decision_threshold, model_version, window_id, reason
            FROM anomaly_alerts
            """
        ).fetchone()
    assert alert is not None
    assert call is not None
    assert call["inference_call_id"] == predictions[0]["inference_call_id"]
    assert call["run_id"] == RUN_ID
    assert call["model_version"] == "iforest-test-v1"
    assert json.loads(call["window_ids_json"]) == [
        f"{RUN_ID}:iot-001:000000",
        f"{RUN_ID}:iot-002:000000",
    ]
    assert call["batch_size"] == 2
    assert call["inference_duration_ms"] >= 0
    assert alert["device_id"] == "iot-001"
    assert alert["reason"] == "score_below_frozen_threshold"
    assert alert["window_id"] == f"{RUN_ID}:iot-001:000000"


def test_reprocessing_is_idempotent_and_does_not_duplicate_alerts(tmp_path: Path) -> None:
    start = 1_800_000_000.0
    path = tmp_path / "run.db"
    seed_database(path, start)
    service = AnalyticsService(config(path, start), artifact=artifact())
    try:
        first = service.process_until(start + 20.0)
        changes_after_first_pass = service.store.connection.total_changes
        second = service.process_until(start + 20.0)
        changes_after_second_pass = service.store.connection.total_changes
    finally:
        service.close()

    assert first.new_predictions == 2
    assert second.new_predictions == 0
    assert second.feature_windows == 2
    assert second.window_predictions == 2
    assert second.inference_calls == 1
    assert second.anomaly_alerts == 1
    assert changes_after_second_pass == changes_after_first_pass


def test_scenario_truth_uses_actual_start_and_end_events(tmp_path: Path) -> None:
    start = 1_800_000_000.0
    path = tmp_path / "run.db"
    seed_database(path, start, with_phase=True)
    service = AnalyticsService(config(path, start, scenario="K2"))
    try:
        summary = service.process_until(start + 20.0)
    finally:
        service.close()

    assert summary.phase_log_complete is True
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            """
            SELECT device_id, ground_truth_label, phase_id, is_target_device
            FROM feature_windows ORDER BY device_id
            """
        ).fetchall()
    assert rows == [
        ("iot-001", "transition", SCENARIO_EVENT_RUN_ID, 1),
        ("iot-002", "normal", SCENARIO_EVENT_RUN_ID, 0),
    ]


def test_phase_end_ahead_of_safe_cutoff_waits_then_relabels(tmp_path: Path) -> None:
    start = 1_800_000_000.0
    path = tmp_path / "run.db"
    seed_database(path, start)
    store = ObservationStore(str(path))
    try:
        store.process(scenario_event("scenario_started", start + 5.0))
        store.process(scenario_event("scenario_ended", start + 25.0))
    finally:
        store.close()
    service = AnalyticsService(
        AnalyticsConfig(
            database_path=path,
            run_id=RUN_ID,
            collection_session_id=COLLECTION_SESSION_ID,
            device_ids=("iot-001", "iot-002"),
            run_start_unix=start,
            run_end_unix=start + 40.0,
            scenario="K2",
        )
    )
    try:
        before_watermark = service.process_until(start + 24.0)
        after_watermark = service.process_until(start + 30.0)
    finally:
        service.close()

    assert before_watermark.phase_log_complete is False
    assert after_watermark.phase_log_complete is True
    with sqlite3.connect(path) as connection:
        labels = connection.execute(
            """
            SELECT DISTINCT ground_truth_label
            FROM feature_windows
            ORDER BY ground_truth_label
            """
        ).fetchall()
    assert labels == [("anomaly",), ("normal",), ("transition",)]


def test_incomplete_scenario_log_marks_truth_unknown(tmp_path: Path) -> None:
    start = 1_800_000_000.0
    path = tmp_path / "run.db"
    seed_database(path, start)
    service = AnalyticsService(config(path, start, scenario="K2"))
    try:
        summary = service.process_until(start + 20.0)
    finally:
        service.close()

    assert summary.phase_log_complete is False
    with sqlite3.connect(path) as connection:
        labels = connection.execute(
            "SELECT DISTINCT ground_truth_label FROM feature_windows"
        ).fetchall()
    assert labels == [("unknown",)]


def test_cli_once_writes_ready_marker_and_json_summary(
    tmp_path: Path, capsys: object
) -> None:
    start = 1_800_000_000.0
    path = tmp_path / "run.db"
    ready = tmp_path / "analytics-ready.json"
    seed_database(path, start)
    result = main(
        [
            "--database-path",
            str(path),
            "--run-id",
            RUN_ID,
            "--collection-session-id",
            COLLECTION_SESSION_ID,
            "--run-start-unix",
            str(start),
            "--run-end-unix",
            str(start + 20.0),
            "--device-ids",
            "iot-001,iot-002",
            "--ready-file",
            str(ready),
            "--once",
        ]
    )

    assert result == 0
    assert json.loads(ready.read_text(encoding="utf-8"))["ready"] is True
    output = json.loads(capsys.readouterr().out)  # type: ignore[attr-defined]
    assert output["feature_windows"] == 2
    assert output["window_predictions"] == 0
