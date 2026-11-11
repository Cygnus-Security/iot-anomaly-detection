from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from pathlib import Path

from src.ml import load_artifact
from src.experiments.planning import load_run_plan
from src.pipeline.evaluation import (
    ActualEvent,
    _event_result,
    _metrics,
    _primary_exclusion,
    evaluate_campaign,
)
from src.pipeline.exporting import export_run
from src.pipeline.training import train_campaign_model


ROOT = Path(__file__).resolve().parents[2]


def copy_plan(destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / "experiments" / "run_plan.csv", destination / "run_plan.csv")
    shutil.copyfile(
        ROOT / "experiments" / "split_manifest.json",
        destination / "split_manifest.json",
    )


def feature_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE feature_windows (
            window_id TEXT PRIMARY KEY,
            run_id TEXT,
            collection_session_id TEXT,
            device_id TEXT,
            window_index INTEGER,
            window_start_utc TEXT,
            window_end_utc TEXT,
            feature_schema_version TEXT,
            feature_status TEXT,
            ground_truth_label TEXT,
            message_count INTEGER,
            message_rate_hz REAL,
            payload_mean_bytes REAL,
            payload_std_bytes REAL,
            interarrival_mean_ms REAL,
            interarrival_std_ms REAL,
            peak_messages_per_second INTEGER
        )
        """
    )


def seed_training_database(path: Path, run_id: str, offset: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        feature_schema(connection)
        for index in range(684):
            connection.execute(
                """
                INSERT INTO feature_windows VALUES (
                    ?, ?, 'collection-session', 'iot-001', ?,
                    '2026-11-11T00:00:00.000000Z',
                    '2026-11-11T00:00:20.000000Z', '1.0', 'ok', 'normal',
                    ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    f"{run_id}:iot-001:{index:06d}",
                    run_id,
                    index,
                    4 + index % 2,
                    0.2 + (index % 2) * 0.05,
                    370.0 + offset + index / 100,
                    2.0 + index / 100,
                    5000.0 + index,
                    100.0 + index / 10,
                    1 + index % 2,
                ),
            )
    (path.parent / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "collection_session_id": "collection-session",
                "status": "completed",
            }
        ),
        encoding="utf-8",
    )


def test_training_freezes_model_threshold_and_provenance(tmp_path: Path) -> None:
    results = tmp_path / "results"
    copy_plan(results)
    for offset, run_id in enumerate(
        ("train-normal-01", "train-normal-02", "validation-normal-01")
    ):
        seed_training_database(
            results / "runs" / run_id / "run.db", run_id, float(offset)
        )

    summary = train_campaign_model(results, model_version="iforest-test-v1")

    assert summary["training_row_count"] == 1368
    assert summary["validation_row_count"] == 684
    assert summary["train_run_ids"] == ["train-normal-01", "train-normal-02"]
    artifact = load_artifact(results / "artifacts", trusted=True)
    assert artifact.metadata.model_version == "iforest-test-v1"
    assert artifact.metadata.threshold == summary["threshold"]
    assert set(summary["files"]) == {
        "model.joblib",
        "model_metadata.json",
        "split_manifest.json",
        "training_features.csv",
        "validation_features.csv",
        "training_data_quality.csv",
    }
    assert summary["reload_score_consistency"] is True


def seed_export_database(path: Path, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE mqtt_observations (
                id INTEGER, received_at_unix REAL, payload_size_bytes INTEGER,
                payload_text TEXT
            );
            INSERT INTO mqtt_observations VALUES (1, 1800000001.0, 123, '{}');
            CREATE TABLE feature_windows (
                window_id TEXT, run_id TEXT, device_id TEXT, window_index INTEGER,
                feature_status TEXT
            );
            CREATE TABLE window_predictions (
                window_id TEXT, run_id TEXT, collection_session_id TEXT,
                device_id TEXT, window_end_utc TEXT, predicted_at_utc TEXT,
                inference_call_id TEXT, anomaly_score REAL,
                predicted_label TEXT, inference_status TEXT,
                decision_threshold REAL, model_version TEXT,
                feature_schema_version TEXT, inference_latency_ms REAL,
                processing_delay_ms REAL
            );
            CREATE TABLE anomaly_alerts (
                id INTEGER, window_id TEXT, run_id TEXT,
                collection_session_id TEXT, detected_at_utc TEXT,
                device_id TEXT, anomaly_score REAL, decision_threshold REAL,
                model_version TEXT, reason TEXT
            );
            CREATE TABLE inference_calls (
                run_id TEXT, model_version TEXT, inference_call_id TEXT,
                window_ids_json TEXT, batch_size INTEGER,
                inference_duration_ms REAL, measured_at_utc TEXT
            );
            CREATE TABLE scenario_events (
                id INTEGER, scenario_run_id TEXT, device_id TEXT,
                scenario TEXT, event_type TEXT, event_at TEXT
            );
            """
        )
        window_id = f"{run_id}:iot-001:000000"
        connection.execute(
            "INSERT INTO feature_windows VALUES (?, ?, 'iot-001', 0, 'ok')",
            (window_id, run_id),
        )
        connection.execute(
            """INSERT INTO window_predictions VALUES (
                ?, ?, 'collection-session', 'iot-001',
                '2026-11-11T00:00:20.000000Z',
                '2026-11-11T00:00:21.000000Z', 'call-1', -0.2,
                'anomaly', 'scored', -0.1, 'model-v1', '1.0', 2.5, 1000.0
            )""",
            (window_id, run_id),
        )
        connection.execute(
            """INSERT INTO anomaly_alerts VALUES (
                1, ?, ?, 'collection-session',
                '2026-11-11T00:00:21.000000Z', 'iot-001', -0.2, -0.1,
                'model-v1', 'score_below_frozen_threshold'
            )""",
            (window_id, run_id),
        )
        connection.execute(
            """INSERT INTO inference_calls VALUES (
                ?, 'model-v1', 'call-1', ?, 1, 2.5,
                '2026-11-11T00:00:21.000000Z'
            )""",
            (run_id, json.dumps([window_id])),
        )


def test_export_preserves_standard_prediction_and_unique_call_timing(
    tmp_path: Path,
) -> None:
    results = tmp_path / "results"
    copy_plan(results)
    run_id = "test-normal-01"
    run_directory = results / "runs" / run_id
    seed_export_database(run_directory / "run.db", run_id)
    (run_directory / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "status": "completed",
                "run_start_utc": "2026-11-11T00:00:00.000000Z",
                "run_end_utc": "2026-11-11T00:05:00.000000Z",
            }
        ),
        encoding="utf-8",
    )

    counts = export_run(results, run_id)

    assert counts["predictions"] == 1
    assert counts["timing_samples"] == 1
    with (run_directory / "predictions.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        prediction = next(csv.DictReader(handle))
    assert prediction["inference_call_id"] == "call-1"
    assert prediction["score"] == "-0.2"
    assert prediction["threshold"] == "-0.1"
    with (run_directory / "timing_samples.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        timing = next(csv.DictReader(handle))
    assert timing["batch_size"] == "1"
    assert timing["inference_duration_ms"] == "2.5"


def metric_row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "included_in_primary_metrics": 1,
        "ground_truth_label": "normal",
        "predicted_label": "normal",
        "feature_status": "ok",
        "inference_status": "scored",
        "window_processing_delay_ms": 10.0,
        "inference_duration_ms": 2.0,
        "inference_call_id": "shared-call",
        "window_id": "window-1",
    }
    row.update(changes)
    return row


def test_metrics_keep_na_semantics_and_count_batch_timing_once() -> None:
    result = _metrics(
        [metric_row(), metric_row(window_id="window-2")],
        scope="scenario",
        scenario_code="K1",
        scenario="normal",
    )

    assert result["tn"] == 2
    assert result["recall"] is None
    assert result["recall_reason"] == "no_actual_anomaly_windows"
    assert result["f1"] is None
    assert result["inference_call_count"] == 1
    assert result["inference_duration_median_ms"] == 2.0


def test_exclusion_priority_is_deterministic() -> None:
    assert _primary_exclusion("missing_record", "missing_prediction", "unknown") == "missing_feature_record"
    assert _primary_exclusion("observation_gap", "missing_prediction", "unknown") == "observation_gap"
    assert _primary_exclusion("insufficient_data", "missing_prediction", "unknown") == "insufficient_data"
    assert _primary_exclusion("ok", "missing_prediction", "unknown") == "inference_not_scored"
    assert _primary_exclusion("ok", "scored", "unknown") == "unknown_ground_truth"
    assert _primary_exclusion("ok", "scored", "transition") == "transition_window"
    assert _primary_exclusion("ok", "scored", "normal") == ""


def event_prediction(recorded_at: str) -> dict[str, object]:
    return {
        "run_id": "test-k2-01",
        "device_id": "iot-001",
        "window_start_utc": "2026-11-11T00:00:50.000000Z",
        "window_end_utc": "2026-11-11T00:01:10.000000Z",
        "prediction_recorded_at_utc": recorded_at,
        "predicted_label": "anomaly",
        "inference_status": "scored",
        "feature_status": "ok",
    }


def test_transition_window_detects_event_but_post_deadline_result_does_not() -> None:
    entry = next(
        item
        for item in load_run_plan(ROOT / "experiments" / "run_plan.csv")
        if item.run_id == "test-k2-01"
    )
    event = ActualEvent(
        run_id=entry.run_id,
        phase_id="scenario-run-1",
        device_id="iot-001",
        start_utc="2026-11-11T00:01:00.000000Z",
        end_utc="2026-11-11T00:02:00.000000Z",
    )

    detected = _event_result(
        entry, event, [event_prediction("2026-11-11T00:01:12.000000Z")]
    )
    late = _event_result(
        entry, event, [event_prediction("2026-11-11T00:02:21.000000Z")]
    )

    assert detected is not None and detected["status"] == "detected"
    assert detected["latency_seconds"] == 12.0
    assert late is not None and late["status"] == "not_detected"
    assert late["latency_seconds"] is None


def test_evaluator_retains_every_missing_scheduled_window(tmp_path: Path) -> None:
    results = tmp_path / "results"
    copy_plan(results)
    with (results / "run_plan.csv").open(encoding="utf-8", newline="") as handle:
        run_ids = [row["run_id"] for row in csv.DictReader(handle)]
    for run_id in run_ids:
        run_directory = results / "runs" / run_id
        run_directory.mkdir(parents=True)
        with sqlite3.connect(run_directory / "run.db") as connection:
            connection.execute("CREATE TABLE feature_windows (window_id TEXT)")
        (run_directory / "run_manifest.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "collection_session_id": f"session-{run_id}",
                    "status": "completed",
                    "run_start_utc": "2026-11-11T00:00:00.000000Z",
                    "run_end_utc": "2026-11-11T00:05:00.000000Z",
                }
            ),
            encoding="utf-8",
        )

    summary = evaluate_campaign(results)

    assert summary["scheduled_windows"] == 6300
    assert summary["test_scheduled_windows"] == 4248
    assert summary["test_evaluated_windows"] == 0
    assert summary["data_quality_run_count"] == 13
    with (results / "evaluation_windows.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 6300
    assert {row["exclusion_reason"] for row in rows} == {"missing_feature_record"}
    with (results / "data_quality_by_run.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        quality = list(csv.DictReader(handle))
    assert len(quality) == 13
    assert sum(int(row["missing_feature_record"]) for row in quality) == 6300
