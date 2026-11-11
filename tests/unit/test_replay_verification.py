from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from src.analytics.storage import AnalyticsStore
from src.features import TelemetryObservation, build_scheduled_windows
from src.ml import fit_isolation_forest, save_artifact, score_feature_rows
from src.pipeline.verification import main, verify_run_replay


ROOT = Path(__file__).resolve().parents[2]
RUN_ID = "test-normal-01"
SESSION_ID = "collection-session-01"
START = 1_800_000_000.0


def model_row(offset: float) -> dict[str, float]:
    return {
        "message_count": 4.0 + offset % 2,
        "message_rate_hz": 0.2 + (offset % 2) * 0.05,
        "payload_mean_bytes": 360.0 + offset,
        "payload_std_bytes": 1.0 + offset / 100,
        "interarrival_mean_ms": 5_000.0 + offset,
        "interarrival_std_ms": 25.0 + offset,
        "peak_messages_per_second": 1.0,
    }


def seed_results(tmp_path: Path) -> Path:
    results = tmp_path / "results"
    results.mkdir()
    shutil.copyfile(ROOT / "experiments/run_plan.csv", results / "run_plan.csv")
    run_directory = results / "runs" / RUN_ID
    run_directory.mkdir(parents=True)
    database = run_directory / "run.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE mqtt_observations (
                id INTEGER PRIMARY KEY,
                device_id TEXT NOT NULL,
                received_at_unix REAL NOT NULL,
                payload_size_bytes INTEGER NOT NULL,
                session_id TEXT,
                message_type TEXT NOT NULL,
                payload_valid INTEGER NOT NULL,
                duplicate INTEGER NOT NULL,
                retain INTEGER NOT NULL
            )
            """
        )
        row_id = 0
        for device_index in range(1, 13):
            for message_index, offset in enumerate((1.0, 6.0, 11.0, 16.0)):
                row_id += 1
                connection.execute(
                    "INSERT INTO mqtt_observations VALUES (?, ?, ?, ?, ?, 'telemetry', 1, 0, 0)",
                    (
                        row_id,
                        f"iot-{device_index:03d}",
                        START + offset,
                        360 + device_index + message_index,
                        f"device-session-{device_index:03d}",
                    ),
                )

    observations = []
    with sqlite3.connect(database) as connection:
        for row in connection.execute(
            """
            SELECT id, device_id, received_at_unix, payload_size_bytes, session_id,
                   message_type, payload_valid, duplicate, retain
            FROM mqtt_observations ORDER BY received_at_unix, id
            """
        ):
            observations.append(
                TelemetryObservation(
                    observation_id=row[0],
                    device_id=row[1],
                    received_at_unix=row[2],
                    payload_size_bytes=row[3],
                    session_id=row[4],
                    message_type=row[5],
                    payload_valid=bool(row[6]),
                    duplicate=bool(row[7]),
                    retain=bool(row[8]),
                )
            )
    windows = build_scheduled_windows(
        run_id=RUN_ID,
        collection_session_id=SESSION_ID,
        device_ids=tuple(f"iot-{index:03d}" for index in range(1, 13)),
        run_start_unix=START,
        run_end_unix=START + 20.0,
        observations=observations,
    )

    training = [model_row(float(index % 19)) for index in range(80)]
    validation = [model_row(float(index % 13) + 0.5) for index in range(30)]
    artifact = fit_isolation_forest(
        training,
        validation,
        model_version="iforest-replay-test-v1",
        train_run_ids=("train-normal-01", "train-normal-02"),
        validation_run_ids=("validation-normal-01",),
    )
    save_artifact(artifact, results / "artifacts")
    scores = score_feature_rows(
        artifact, [window.features.as_dict() for window in windows if window.features]
    )
    with AnalyticsStore(database) as store:
        store.persist_feature_windows(windows)
        store.persist_predictions(
            windows,
            scores,
            inference_call_id="online-call-01",
            predicted_at_unix=START + 21.0,
            inference_latency_ms=1.25,
        )
    (run_directory / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": RUN_ID,
                "collection_session_id": SESSION_ID,
                "status": "completed",
                "run_start_utc": "2027-01-15T08:00:00.000000Z",
                "effective_duration_seconds": 20.0,
            }
        ),
        encoding="utf-8",
    )
    return results


def test_replay_matches_every_feature_and_prediction(tmp_path: Path) -> None:
    results = seed_results(tmp_path)

    report = verify_run_replay(results, RUN_ID)

    assert report["status"] == "pass"
    assert report["counts"]["scheduled_windows"] == 12
    assert report["counts"]["persisted_prediction_rows"] == 12
    assert report["mismatch_count"] == 0
    evidence = results / report["evidence_path"]
    assert json.loads(evidence.read_text(encoding="utf-8"))["status"] == "pass"


def test_replay_reports_field_tampering_without_mutating_database(tmp_path: Path) -> None:
    results = seed_results(tmp_path)
    database = results / "runs" / RUN_ID / "run.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE feature_windows SET message_rate_hz = 999 WHERE window_index = 0"
        )
        connection.execute(
            "UPDATE window_predictions SET anomaly_score = 999 WHERE window_id LIKE '%000000'"
        )

    report = verify_run_replay(results, RUN_ID)

    assert report["status"] == "fail"
    assert report["mismatch_counts"]["feature_field_mismatches"] == 12
    assert report["mismatch_counts"]["prediction_field_mismatches"] == 12
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM window_predictions WHERE anomaly_score = 999"
        ).fetchone()[0] == 12


def test_replay_fails_on_missing_and_extra_prediction_rows(tmp_path: Path) -> None:
    results = seed_results(tmp_path)
    database = results / "runs" / RUN_ID / "run.db"
    with sqlite3.connect(database) as connection:
        original = connection.execute(
            "SELECT * FROM window_predictions ORDER BY window_id LIMIT 1"
        ).fetchone()
        columns = [row[1] for row in connection.execute("PRAGMA table_info(window_predictions)")]
        connection.execute(
            "DELETE FROM anomaly_alerts WHERE window_id = ?", (original[0],)
        )
        connection.execute("DELETE FROM window_predictions WHERE window_id = ?", (original[0],))
        values = list(original)
        values[0] = f"{RUN_ID}:iot-999:999999"
        connection.execute(
            f"INSERT INTO window_predictions ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            values,
        )

    report = verify_run_replay(results, RUN_ID)

    assert report["status"] == "fail"
    assert report["mismatch_counts"]["missing_prediction_rows"] == 1
    assert report["mismatch_counts"]["extra_prediction_rows"] == 1


def test_output_must_remain_inside_results_and_cli_reports_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    results = seed_results(tmp_path)
    with pytest.raises(ValueError, match="inside the results"):
        verify_run_replay(results, RUN_ID, output_path=tmp_path / "outside.json")

    assert main(["--results-dir", str(results), "--run-id", RUN_ID]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "pass"
