"""SQLite contract for derived feature windows, predictions, and alerts.

These tables extend a per-run collector database without changing the raw
collector schema version. Raw observations remain the reproducible source of
truth; every derived record carries both the experiment run ID and the
collection session ID.
"""

from __future__ import annotations

import sqlite3
import time
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from src.features import FeatureWindow, TelemetryObservation
from src.ml import ScoreResult


def utc_from_unix(value: float) -> str:
    """Return a canonical, microsecond-resolution UTC timestamp."""

    return (
        datetime.fromtimestamp(value, tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


class AnalyticsStore:
    """Single-process writer for derived analytics records."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(
            str(self.database_path), timeout=30.0, check_same_thread=False
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self._create_schema()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "AnalyticsStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS feature_windows (
                window_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                collection_session_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                window_index INTEGER NOT NULL,
                window_start_utc TEXT NOT NULL,
                window_end_utc TEXT NOT NULL,
                window_start_unix REAL NOT NULL,
                window_end_unix REAL NOT NULL,
                feature_schema_version TEXT NOT NULL,
                observed_message_count INTEGER NOT NULL,
                feature_status TEXT NOT NULL CHECK(feature_status IN (
                    'ok', 'insufficient_data', 'observation_gap', 'invalid_feature'
                )),
                exclusion_reason TEXT,
                message_count INTEGER,
                message_rate_hz REAL,
                payload_mean_bytes REAL,
                payload_std_bytes REAL,
                interarrival_mean_ms REAL,
                interarrival_std_ms REAL,
                peak_messages_per_second INTEGER,
                ground_truth_label TEXT NOT NULL CHECK(ground_truth_label IN (
                    'normal', 'anomaly', 'transition', 'unknown'
                )),
                phase_id TEXT,
                is_target_device INTEGER NOT NULL,
                updated_at_utc TEXT NOT NULL,
                UNIQUE(run_id, collection_session_id, device_id, window_index)
            );

            CREATE INDEX IF NOT EXISTS idx_feature_windows_run_device_time
                ON feature_windows(run_id, collection_session_id, device_id,
                                   window_start_unix);
            CREATE INDEX IF NOT EXISTS idx_feature_windows_truth
                ON feature_windows(run_id, ground_truth_label);

            CREATE TABLE IF NOT EXISTS inference_calls (
                inference_call_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                collection_session_id TEXT NOT NULL,
                model_version TEXT NOT NULL,
                window_ids_json TEXT NOT NULL,
                batch_size INTEGER NOT NULL CHECK(batch_size > 0),
                inference_duration_ms REAL NOT NULL
                    CHECK(inference_duration_ms >= 0),
                measured_at_utc TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_inference_calls_run_measured
                ON inference_calls(run_id, measured_at_utc);

            CREATE TABLE IF NOT EXISTS window_predictions (
                window_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                collection_session_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                window_end_utc TEXT NOT NULL,
                predicted_at_utc TEXT NOT NULL,
                anomaly_score REAL NOT NULL,
                predicted_label TEXT NOT NULL CHECK(predicted_label IN (
                    'normal', 'anomaly'
                )),
                decision_threshold REAL NOT NULL,
                model_version TEXT NOT NULL,
                feature_schema_version TEXT NOT NULL,
                inference_call_id TEXT,
                inference_status TEXT NOT NULL DEFAULT 'scored'
                    CHECK(inference_status IN ('scored')),
                inference_latency_ms REAL NOT NULL,
                processing_delay_ms REAL NOT NULL,
                FOREIGN KEY(window_id) REFERENCES feature_windows(window_id)
                    ON DELETE CASCADE,
                FOREIGN KEY(inference_call_id)
                    REFERENCES inference_calls(inference_call_id)
            );

            CREATE INDEX IF NOT EXISTS idx_window_predictions_run_label
                ON window_predictions(run_id, predicted_label, predicted_at_utc);

            CREATE TABLE IF NOT EXISTS anomaly_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_id TEXT NOT NULL UNIQUE,
                run_id TEXT NOT NULL,
                collection_session_id TEXT NOT NULL,
                detected_at_utc TEXT NOT NULL,
                device_id TEXT NOT NULL,
                anomaly_score REAL NOT NULL,
                decision_threshold REAL NOT NULL,
                model_version TEXT NOT NULL,
                reason TEXT NOT NULL,
                FOREIGN KEY(window_id) REFERENCES window_predictions(window_id)
                    ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_anomaly_alerts_run_detected
                ON anomaly_alerts(run_id, detected_at_utc);
            """
        )
        prediction_columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(window_predictions)"
            ).fetchall()
        }
        if "inference_call_id" not in prediction_columns:
            self.connection.execute(
                "ALTER TABLE window_predictions ADD COLUMN inference_call_id TEXT"
            )
        if "inference_status" not in prediction_columns:
            self.connection.execute(
                """
                ALTER TABLE window_predictions
                ADD COLUMN inference_status TEXT NOT NULL DEFAULT 'scored'
                """
            )
        self.connection.commit()

    def raw_schema_version(self) -> int | None:
        """Return the collector schema version, if the collector initialized it."""

        try:
            row = self.connection.execute(
                "SELECT schema_version FROM schema_metadata LIMIT 1"
            ).fetchone()
        except sqlite3.OperationalError:
            return None
        return None if row is None else int(row[0])

    def load_observations(
        self, *, run_start_unix: float, run_end_unix: float
    ) -> list[TelemetryObservation]:
        """Read canonical-input fields from the immutable raw table."""

        rows = self.connection.execute(
            """
            SELECT id, device_id, received_at_unix, payload_size_bytes,
                   session_id, message_type, payload_valid, duplicate, retain
            FROM mqtt_observations
            WHERE received_at_unix >= ? AND received_at_unix < ?
            ORDER BY received_at_unix, id
            """,
            (run_start_unix, run_end_unix),
        ).fetchall()
        return [
            TelemetryObservation(
                observation_id=int(row["id"]),
                device_id=str(row["device_id"]),
                received_at_unix=float(row["received_at_unix"]),
                payload_size_bytes=int(row["payload_size_bytes"]),
                session_id=(
                    None if row["session_id"] is None else str(row["session_id"])
                ),
                message_type=str(row["message_type"]),
                payload_valid=bool(row["payload_valid"]),
                duplicate=bool(row["duplicate"]),
                retain=bool(row["retain"]),
            )
            for row in rows
        ]

    def scenario_events(self) -> list[sqlite3.Row]:
        """Return actual scenario events from this run-isolated database."""

        return list(
            self.connection.execute(
                """
                SELECT scenario_run_id, device_id, scenario, event_type,
                       event_at, received_at, duration_seconds
                FROM scenario_events
                ORDER BY event_at, id
                """,
            ).fetchall()
        )

    def persist_feature_windows(self, windows: Iterable[FeatureWindow]) -> int:
        """Upsert scheduled windows; this permits final ground-truth reconciliation."""

        now = utc_from_unix(time.time())
        records: list[tuple[object, ...]] = []
        for window in windows:
            record = window.as_record()
            records.append(
                (
                    record["window_id"],
                    record["run_id"],
                    record["collection_session_id"],
                    record["device_id"],
                    record["window_index"],
                    utc_from_unix(float(record["window_start_unix"])),
                    utc_from_unix(float(record["window_end_unix"])),
                    record["window_start_unix"],
                    record["window_end_unix"],
                    record["feature_schema_version"],
                    record["observed_message_count"],
                    record["feature_status"],
                    record["exclusion_reason"],
                    record["message_count"],
                    record["message_rate_hz"],
                    record["payload_mean_bytes"],
                    record["payload_std_bytes"],
                    record["interarrival_mean_ms"],
                    record["interarrival_std_ms"],
                    record["peak_messages_per_second"],
                    record["ground_truth_label"],
                    record["phase_id"],
                    int(bool(record["is_target_device"])),
                    now,
                )
            )
        if not records:
            return 0
        with self.connection:
            self.connection.executemany(
                """
                INSERT INTO feature_windows (
                    window_id, run_id, collection_session_id, device_id,
                    window_index, window_start_utc, window_end_utc,
                    window_start_unix, window_end_unix, feature_schema_version,
                    observed_message_count, feature_status, exclusion_reason,
                    message_count, message_rate_hz, payload_mean_bytes,
                    payload_std_bytes, interarrival_mean_ms,
                    interarrival_std_ms, peak_messages_per_second,
                    ground_truth_label, phase_id, is_target_device, updated_at_utc
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                ON CONFLICT(window_id) DO UPDATE SET
                    observed_message_count = excluded.observed_message_count,
                    feature_status = excluded.feature_status,
                    exclusion_reason = excluded.exclusion_reason,
                    message_count = excluded.message_count,
                    message_rate_hz = excluded.message_rate_hz,
                    payload_mean_bytes = excluded.payload_mean_bytes,
                    payload_std_bytes = excluded.payload_std_bytes,
                    interarrival_mean_ms = excluded.interarrival_mean_ms,
                    interarrival_std_ms = excluded.interarrival_std_ms,
                    peak_messages_per_second = excluded.peak_messages_per_second,
                    ground_truth_label = excluded.ground_truth_label,
                    phase_id = excluded.phase_id,
                    is_target_device = excluded.is_target_device,
                    updated_at_utc = excluded.updated_at_utc
                WHERE feature_windows.observed_message_count
                          IS NOT excluded.observed_message_count
                   OR feature_windows.feature_status
                          IS NOT excluded.feature_status
                   OR feature_windows.exclusion_reason
                          IS NOT excluded.exclusion_reason
                   OR feature_windows.message_count
                          IS NOT excluded.message_count
                   OR feature_windows.message_rate_hz
                          IS NOT excluded.message_rate_hz
                   OR feature_windows.payload_mean_bytes
                          IS NOT excluded.payload_mean_bytes
                   OR feature_windows.payload_std_bytes
                          IS NOT excluded.payload_std_bytes
                   OR feature_windows.interarrival_mean_ms
                          IS NOT excluded.interarrival_mean_ms
                   OR feature_windows.interarrival_std_ms
                          IS NOT excluded.interarrival_std_ms
                   OR feature_windows.peak_messages_per_second
                          IS NOT excluded.peak_messages_per_second
                   OR feature_windows.ground_truth_label
                          IS NOT excluded.ground_truth_label
                   OR feature_windows.phase_id
                          IS NOT excluded.phase_id
                   OR feature_windows.is_target_device
                          IS NOT excluded.is_target_device
                """,
                records,
            )
        return len(records)

    def existing_prediction_ids(self, window_ids: Sequence[str]) -> set[str]:
        if not window_ids:
            return set()
        existing: set[str] = set()
        chunk_size = 500
        for start in range(0, len(window_ids), chunk_size):
            chunk = window_ids[start : start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.connection.execute(
                f"SELECT window_id FROM window_predictions "
                f"WHERE window_id IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            existing.update(str(row[0]) for row in rows)
        return existing

    def persist_predictions(
        self,
        windows: Sequence[FeatureWindow],
        scores: Sequence[ScoreResult],
        *,
        inference_call_id: str,
        predicted_at_unix: float,
        inference_latency_ms: float,
    ) -> int:
        """Insert every new model decision and materialize anomaly alerts."""

        if len(windows) != len(scores):
            raise ValueError("windows and scores must have equal length")
        if not windows:
            return 0
        if not inference_call_id.strip():
            raise ValueError("inference_call_id must not be empty")
        if inference_latency_ms < 0:
            raise ValueError("inference_latency_ms must be non-negative")
        predicted_at = utc_from_unix(predicted_at_unix)
        per_row_latency = inference_latency_ms / len(windows)
        prediction_rows: list[tuple[object, ...]] = []
        alert_rows: list[tuple[object, ...]] = []
        for window, score in zip(windows, scores, strict=True):
            prediction_rows.append(
                (
                    window.window_id,
                    window.run_id,
                    window.collection_session_id,
                    window.device_id,
                    utc_from_unix(window.window_end_unix),
                    predicted_at,
                    score.score,
                    score.predicted_label,
                    score.threshold,
                    score.model_version,
                    "1.0",
                    inference_call_id,
                    "scored",
                    per_row_latency,
                    max(0.0, (predicted_at_unix - window.window_end_unix) * 1000.0),
                )
            )
            if score.predicted_label == "anomaly":
                alert_rows.append(
                    (
                        window.window_id,
                        window.run_id,
                        window.collection_session_id,
                        predicted_at,
                        window.device_id,
                        score.score,
                        score.threshold,
                        score.model_version,
                        "score_below_frozen_threshold",
                    )
                )
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO inference_calls (
                    inference_call_id, run_id, collection_session_id,
                    model_version, window_ids_json, batch_size,
                    inference_duration_ms, measured_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    inference_call_id,
                    windows[0].run_id,
                    windows[0].collection_session_id,
                    scores[0].model_version,
                    json.dumps(
                        [window.window_id for window in windows],
                        separators=(",", ":"),
                    ),
                    len(windows),
                    inference_latency_ms,
                    predicted_at,
                ),
            )
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO window_predictions (
                    window_id, run_id, collection_session_id, device_id,
                    window_end_utc, predicted_at_utc, anomaly_score,
                    predicted_label, decision_threshold, model_version,
                    feature_schema_version, inference_call_id, inference_status,
                    inference_latency_ms, processing_delay_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                prediction_rows,
            )
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO anomaly_alerts (
                    window_id, run_id, collection_session_id, detected_at_utc,
                    device_id, anomaly_score, decision_threshold, model_version,
                    reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                alert_rows,
            )
        return len(prediction_rows)

    def counts(self, run_id: str) -> dict[str, int]:
        result: dict[str, int] = {}
        for table in (
            "feature_windows",
            "inference_calls",
            "window_predictions",
            "anomaly_alerts",
        ):
            row = self.connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE run_id = ?", (run_id,)
            ).fetchone()
            result[table] = int(row[0])
        return result
