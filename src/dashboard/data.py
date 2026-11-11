"""Read-only dashboard queries over the live SQLite observation store."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

SUPPORTED_SCHEMA_VERSION = 3


class DashboardDataError(RuntimeError):
    """Raised when the live database cannot satisfy a dashboard query."""


@dataclass(frozen=True, slots=True)
class Overview:
    """Top-level counters whose meaning is explicit and testable."""

    device_count: int
    stored_online_count: int
    observation_count: int
    telemetry_count: int
    invalid_payload_count: int
    messages_last_minute: int
    messages_per_second_last_minute: float
    abnormal_scenario_runs: int
    latest_received_at: str | None


class DashboardRepository:
    """Open short-lived read-only connections so live WAL writes stay safe."""

    def __init__(self, database_path: str) -> None:
        self.database_path = str(Path(database_path).resolve())

    def check_ready(self) -> None:
        """Fail with a useful message when the collector database is unavailable."""

        path = Path(self.database_path)
        if not path.is_file():
            raise DashboardDataError(f"Database does not exist: {path}")
        try:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT schema_version FROM schema_metadata LIMIT 1"
                ).fetchone()
        except sqlite3.Error as exc:
            raise DashboardDataError(f"Unable to read the database: {exc}") from exc
        if row is None or int(row[0]) != SUPPORTED_SCHEMA_VERSION:
            raise DashboardDataError(
                f"Dashboard requires database schema version {SUPPORTED_SCHEMA_VERSION}"
            )

    def _connect(self) -> sqlite3.Connection:
        uri = Path(self.database_path).as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _frame(self, query: str, parameters: Sequence[Any] = ()) -> pd.DataFrame:
        try:
            with self._connect() as connection:
                return pd.read_sql_query(query, connection, params=tuple(parameters))
        except (sqlite3.Error, pd.errors.DatabaseError) as exc:
            raise DashboardDataError(str(exc)) from exc

    def overview(self, now_unix: float | None = None) -> Overview:
        now = time.time() if now_unix is None else now_unix
        query = """
            SELECT
                (SELECT COUNT(*) FROM devices) AS device_count,
                (SELECT COUNT(*) FROM devices WHERE is_online = 1) AS online_count,
                (SELECT COUNT(*) FROM mqtt_observations) AS observation_count,
                (SELECT COUNT(*) FROM mqtt_observations
                 WHERE message_type = 'telemetry' AND payload_valid = 1
                   AND duplicate = 0) AS telemetry_count,
                (SELECT COUNT(*) FROM mqtt_observations
                 WHERE payload_valid = 0) AS invalid_payload_count,
                (SELECT COUNT(*) FROM mqtt_observations
                 WHERE message_type = 'telemetry' AND payload_valid = 1
                   AND duplicate = 0
                   AND received_at_unix >= ?) AS messages_last_minute,
                (SELECT COUNT(*) FROM scenario_events
                 WHERE event_type = 'scenario_started' AND scenario != 'normal')
                    AS abnormal_scenario_runs,
                (SELECT MAX(received_at) FROM mqtt_observations) AS latest_received_at
        """
        frame = self._frame(query, (now - 60.0,))
        if frame.empty:
            raise DashboardDataError("Unable to read dashboard overview metrics")
        row = frame.iloc[0]
        messages_last_minute = int(row["messages_last_minute"] or 0)
        return Overview(
            device_count=int(row["device_count"] or 0),
            stored_online_count=int(row["online_count"] or 0),
            observation_count=int(row["observation_count"] or 0),
            telemetry_count=int(row["telemetry_count"] or 0),
            invalid_payload_count=int(row["invalid_payload_count"] or 0),
            messages_last_minute=messages_last_minute,
            messages_per_second_last_minute=messages_last_minute / 60.0,
            abnormal_scenario_runs=int(row["abnormal_scenario_runs"] or 0),
            latest_received_at=(
                str(row["latest_received_at"])
                if row["latest_received_at"] is not None
                else None
            ),
        )

    def devices(
        self,
        *,
        window_seconds: int,
        now_unix: float | None = None,
        stale_after_seconds: float = 15.0,
    ) -> pd.DataFrame:
        now = time.time() if now_unix is None else now_unix
        cutoff = 0.0 if window_seconds <= 0 else now - window_seconds
        frame = self._frame(
            """
            SELECT
                d.device_id,
                d.first_seen_at,
                d.last_seen_at,
                d.last_telemetry_at,
                d.last_status_event,
                d.is_online,
                d.last_sequence,
                COUNT(o.id) AS telemetry_in_window,
                AVG(o.payload_size_bytes) AS avg_payload_bytes,
                AVG(o.inter_arrival_seconds) AS avg_inter_arrival_seconds
            FROM devices AS d
            LEFT JOIN mqtt_observations AS o
              ON o.device_id = d.device_id
             AND o.message_type = 'telemetry'
             AND o.payload_valid = 1
             AND o.duplicate = 0
             AND o.received_at_unix >= ?
            GROUP BY d.device_id
            ORDER BY d.device_id
            """,
            (cutoff,),
        )
        if frame.empty:
            frame["observed_state"] = pd.Series(dtype="string")
            return frame
        last_seen = pd.to_datetime(frame["last_seen_at"], utc=True, errors="coerce")
        # Pandas 3 may use microsecond rather than nanosecond datetime storage;
        # Timestamp.timestamp() keeps this calculation unit-independent.
        last_seen_unix = last_seen.map(
            lambda value: value.timestamp() if pd.notna(value) else 0.0
        )
        age = now - last_seen_unix
        frame["age_seconds"] = age.clip(lower=0).round(1)
        frame["observed_state"] = "unknown"
        frame.loc[frame["is_online"] == 0, "observed_state"] = "offline"
        frame.loc[frame["is_online"] == 1, "observed_state"] = "online"
        frame.loc[
            (frame["is_online"] == 1)
            & (frame["age_seconds"] > stale_after_seconds),
            "observed_state",
        ] = "stale"
        return frame

    def traffic_series(
        self,
        *,
        window_seconds: int,
        device_id: str | None = None,
        now_unix: float | None = None,
    ) -> pd.DataFrame:
        now = time.time() if now_unix is None else now_unix
        cutoff = 0.0 if window_seconds <= 0 else now - window_seconds
        bucket_seconds = 10 if 0 < window_seconds <= 900 else 60
        device_clause = "" if device_id is None else "AND device_id = ?"
        parameters: list[Any] = [bucket_seconds, bucket_seconds, cutoff]
        if device_id is not None:
            parameters.append(device_id)
        frame = self._frame(
            f"""
            SELECT
                CAST(received_at_unix / ? AS INTEGER) * ? AS bucket_unix,
                COUNT(*) AS messages
            FROM mqtt_observations
            WHERE message_type = 'telemetry'
              AND payload_valid = 1
              AND duplicate = 0
              AND received_at_unix >= ?
              {device_clause}
            GROUP BY bucket_unix
            ORDER BY bucket_unix
            """,
            parameters,
        )
        if not frame.empty:
            frame["bucket_at"] = pd.to_datetime(
                frame["bucket_unix"], unit="s", utc=True
            )
        return frame

    def label_counts(
        self,
        *,
        window_seconds: int,
        device_id: str | None = None,
        now_unix: float | None = None,
    ) -> pd.DataFrame:
        now = time.time() if now_unix is None else now_unix
        cutoff = 0.0 if window_seconds <= 0 else now - window_seconds
        device_clause = "" if device_id is None else "AND device_id = ?"
        parameters: list[Any] = [cutoff]
        if device_id is not None:
            parameters.append(device_id)
        return self._frame(
            f"""
            SELECT ground_truth_label, COUNT(*) AS messages
            FROM mqtt_observations
            WHERE message_type = 'telemetry'
              AND payload_valid = 1
              AND duplicate = 0
              AND received_at_unix >= ?
              {device_clause}
            GROUP BY ground_truth_label
            ORDER BY messages DESC
            """,
            parameters,
        )

    def latest_observations(
        self,
        *,
        limit: int = 100,
        device_id: str | None = None,
    ) -> pd.DataFrame:
        device_clause = "" if device_id is None else "AND device_id = ?"
        parameters: list[Any] = [] if device_id is None else [device_id]
        parameters.append(limit)
        return self._frame(
            f"""
            SELECT received_at, device_id, message_type, topic, sequence,
                   payload_size_bytes, inter_arrival_seconds,
                   ground_truth_label, payload_valid, validation_error
            FROM mqtt_observations
            WHERE NOT (message_type = 'telemetry' AND duplicate = 1)
            {device_clause}
            ORDER BY id DESC
            LIMIT ?
            """,
            parameters,
        )

    def device_telemetry(self, device_id: str, limit: int = 300) -> pd.DataFrame:
        frame = self._frame(
            """
            SELECT received_at, sequence, payload_size_bytes,
                   inter_arrival_seconds, ground_truth_label, payload_text
            FROM mqtt_observations
            WHERE device_id = ? AND message_type = 'telemetry'
              AND payload_valid = 1
              AND duplicate = 0
            ORDER BY id DESC
            LIMIT ?
            """,
            (device_id, limit),
        )
        if frame.empty:
            return frame
        frame = frame.iloc[::-1].reset_index(drop=True)
        frame["received_at"] = pd.to_datetime(
            frame["received_at"], utc=True, errors="coerce"
        )
        sensor_rows = [self._sensor_values(value) for value in frame["payload_text"]]
        frame["temperature_c"] = [item[0] for item in sensor_rows]
        frame["humidity_percent"] = [item[1] for item in sensor_rows]
        return frame.drop(columns=["payload_text"])

    @staticmethod
    def _sensor_values(payload_text: str) -> tuple[float | None, float | None]:
        try:
            payload = json.loads(payload_text)
            measurements = payload.get("measurements")
            if isinstance(measurements, dict):
                return (
                    measurements.get("temperature_c"),
                    measurements.get("relative_humidity_pct"),
                )
            sensor = payload.get("sensor", {})
            return sensor.get("temperature_c"), sensor.get("humidity_percent")
        except (json.JSONDecodeError, AttributeError):
            return None, None

    def scenario_runs(self, limit: int = 100) -> pd.DataFrame:
        frame = self._frame(
            """
            SELECT
                scenario_run_id,
                MAX(device_id) AS device_id,
                MAX(scenario) AS scenario,
                MIN(CASE WHEN event_type = 'scenario_started' THEN event_at END)
                    AS started_at,
                MAX(CASE WHEN event_type = 'scenario_ended' THEN event_at END)
                    AS ended_at,
                MAX(seed) AS seed,
                MAX(duration_seconds) AS configured_duration_seconds,
                MAX(CASE WHEN event_type = 'scenario_started' THEN details_json END)
                    AS start_details
            FROM scenario_events
            GROUP BY scenario_run_id
            ORDER BY MIN(id) DESC
            LIMIT ?
            """,
            (limit,),
        )
        if frame.empty:
            frame["run_state"] = pd.Series(dtype="string")
            return frame
        started = pd.to_datetime(frame["started_at"], utc=True, errors="coerce")
        ended = pd.to_datetime(frame["ended_at"], utc=True, errors="coerce")
        frame["actual_duration_seconds"] = (ended - started).dt.total_seconds().round(3)
        frame["run_state"] = frame["ended_at"].apply(
            lambda value: "completed" if pd.notna(value) else "active"
        )
        return frame

    def has_detection_table(self) -> bool:
        frame = self._frame(
            """
            SELECT COUNT(*) AS table_count
            FROM sqlite_master
            WHERE type = 'table' AND name = 'anomaly_alerts'
            """
        )
        return bool(int(frame.iloc[0]["table_count"]))

    def has_prediction_table(self) -> bool:
        """Return whether Phase 3 has initialized the all-decisions table."""

        frame = self._frame(
            """
            SELECT COUNT(*) AS table_count
            FROM sqlite_master
            WHERE type = 'table' AND name = 'window_predictions'
            """
        )
        return bool(int(frame.iloc[0]["table_count"]))

    def prediction_count(self) -> int:
        if not self.has_prediction_table():
            return 0
        frame = self._frame("SELECT COUNT(*) AS prediction_count FROM window_predictions")
        return int(frame.iloc[0]["prediction_count"] or 0)

    def alert_count(self) -> int:
        if not self.has_detection_table():
            return 0
        frame = self._frame("SELECT COUNT(*) AS alert_count FROM anomaly_alerts")
        return int(frame.iloc[0]["alert_count"] or 0)

    def predictions(
        self, *, limit: int = 500, device_id: str | None = None
    ) -> pd.DataFrame:
        """Read every model decision, including normal results, for traceability."""

        device_clause = "" if device_id is None else "WHERE p.device_id = ?"
        parameters: list[Any] = [] if device_id is None else [device_id]
        parameters.append(limit)
        frame = self._frame(
            f"""
            SELECT p.predicted_at_utc, p.device_id, p.window_id,
                   p.anomaly_score, p.decision_threshold, p.predicted_label,
                   p.model_version, p.inference_status,
                   p.processing_delay_ms, f.window_start_utc, f.window_end_utc,
                   f.ground_truth_label, f.feature_status
            FROM window_predictions AS p
            LEFT JOIN feature_windows AS f ON f.window_id = p.window_id
            {device_clause}
            ORDER BY p.predicted_at_utc DESC
            LIMIT ?
            """,
            parameters,
        )
        if not frame.empty:
            frame["window_end_utc"] = pd.to_datetime(
                frame["window_end_utc"], utc=True, errors="coerce"
            )
        return frame

    def detections(self, limit: int = 200) -> pd.DataFrame:
        """Read the Phase 3 anomaly-alert contract when that table exists."""

        return self._frame(
            """
            SELECT detected_at_utc, device_id, anomaly_score,
                   decision_threshold, model_version, window_id, reason
            FROM anomaly_alerts
            ORDER BY detected_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        )
