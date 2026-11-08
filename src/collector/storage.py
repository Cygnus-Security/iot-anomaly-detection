"""SQLite schema and persistence for accepted MQTT observations."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.messages import (
    MessageValidationError,
    decode_object,
    topic_identity,
    validate_status,
    validate_telemetry,
)

SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class ObservedMessage:
    """Metadata available to a normal MQTT subscriber callback."""

    topic: str
    payload: bytes
    qos: int
    retain: bool
    duplicate: bool
    received_at_unix: float

    @property
    def received_at(self) -> str:
        return datetime.fromtimestamp(
            self.received_at_unix, tz=timezone.utc
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")


class ObservationStore:
    """Single-writer SQLite store with active ground-truth state."""

    def __init__(self, database_path: str) -> None:
        self.database_path = database_path
        path = Path(database_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        # The connection is created during service initialization and then owned
        # by the dedicated writer thread until shutdown.
        self.connection = sqlite3.connect(
            database_path, timeout=30.0, check_same_thread=False
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self._create_schema()
        self._last_telemetry = self._load_last_telemetry()
        self._active_scenarios = self._load_active_scenarios()

    def close(self) -> None:
        self.connection.close()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_metadata (
                schema_version INTEGER NOT NULL,
                initialized_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS mqtt_observations (
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

            CREATE INDEX IF NOT EXISTS idx_observations_device_time
                ON mqtt_observations(device_id, received_at_unix);
            CREATE INDEX IF NOT EXISTS idx_observations_scenario
                ON mqtt_observations(scenario_run_id);
            CREATE INDEX IF NOT EXISTS idx_observations_type_time
                ON mqtt_observations(message_type, received_at_unix);

            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                last_telemetry_at TEXT,
                last_status_at TEXT,
                last_status_event TEXT,
                is_online INTEGER,
                last_sequence INTEGER
            );

            CREATE TABLE IF NOT EXISTS scenario_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_message_id TEXT NOT NULL UNIQUE,
                scenario_run_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                scenario TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN ('scenario_started', 'scenario_ended')),
                event_at TEXT NOT NULL,
                received_at TEXT NOT NULL,
                seed INTEGER,
                duration_seconds REAL,
                details_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_scenario_events_run
                ON scenario_events(scenario_run_id, event_at);
            """
        )
        count = self.connection.execute(
            "SELECT COUNT(*) FROM schema_metadata"
        ).fetchone()[0]
        if count == 0:
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            self.connection.execute(
                "INSERT INTO schema_metadata(schema_version, initialized_at) VALUES (?, ?)",
                (SCHEMA_VERSION, now),
            )
        else:
            version = self.connection.execute(
                "SELECT schema_version FROM schema_metadata LIMIT 1"
            ).fetchone()[0]
            if version != SCHEMA_VERSION:
                raise RuntimeError(
                    f"unsupported database schema {version}; expected {SCHEMA_VERSION}"
                )
        self.connection.commit()

    def _load_last_telemetry(self) -> dict[str, float]:
        rows = self.connection.execute(
            """
            SELECT device_id, MAX(received_at_unix) AS last_received
            FROM mqtt_observations
            WHERE message_type = 'telemetry' AND payload_valid = 1
            GROUP BY device_id
            """
        ).fetchall()
        return {str(row["device_id"]): float(row["last_received"]) for row in rows}

    def _load_active_scenarios(self) -> dict[str, tuple[str, str]]:
        active: dict[str, tuple[str, str]] = {}
        rows = self.connection.execute(
            """
            SELECT device_id, scenario, scenario_run_id, event_type
            FROM scenario_events
            ORDER BY id
            """
        ).fetchall()
        for row in rows:
            device_id = str(row["device_id"])
            if row["event_type"] == "scenario_started":
                active[device_id] = (
                    str(row["scenario"]),
                    str(row["scenario_run_id"]),
                )
            elif active.get(device_id, (None, None))[1] == row["scenario_run_id"]:
                active.pop(device_id, None)
        return active

    def process(self, observed: ObservedMessage) -> int:
        """Validate, label, and persist one broker-forwarded publication."""

        device_id, message_type = topic_identity(observed.topic)
        payload_text = observed.payload.decode("utf-8", errors="replace")
        parsed: dict[str, Any] = {}
        valid = True
        validation_error: str | None = None
        try:
            parsed = decode_object(observed.payload)
            if message_type == "telemetry":
                validate_telemetry(parsed, device_id)
            else:
                validate_status(parsed, device_id)
        except MessageValidationError as exc:
            valid = False
            validation_error = str(exc)

        event = parsed.get("event") if valid and message_type == "status" else None
        scenario = parsed.get("scenario") if isinstance(parsed.get("scenario"), str) else None
        run_id = (
            parsed.get("scenario_run_id")
            if isinstance(parsed.get("scenario_run_id"), str)
            else None
        )
        if event == "scenario_started" and scenario and run_id:
            self._active_scenarios[device_id] = (scenario, run_id)

        label, active_run_id = self._active_scenarios.get(
            device_id, ("normal", None)
        )
        if event in {"scenario_started", "scenario_ended"} and scenario and run_id:
            label, active_run_id = scenario, run_id

        inter_arrival: float | None = None
        if valid and message_type == "telemetry":
            previous = self._last_telemetry.get(device_id)
            if previous is not None:
                inter_arrival = max(0.0, observed.received_at_unix - previous)
            self._last_telemetry[device_id] = observed.received_at_unix

        message_id = parsed.get("message_id") if valid else None
        publisher_timestamp = parsed.get("timestamp") if valid else None
        sequence = parsed.get("sequence") if valid and message_type == "telemetry" else None

        with self.connection:
            cursor = self.connection.execute(
                """
                INSERT INTO mqtt_observations (
                    received_at, received_at_unix, device_id, topic, message_type,
                    message_id, publisher_timestamp, sequence, payload_size_bytes,
                    payload_text, qos, retain, duplicate, payload_valid,
                    validation_error, inter_arrival_seconds, ground_truth_label,
                    scenario_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observed.received_at,
                    observed.received_at_unix,
                    device_id,
                    observed.topic,
                    message_type,
                    message_id,
                    publisher_timestamp,
                    sequence,
                    len(observed.payload),
                    payload_text,
                    observed.qos,
                    int(observed.retain),
                    int(observed.duplicate),
                    int(valid),
                    validation_error,
                    inter_arrival,
                    label,
                    active_run_id,
                ),
            )
            self._upsert_device(
                device_id=device_id,
                received_at=observed.received_at,
                message_type=message_type,
                event=str(event) if event is not None else None,
                sequence=int(sequence) if sequence is not None else None,
                valid=valid,
            )
            if valid and event in {"scenario_started", "scenario_ended"}:
                self._insert_scenario_event(parsed, observed.received_at)

        if event == "scenario_ended" and run_id:
            current = self._active_scenarios.get(device_id)
            if current is not None and current[1] == run_id:
                self._active_scenarios.pop(device_id, None)
        return int(cursor.lastrowid)

    def _upsert_device(
        self,
        *,
        device_id: str,
        received_at: str,
        message_type: str,
        event: str | None,
        sequence: int | None,
        valid: bool,
    ) -> None:
        last_telemetry = received_at if valid and message_type == "telemetry" else None
        last_status = received_at if valid and message_type == "status" else None
        online: int | None = None
        if event in {"online", "heartbeat"}:
            online = 1
        elif event == "offline":
            online = 0
        self.connection.execute(
            """
            INSERT INTO devices (
                device_id, first_seen_at, last_seen_at, last_telemetry_at,
                last_status_at, last_status_event, is_online, last_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                last_seen_at = excluded.last_seen_at,
                last_telemetry_at = COALESCE(excluded.last_telemetry_at, devices.last_telemetry_at),
                last_status_at = COALESCE(excluded.last_status_at, devices.last_status_at),
                last_status_event = COALESCE(excluded.last_status_event, devices.last_status_event),
                is_online = COALESCE(excluded.is_online, devices.is_online),
                last_sequence = COALESCE(excluded.last_sequence, devices.last_sequence)
            """,
            (
                device_id,
                received_at,
                received_at,
                last_telemetry,
                last_status,
                event,
                online,
                sequence,
            ),
        )

    def _insert_scenario_event(
        self, message: dict[str, Any], received_at: str
    ) -> None:
        details = message.get("details")
        if not isinstance(details, dict):
            details = {}
        self.connection.execute(
            """
            INSERT OR IGNORE INTO scenario_events (
                event_message_id, scenario_run_id, request_id, device_id,
                scenario, event_type, event_at, received_at, seed,
                duration_seconds, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message["message_id"],
                message["scenario_run_id"],
                message["request_id"],
                message["device_id"],
                message["scenario"],
                message["event"],
                message["timestamp"],
                received_at,
                message.get("seed"),
                message.get("duration_seconds"),
                json.dumps(details, separators=(",", ":"), sort_keys=True),
            ),
        )
