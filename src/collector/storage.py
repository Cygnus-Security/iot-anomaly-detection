"""SQLite persistence for broker-forwarded MQTT observations and lab events."""

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
    validate_experiment_event,
    validate_status,
    validate_telemetry,
)

SCHEMA_VERSION = 3
SCHEMA_MAINTENANCE_REVISION = 1
ACTIVE_SCENARIO_GRACE_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class ObservedMessage:
    """Metadata exposed by the MQTT subscriber callback."""

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
    """Single-writer SQLite store with application-level QoS 1 deduplication."""

    def __init__(self, database_path: str, *, max_payload_bytes: int = 4096) -> None:
        self.database_path = database_path
        self.max_payload_bytes = max_payload_bytes
        path = Path(database_path)
        path.parent.mkdir(parents=True, exist_ok=True)
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
        # The four Phase 2 tables intentionally remain backward compatible.
        # Logical v1 protocol names map as follows: publisher_timestamp stores
        # sent_at_utc, sequence stores sequence_no, and duplicate stores the
        # application-level is_duplicate decision.
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_metadata (
                schema_version INTEGER NOT NULL,
                initialized_at TEXT NOT NULL,
                maintenance_revision INTEGER NOT NULL DEFAULT 0
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
                session_id TEXT,
                sequence INTEGER,
                payload_size_bytes INTEGER NOT NULL,
                payload_text TEXT NOT NULL,
                qos INTEGER NOT NULL,
                retain INTEGER NOT NULL,
                mqtt_duplicate INTEGER NOT NULL DEFAULT 0,
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
        metadata_columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(schema_metadata)"
            ).fetchall()
        }
        if "maintenance_revision" not in metadata_columns:
            self.connection.execute(
                """
                ALTER TABLE schema_metadata
                ADD COLUMN maintenance_revision INTEGER NOT NULL DEFAULT 0
                """
            )
        count = self.connection.execute(
            "SELECT COUNT(*) FROM schema_metadata"
        ).fetchone()[0]
        if count == 0:
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            self.connection.execute(
                """INSERT INTO schema_metadata(
                       schema_version, initialized_at, maintenance_revision
                   ) VALUES (?, ?, ?)""",
                (SCHEMA_VERSION, now, SCHEMA_MAINTENANCE_REVISION),
            )
        else:
            version = int(
                self.connection.execute(
                    "SELECT schema_version FROM schema_metadata LIMIT 1"
                ).fetchone()[0]
            )
            if version == 1:
                self._migrate_v1_to_v3()
            elif version == 2:
                self._migrate_v2_to_v3()
            elif version != SCHEMA_VERSION:
                raise RuntimeError(
                    f"unsupported database schema {version}; expected {SCHEMA_VERSION}"
                )
            maintenance_revision = int(
                self.connection.execute(
                    "SELECT maintenance_revision FROM schema_metadata LIMIT 1"
                ).fetchone()[0]
            )
            if maintenance_revision < SCHEMA_MAINTENANCE_REVISION:
                self._rebuild_derived_state()
                self.connection.execute(
                    "UPDATE schema_metadata SET maintenance_revision = ?",
                    (SCHEMA_MAINTENANCE_REVISION,),
                )
        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_observations_session_sequence
            ON mqtt_observations(device_id, session_id, sequence)
            """
        )
        self.connection.commit()

    def _migrate_v1_to_v3(self) -> None:
        columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(mqtt_observations)"
            ).fetchall()
        }
        if "session_id" not in columns:
            self.connection.execute(
                "ALTER TABLE mqtt_observations ADD COLUMN session_id TEXT"
            )
        if "mqtt_duplicate" not in columns:
            self.connection.execute(
                """
                ALTER TABLE mqtt_observations
                ADD COLUMN mqtt_duplicate INTEGER NOT NULL DEFAULT 0
                """
            )
        # In schema v1, `duplicate` stored Paho's MQTT DUP flag. Preserve that
        # diagnostic value separately; legacy rows have no session key from
        # which an application duplicate can be derived. Run this repair even
        # when the column already exists so restart after a partially completed
        # ALTER remains safe and deterministic.
        self.connection.execute(
            """
            UPDATE mqtt_observations
            SET mqtt_duplicate = CASE
                    WHEN mqtt_duplicate = 1 OR duplicate = 1 THEN 1 ELSE 0 END,
                duplicate = 0
            WHERE session_id IS NULL
            """
        )
        self.connection.execute(
            "UPDATE schema_metadata SET schema_version = ?", (SCHEMA_VERSION,)
        )

    def _migrate_v2_to_v3(self) -> None:
        """Separate MQTT's DUP flag from application-key deduplication."""

        columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(mqtt_observations)"
            ).fetchall()
        }
        if "mqtt_duplicate" not in columns:
            self.connection.execute(
                """
                ALTER TABLE mqtt_observations
                ADD COLUMN mqtt_duplicate INTEGER NOT NULL DEFAULT 0
                """
            )
        # These transformations deliberately run even if a previous startup
        # already added the column. SQLite can preserve DDL across an interrupted
        # migration, so semantic repairs must be restart-idempotent.
        self.connection.execute(
            """
            UPDATE mqtt_observations
            SET mqtt_duplicate = CASE
                    WHEN mqtt_duplicate = 1 OR duplicate = 1 THEN 1 ELSE 0 END,
                duplicate = 0
            WHERE session_id IS NULL
            """
        )
        # Schema v2 combined MQTT's callback DUP flag with application-key
        # duplication. Exact broker-DUP provenance cannot be recovered for
        # session-aware rows, so leave mqtt_duplicate as false-or-unknown and
        # deterministically reconstruct only application duplication.
        self.connection.execute(
            """
            UPDATE mqtt_observations AS current
            SET duplicate = CASE
                WHEN current.message_type = 'telemetry'
                 AND current.payload_valid = 1
                 AND current.session_id IS NOT NULL
                 AND EXISTS (
                     SELECT 1 FROM mqtt_observations AS earlier
                     WHERE earlier.message_type = 'telemetry'
                       AND earlier.payload_valid = 1
                       AND earlier.device_id = current.device_id
                       AND earlier.session_id = current.session_id
                       AND earlier.sequence = current.sequence
                       AND (
                           earlier.received_at_unix < current.received_at_unix
                           OR (
                               earlier.received_at_unix = current.received_at_unix
                               AND earlier.id < current.id
                           )
                       )
                 )
                THEN 1 ELSE 0 END
            WHERE current.session_id IS NOT NULL
            """
        )
        self.connection.execute(
            "UPDATE schema_metadata SET schema_version = ?", (SCHEMA_VERSION,)
        )

    def _rebuild_derived_state(self) -> None:
        """Rebuild values whose meaning depends on canonical telemetry rows."""

        self.connection.execute(
            """
            UPDATE mqtt_observations
            SET inter_arrival_seconds = NULL
            WHERE message_type = 'telemetry'
            """
        )
        rows = self.connection.execute(
            """
            SELECT id, device_id, received_at_unix
            FROM mqtt_observations
            WHERE message_type = 'telemetry'
              AND payload_valid = 1
              AND duplicate = 0
            ORDER BY device_id, received_at_unix, id
            """
        ).fetchall()
        previous_by_device: dict[str, float] = {}
        updates: list[tuple[float | None, int]] = []
        for row in rows:
            device_id = str(row["device_id"])
            received_at_unix = float(row["received_at_unix"])
            previous = previous_by_device.get(device_id)
            interval = (
                None
                if previous is None
                else max(0.0, received_at_unix - previous)
            )
            updates.append((interval, int(row["id"])))
            previous_by_device[device_id] = received_at_unix
        self.connection.executemany(
            "UPDATE mqtt_observations SET inter_arrival_seconds = ? WHERE id = ?",
            updates,
        )

        self.connection.execute("DELETE FROM devices")
        observations = self.connection.execute(
            """
            SELECT received_at, received_at_unix, publisher_timestamp, device_id,
                   message_type, sequence, retain, payload_text
            FROM mqtt_observations
            WHERE payload_valid = 1
              AND NOT (message_type = 'telemetry' AND duplicate = 1)
            ORDER BY received_at_unix, id
            """
        ).fetchall()
        for row in observations:
            message_type = str(row["message_type"])
            state: str | None = None
            if message_type == "status":
                try:
                    payload = json.loads(str(row["payload_text"]))
                except (json.JSONDecodeError, TypeError):
                    payload = {}
                candidate = payload.get("state", payload.get("event"))
                if candidate == "heartbeat":
                    candidate = "online"
                if candidate in {"online", "offline"}:
                    state = str(candidate)

            effective_seen_at = str(row["received_at"])
            if message_type == "status" and bool(row["retain"]):
                effective_seen_at = self._retained_status_time(
                    effective_seen_at,
                    (
                        str(row["publisher_timestamp"])
                        if row["publisher_timestamp"]
                        else None
                    ),
                )
            self._upsert_device(
                device_id=str(row["device_id"]),
                received_at=effective_seen_at,
                message_type=message_type,
                state=state,
                sequence=(
                    int(row["sequence"])
                    if row["sequence"] is not None
                    else None
                ),
            )

    @staticmethod
    def _retained_status_time(
        received_at: str, publisher_timestamp: str | None
    ) -> str:
        """Use historical status time without allowing a future clock to poison state."""

        if not publisher_timestamp:
            return received_at
        try:
            received = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
            published = datetime.fromisoformat(
                publisher_timestamp.replace("Z", "+00:00")
            )
            if received.tzinfo is None or published.tzinfo is None:
                return received_at
            effective = published if published <= received else received
        except (TypeError, ValueError):
            return received_at
        return effective.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")

    def _load_last_telemetry(self) -> dict[str, float]:
        rows = self.connection.execute(
            """
            SELECT device_id, MAX(received_at_unix) AS last_received
            FROM mqtt_observations
            WHERE message_type = 'telemetry'
              AND payload_valid = 1
              AND duplicate = 0
            GROUP BY device_id
            """
        ).fetchall()
        return {str(row["device_id"]): float(row["last_received"]) for row in rows}

    def _load_active_scenarios(self) -> dict[str, tuple[str, str, float]]:
        active: dict[str, tuple[str, str, float]] = {}
        now = datetime.now(timezone.utc).timestamp()
        rows = self.connection.execute(
            """
            SELECT device_id, scenario, scenario_run_id, event_type,
                   received_at, duration_seconds
            FROM scenario_events
            ORDER BY id
            """
        ).fetchall()
        for row in rows:
            device_id = str(row["device_id"])
            run_id = str(row["scenario_run_id"])
            if row["event_type"] == "scenario_started":
                try:
                    started = datetime.fromisoformat(
                        str(row["received_at"]).replace("Z", "+00:00")
                    ).timestamp()
                    duration = float(row["duration_seconds"] or 0.0)
                except (TypeError, ValueError):
                    continue
                expires_at = started + duration + ACTIVE_SCENARIO_GRACE_SECONDS
                if now <= expires_at:
                    active[device_id] = (
                        str(row["scenario"]),
                        run_id,
                        expires_at,
                    )
                else:
                    active.pop(device_id, None)
            elif active.get(device_id, (None, None))[1] == run_id:
                active.pop(device_id, None)
        return active

    def process(self, observed: ObservedMessage) -> int:
        """Validate and persist one broker-forwarded publication."""

        device_id, message_type = topic_identity(observed.topic)
        payload_size = len(observed.payload)
        payload_text = observed.payload.decode("utf-8", errors="replace")
        parsed: dict[str, Any] = {}
        valid = True
        validation_error: str | None = None

        if payload_size > self.max_payload_bytes:
            valid = False
            validation_error = (
                f"payload exceeds {self.max_payload_bytes}-byte collector limit"
            )
            payload_text = (
                f"[payload omitted: {payload_size} bytes exceeds "
                f"{self.max_payload_bytes}-byte limit]"
            )
        else:
            try:
                parsed = decode_object(observed.payload)
                if message_type == "telemetry":
                    validate_telemetry(parsed, device_id)
                elif message_type == "status":
                    validate_status(parsed, device_id)
                else:
                    validate_experiment_event(parsed, device_id)
            except MessageValidationError as exc:
                valid = False
                validation_error = str(exc)

        if message_type == "experiment":
            if not valid:
                raise MessageValidationError(
                    validation_error or "invalid experiment event"
                )
            with self.connection:
                event_id = self._insert_scenario_event(parsed, observed.received_at)
                if event_id:
                    self._apply_scenario_event(parsed, observed.received_at_unix)
            return event_id

        session_id = (
            str(parsed["session_id"])
            if valid and isinstance(parsed.get("session_id"), str)
            else None
        )
        sequence = (
            int(parsed["sequence_no"])
            if valid
            and message_type == "telemetry"
            and isinstance(parsed.get("sequence_no"), int)
            else None
        )
        is_duplicate = False
        if valid and message_type == "telemetry" and session_id is not None:
            is_duplicate = self._telemetry_already_seen(
                device_id, session_id, int(sequence)
            )

        inter_arrival: float | None = None
        if valid and message_type == "telemetry" and not is_duplicate:
            previous = self._last_telemetry.get(device_id)
            if previous is not None:
                inter_arrival = max(0.0, observed.received_at_unix - previous)
            self._last_telemetry[device_id] = observed.received_at_unix

        state = (
            str(parsed["state"])
            if valid
            and message_type == "status"
            and isinstance(parsed.get("state"), str)
            else None
        )
        active_scenario = self._active_scenarios.get(device_id)
        if (
            active_scenario is not None
            and observed.received_at_unix > active_scenario[2]
        ):
            self._active_scenarios.pop(device_id, None)
            active_scenario = None
        if active_scenario is None:
            scenario_label, active_run_id = "normal", None
        else:
            scenario_label, active_run_id = active_scenario[:2]
        message_id = (
            str(parsed["message_id"])
            if valid and isinstance(parsed.get("message_id"), str)
            else None
        )
        publisher_timestamp = (
            str(parsed["sent_at_utc"])
            if valid and isinstance(parsed.get("sent_at_utc"), str)
            else None
        )

        with self.connection:
            cursor = self.connection.execute(
                """
                INSERT INTO mqtt_observations (
                    received_at, received_at_unix, device_id, topic, message_type,
                    message_id, publisher_timestamp, session_id, sequence,
                    payload_size_bytes, payload_text, qos, retain, mqtt_duplicate,
                    duplicate, payload_valid, validation_error,
                    inter_arrival_seconds, ground_truth_label, scenario_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observed.received_at,
                    observed.received_at_unix,
                    device_id,
                    observed.topic,
                    message_type,
                    message_id,
                    publisher_timestamp,
                    session_id,
                    sequence,
                    payload_size,
                    payload_text,
                    observed.qos,
                    int(observed.retain),
                    int(observed.duplicate),
                    int(is_duplicate),
                    int(valid),
                    validation_error,
                    inter_arrival,
                    scenario_label,
                    active_run_id,
                ),
            )
            if valid and not is_duplicate:
                effective_seen_at = observed.received_at
                if message_type == "status" and observed.retain:
                    effective_seen_at = self._retained_status_time(
                        effective_seen_at, publisher_timestamp
                    )
                self._upsert_device(
                    device_id=device_id,
                    received_at=effective_seen_at,
                    message_type=message_type,
                    state=state,
                    sequence=sequence,
                )
        return int(cursor.lastrowid)

    def _telemetry_already_seen(
        self, device_id: str, session_id: str, sequence: int
    ) -> bool:
        row = self.connection.execute(
            """
            SELECT 1
            FROM mqtt_observations
            WHERE device_id = ? AND session_id = ? AND sequence = ?
              AND message_type = 'telemetry' AND payload_valid = 1
            LIMIT 1
            """,
            (device_id, session_id, sequence),
        ).fetchone()
        return row is not None

    def _apply_scenario_event(
        self, message: dict[str, Any], received_at_unix: float
    ) -> None:
        device_id = str(message["device_id"])
        run_id = str(message["experiment_run_id"])
        if message["event"] == "scenario_started":
            duration = float(message["duration_seconds"])
            expires_at = (
                received_at_unix + duration + ACTIVE_SCENARIO_GRACE_SECONDS
            )
            self._active_scenarios[device_id] = (
                str(message["scenario"]),
                run_id,
                expires_at,
            )
        elif self._active_scenarios.get(device_id, (None, None))[1] == run_id:
            self._active_scenarios.pop(device_id, None)

    def _upsert_device(
        self,
        *,
        device_id: str,
        received_at: str,
        message_type: str,
        state: str | None,
        sequence: int | None,
    ) -> None:
        last_telemetry = received_at if message_type == "telemetry" else None
        last_status = received_at if message_type == "status" else None
        online: int | None = None
        if state == "online":
            online = 1
        elif state == "offline":
            online = 0
        self.connection.execute(
            """
            INSERT INTO devices (
                device_id, first_seen_at, last_seen_at, last_telemetry_at,
                last_status_at, last_status_event, is_online, last_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                last_seen_at = CASE
                    WHEN julianday(excluded.last_seen_at) >= julianday(devices.last_seen_at)
                    THEN excluded.last_seen_at ELSE devices.last_seen_at END,
                last_telemetry_at = CASE
                    WHEN excluded.last_telemetry_at IS NULL THEN devices.last_telemetry_at
                    WHEN devices.last_telemetry_at IS NULL
                      OR julianday(excluded.last_telemetry_at) >= julianday(devices.last_telemetry_at)
                    THEN excluded.last_telemetry_at ELSE devices.last_telemetry_at END,
                last_status_at = CASE
                    WHEN excluded.last_status_at IS NULL THEN devices.last_status_at
                    WHEN devices.last_status_at IS NULL
                      OR julianday(excluded.last_status_at) >= julianday(devices.last_status_at)
                    THEN excluded.last_status_at ELSE devices.last_status_at END,
                last_status_event = CASE
                    WHEN excluded.last_status_at IS NOT NULL
                     AND (devices.last_status_at IS NULL
                       OR julianday(excluded.last_status_at) >= julianday(devices.last_status_at))
                    THEN excluded.last_status_event ELSE devices.last_status_event END,
                is_online = CASE
                    WHEN excluded.last_status_at IS NOT NULL
                     AND (devices.last_status_at IS NULL
                       OR julianday(excluded.last_status_at) >= julianday(devices.last_status_at))
                    THEN excluded.is_online ELSE devices.is_online END,
                last_sequence = COALESCE(excluded.last_sequence, devices.last_sequence)
            """,
            (
                device_id,
                received_at,
                received_at,
                last_telemetry,
                last_status,
                state,
                online,
                sequence,
            ),
        )

    def _insert_scenario_event(
        self, message: dict[str, Any], received_at: str
    ) -> int:
        details = message.get("details")
        if not isinstance(details, dict):
            details = {}
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO scenario_events (
                event_message_id, scenario_run_id, request_id, device_id,
                scenario, event_type, event_at, received_at, seed,
                duration_seconds, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message["event_id"],
                message["experiment_run_id"],
                message["request_id"],
                message["device_id"],
                message["scenario"],
                message["event"],
                message["event_at_utc"],
                received_at,
                message.get("seed"),
                message.get("duration_seconds"),
                json.dumps(details, separators=(",", ":"), sort_keys=True),
            ),
        )
        return int(cursor.lastrowid) if cursor.rowcount else 0
