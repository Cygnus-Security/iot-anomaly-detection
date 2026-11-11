"""Atomic, cross-process progress state for the Collector writer queue.

The MQTT callback timestamps an observation before it enters the SQLite writer
queue.  Analytics reads this small sidecar file before closing a time window so
it cannot mistake a delayed database write for missing telemetry.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


STATE_SCHEMA_VERSION = "1.0"


def runtime_state_path(database_path: str | Path) -> Path:
    """Return the sidecar path shared by Collector and Analytics."""

    return Path(f"{database_path}.collector-state.json")


def _utc_from_unix(value: float) -> str:
    return (
        datetime.fromtimestamp(value, tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _topic_identity(topic: str) -> tuple[str | None, str]:
    parts = topic.split("/")
    if len(parts) == 5 and parts[:3] == ["iot", "v1", "devices"]:
        return parts[3] or None, parts[4]
    if len(parts) == 5 and parts[:3] == ["experiments", "v1", "devices"]:
        return parts[3] or None, "experiment_event"
    return None, "unknown"


@dataclass(frozen=True, slots=True)
class CollectorStateSnapshot:
    """Validated subset of the Collector sidecar consumed by Analytics."""

    queue_depth: int
    oldest_pending_received_at_unix: float | None
    last_enqueued_at_unix: float | None
    last_persisted_received_at_unix: float | None
    dropped_message_count: int
    loss_events: tuple[dict[str, Any], ...]
    accepting_messages: bool


def load_runtime_state(path: str | Path) -> CollectorStateSnapshot | None:
    """Read one atomic snapshot; unavailable or malformed state is explicit."""

    try:
        decoded = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, Mapping) or decoded.get("schema_version") != STATE_SCHEMA_VERSION:
        return None
    try:
        queue_depth = int(decoded["queue_depth"])
        dropped = int(decoded["dropped_message_count"])
        oldest_raw = decoded.get("oldest_pending_received_at_unix")
        enqueued_raw = decoded.get("last_enqueued_at_unix")
        persisted_raw = decoded.get("last_persisted_received_at_unix")
        loss_raw = decoded.get("loss_events", [])
        if queue_depth < 0 or dropped < 0 or not isinstance(loss_raw, list):
            return None
        if queue_depth > 0 and oldest_raw is None:
            return None
        loss_events = tuple(
            dict(item) for item in loss_raw if isinstance(item, Mapping)
        )
        return CollectorStateSnapshot(
            queue_depth=queue_depth,
            oldest_pending_received_at_unix=(
                None if oldest_raw is None else float(oldest_raw)
            ),
            last_enqueued_at_unix=(None if enqueued_raw is None else float(enqueued_raw)),
            last_persisted_received_at_unix=(
                None if persisted_raw is None else float(persisted_raw)
            ),
            dropped_message_count=dropped,
            loss_events=loss_events,
            accepting_messages=bool(decoded.get("accepting_messages", False)),
        )
    except (KeyError, TypeError, ValueError):
        return None


class CollectorRuntimeState:
    """Thread-safe writer for queue progress and explicit collection loss."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._pending: deque[tuple[float, str]] = deque()
        self._last_enqueued_at_unix: float | None = None
        self._last_persisted_received_at_unix: float | None = None
        self._dropped_message_count = 0
        self._loss_events: list[dict[str, Any]] = []
        self._accepting_messages = True
        self._started_at_unix = time.time()
        with self._lock:
            self._write_locked()

    def reserve(self, *, received_at_unix: float, topic: str) -> None:
        """Publish pending state before placing an item on the in-process queue."""

        with self._lock:
            self._pending.append((received_at_unix, topic))
            self._last_enqueued_at_unix = received_at_unix
            self._write_locked()

    def persisted(self, *, received_at_unix: float, topic: str) -> None:
        with self._lock:
            self._remove_pending_locked(received_at_unix, topic)
            self._last_persisted_received_at_unix = received_at_unix
            self._write_locked()

    def dropped(
        self, *, received_at_unix: float, topic: str, reason: str
    ) -> None:
        """Remove a reservation and record a device-scoped loss event."""

        with self._lock:
            self._remove_pending_locked(received_at_unix, topic)
            device_id, message_type = _topic_identity(topic)
            self._dropped_message_count += 1
            self._loss_events.append(
                {
                    "received_at_unix": received_at_unix,
                    "received_at_utc": _utc_from_unix(received_at_unix),
                    "device_id": device_id,
                    "message_type": message_type,
                    "reason": reason,
                }
            )
            self._write_locked()

    def stop(self) -> None:
        with self._lock:
            self._accepting_messages = False
            self._write_locked()

    def _remove_pending_locked(self, received_at_unix: float, topic: str) -> None:
        expected = (received_at_unix, topic)
        if self._pending and self._pending[0] == expected:
            self._pending.popleft()
            return
        # Queue-full rejection removes the newest reservation, while a storage
        # error normally removes the oldest.  Searching keeps the state correct
        # for either concurrent ordering without silently underflowing.
        try:
            self._pending.remove(expected)
        except ValueError as exc:
            raise RuntimeError("collector runtime-state reservation is missing") from exc

    def _mapping_locked(self) -> dict[str, Any]:
        now = time.time()
        return {
            "schema_version": STATE_SCHEMA_VERSION,
            "started_at_utc": _utc_from_unix(self._started_at_unix),
            "updated_at_utc": _utc_from_unix(now),
            "accepting_messages": self._accepting_messages,
            "queue_depth": len(self._pending),
            "oldest_pending_received_at_unix": (
                None if not self._pending else min(item[0] for item in self._pending)
            ),
            "last_enqueued_at_unix": self._last_enqueued_at_unix,
            "last_persisted_received_at_unix": self._last_persisted_received_at_unix,
            "dropped_message_count": self._dropped_message_count,
            "loss_events": self._loss_events,
        }

    def _write_locked(self) -> None:
        temporary = self.path.with_name(
            f".{self.path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        temporary.write_text(
            json.dumps(self._mapping_locked(), sort_keys=True, separators=(",", ":"))
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, self.path)
