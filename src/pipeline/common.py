"""Shared, read-only access helpers for campaign result directories."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


def parse_utc(value: str) -> datetime:
    """Parse a timezone-qualified timestamp and normalize it to UTC."""

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must contain a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json_object(path: str | Path) -> dict[str, Any]:
    decoded = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return decoded


def find_run_database(run_directory: str | Path) -> Path:
    """Resolve the single run-isolated SQLite database without broad guessing."""

    directory = Path(run_directory)
    candidates = (
        directory / "run.db",
        directory / "iot_anomaly.db",
        directory / "runtime" / "iot_anomaly.db",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    discovered = sorted(directory.glob("*.db"))
    if len(discovered) == 1:
        return discovered[0]
    if not discovered:
        raise FileNotFoundError(f"no run database found in {directory}")
    raise ValueError(
        f"multiple run databases found in {directory}: "
        + ", ".join(path.name for path in discovered)
    )


def connect_read_only(path: str | Path) -> sqlite3.Connection:
    database = Path(path).resolve()
    connection = sqlite3.connect(
        f"file:{database}?mode=ro", uri=True, timeout=30.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def table_columns(connection: sqlite3.Connection, table: str) -> tuple[str, ...]:
    if not table_exists(connection, table):
        return ()
    return tuple(
        str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')
    )


def query_records(
    connection: sqlite3.Connection,
    query: str,
    parameters: Sequence[object] = (),
) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(query, tuple(parameters))]


def percentile(values: Iterable[float], quantile: float) -> float | None:
    """Return a linearly interpolated sample quantile, or None for no samples."""

    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    if not 0 <= quantile <= 1:
        raise ValueError("quantile must be in [0, 1]")
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def validate_database_identity(
    rows: Iterable[Mapping[str, Any]], expected_run_id: str
) -> None:
    observed = {str(row["run_id"]) for row in rows if row.get("run_id") is not None}
    if observed and observed != {expected_run_id}:
        raise ValueError(
            f"database identity mismatch for {expected_run_id}: {sorted(observed)}"
        )
