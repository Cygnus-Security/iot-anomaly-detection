"""Export unmodified Phase 2 SQLite tables to separate CSV files."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Sequence

TABLES = ("mqtt_observations", "devices", "scenario_events")


def export_table(connection: sqlite3.Connection, table: str, output: Path) -> int:
    cursor = connection.execute(f"SELECT * FROM {table} ORDER BY 1")
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(column[0] for column in cursor.description)
        count = 0
        for row in cursor:
            writer.writerow(row)
            count += 1
    return count


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export raw Phase 2 tables to CSV.")
    parser.add_argument("--database", default="data/runtime/iot_anomaly.db")
    parser.add_argument("--output-dir", default="data/exports")
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(f"file:{args.database}?mode=ro", uri=True)
    try:
        for table in TABLES:
            count = export_table(connection, table, output_dir / f"{table}.csv")
            print(f"{table}: {count} rows")
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
