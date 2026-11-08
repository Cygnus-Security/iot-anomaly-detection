"""Print a concise, evidence-based snapshot of the Phase 2 database."""

from __future__ import annotations

import argparse
import json
import sqlite3
from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default="data/runtime/iot_anomaly.db")
    args = parser.parse_args(argv)
    connection = sqlite3.connect(f"file:{args.database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        summary = {
            "devices": connection.execute("SELECT COUNT(*) FROM devices").fetchone()[0],
            "online_devices": connection.execute(
                "SELECT COUNT(*) FROM devices WHERE is_online = 1"
            ).fetchone()[0],
            "observations": connection.execute(
                "SELECT COUNT(*) FROM mqtt_observations"
            ).fetchone()[0],
            "valid_telemetry": connection.execute(
                """SELECT COUNT(*) FROM mqtt_observations
                   WHERE message_type = 'telemetry' AND payload_valid = 1"""
            ).fetchone()[0],
            "invalid_payloads": connection.execute(
                "SELECT COUNT(*) FROM mqtt_observations WHERE payload_valid = 0"
            ).fetchone()[0],
            "scenario_events": connection.execute(
                "SELECT COUNT(*) FROM scenario_events"
            ).fetchone()[0],
            "labels": {
                str(row["ground_truth_label"]): int(row["count"])
                for row in connection.execute(
                    """SELECT ground_truth_label, COUNT(*) AS count
                       FROM mqtt_observations
                       WHERE message_type = 'telemetry'
                       GROUP BY ground_truth_label ORDER BY ground_truth_label"""
                )
            },
        }
    finally:
        connection.close()
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
