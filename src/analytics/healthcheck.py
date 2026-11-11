"""Health check for the analytics tables and optional ready marker."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path


def main() -> int:
    database = os.environ.get("SQLITE_PATH")
    if not database or not Path(database).is_file():
        return 1
    ready = os.environ.get("ANALYTICS_READY_FILE")
    if ready:
        path = Path(ready)
        if not path.is_file():
            return 1
        try:
            if json.loads(path.read_text(encoding="utf-8")).get("ready") is not True:
                return 1
        except (OSError, json.JSONDecodeError, AttributeError):
            return 1
    try:
        with sqlite3.connect(database, timeout=5.0) as connection:
            names = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type = 'table' AND name IN (
                        'feature_windows', 'inference_calls',
                        'window_predictions', 'anomaly_alerts'
                    )
                    """
                )
            }
    except sqlite3.Error:
        return 1
    return 0 if names == {
        "feature_windows",
        "inference_calls",
        "window_predictions",
        "anomaly_alerts",
    } else 1


if __name__ == "__main__":
    raise SystemExit(main())
