"""Read-only collector readiness check used by Docker Compose."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    args = parser.parse_args(argv)
    if not Path(args.database).is_file():
        return 1
    try:
        connection = sqlite3.connect(f"file:{args.database}?mode=ro", uri=True, timeout=1)
        row = connection.execute(
            "SELECT schema_version FROM schema_metadata LIMIT 1"
        ).fetchone()
        connection.close()
    except sqlite3.Error:
        return 1
    return 0 if row == (1,) else 1


if __name__ == "__main__":
    raise SystemExit(main())
