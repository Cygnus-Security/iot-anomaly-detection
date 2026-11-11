#!/usr/bin/env python3
"""Capture a redacted, reproducible lab environment record as JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from src.experiments.artifacts import capture_environment, write_json_atomic


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="results/environment.json")
    parser.add_argument("--repository-root", default=".")
    parser.add_argument("--effective-config-json")
    args = parser.parse_args(argv)

    config: dict[str, object] = {}
    if args.effective_config_json:
        with Path(args.effective_config_json).open(encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            parser.error("--effective-config-json must contain a JSON object")
        config = value

    environment = capture_environment(
        repository_root=args.repository_root,
        effective_config=config,
    )
    destination = write_json_atomic(args.output, environment)
    print(f"Wrote redacted environment record: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
