"""CLI for creating a non-overwriting Chapter 4 campaign result tree."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from src.experiments.artifacts import initialize_campaign


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prepare the frozen 13-run campaign under an empty results folder."
    )
    parser.add_argument("--plan", default="experiments/run_plan.csv")
    parser.add_argument("--split", default="experiments/split_manifest.json")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--repository-root", default=".")
    parser.add_argument(
        "--effective-config-json",
        help="Optional JSON configuration to redact and record in environment.json.",
    )
    args = parser.parse_args(argv)

    effective_config: dict[str, object] = {}
    if args.effective_config_json:
        with Path(args.effective_config_json).open(encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            parser.error("--effective-config-json must contain a JSON object")
        effective_config = value

    root = initialize_campaign(
        plan_path=args.plan,
        split_path=args.split,
        results_root=args.results_dir,
        repository_root=args.repository_root,
        effective_config=effective_config,
    )
    print(f"Prepared campaign results tree: {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
