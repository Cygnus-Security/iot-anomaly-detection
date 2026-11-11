"""Normal-only Isolation Forest training and validation-threshold calibration."""

from __future__ import annotations

import argparse
import json
import math
import shutil
import time
from collections import Counter
from pathlib import Path
from typing import Any, Sequence

from src.experiments.artifacts import (
    expected_window_count,
    write_csv_atomic,
    write_json_atomic,
)
from src.experiments.planning import (
    load_run_plan,
    load_split_manifest,
    validate_split_manifest,
)
from src.ml import (
    FEATURE_NAMES,
    fit_isolation_forest,
    load_artifact,
    save_artifact,
    score_feature_rows,
)
from src.pipeline.common import (
    connect_read_only,
    find_run_database,
    load_json_object,
    query_records,
    sha256_file,
    table_exists,
    validate_database_identity,
)

FEATURE_EXPORT_COLUMNS = (
    "run_id",
    "collection_session_id",
    "device_id",
    "window_id",
    "window_start_utc",
    "window_end_utc",
    "feature_schema_version",
    "feature_status",
    "ground_truth_label",
    *FEATURE_NAMES,
)


def _normal_feature_rows(
    results_root: Path, entries: Sequence[Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    output: list[dict[str, Any]] = []
    quality: list[dict[str, Any]] = []
    for entry in entries:
        run_id = str(entry.run_id)
        run_directory = results_root / "runs" / run_id
        manifest = load_json_object(run_directory / "run_manifest.json")
        if manifest.get("run_id") != run_id or manifest.get("status") != "completed":
            raise ValueError(f"training input run is not completed: {run_id}")
        expected_session = str(manifest.get("collection_session_id", ""))
        if not expected_session:
            raise ValueError(f"collection session is missing for {run_id}")
        database = find_run_database(run_directory)
        with connect_read_only(database) as connection:
            if not table_exists(connection, "feature_windows"):
                raise ValueError(f"feature_windows is missing for {run_id}")
            all_rows = query_records(
                connection,
                """
                SELECT run_id, collection_session_id, device_id, window_id,
                       window_start_utc, window_end_utc, feature_schema_version,
                       feature_status, ground_truth_label, message_count, message_rate_hz,
                       payload_mean_bytes, payload_std_bytes,
                       interarrival_mean_ms, interarrival_std_ms,
                       peak_messages_per_second
                FROM feature_windows
                ORDER BY run_id, device_id, window_index
                """,
            )
        validate_database_identity(all_rows, run_id)
        expected = expected_window_count(
            entry.duration_seconds, device_count=entry.device_count
        )
        if len(all_rows) != expected:
            raise ValueError(
                f"scheduled feature coverage mismatch for {run_id}: "
                f"expected {expected}, found {len(all_rows)}"
            )
        schemas = {str(row["feature_schema_version"]) for row in all_rows}
        if schemas != {"1.0"}:
            raise ValueError(f"unsupported feature schema for {run_id}: {sorted(schemas)}")
        sessions = {str(row["collection_session_id"]) for row in all_rows}
        if sessions != {expected_session}:
            raise ValueError(
                f"collection session mismatch for {run_id}: {sorted(sessions)}"
            )
        rows = [
            row
            for row in all_rows
            if row["feature_status"] == "ok"
            and row["ground_truth_label"] == "normal"
        ]
        if not rows:
            raise ValueError(f"no usable normal feature windows for {run_id}")
        statuses = Counter(str(row["feature_status"]) for row in all_rows)
        quality.append(
            {
                "run_id": run_id,
                "scheduled_windows": expected,
                "usable_normal_windows": len(rows),
                "feature_coverage": len(rows) / expected,
                "ok_windows": statuses["ok"],
                "insufficient_data_windows": statuses["insufficient_data"],
                "observation_gap_windows": statuses["observation_gap"],
                "invalid_feature_windows": statuses["invalid_feature"],
            }
        )
        output.extend(rows)
    return output, quality


def train_campaign_model(
    results_directory: str | Path,
    *,
    model_version: str = "iforest-ch4-v1",
    force: bool = False,
) -> dict[str, Any]:
    """Fit train runs, calibrate on validation, and freeze a trusted artifact."""

    results_root = Path(results_directory).resolve()
    plan_path = results_root / "run_plan.csv"
    split_path = results_root / "split_manifest.json"
    entries = load_run_plan(plan_path)
    splits = load_split_manifest(split_path)
    validate_split_manifest(splits, entries)

    artifact_directory = results_root / "artifacts"
    protected_names = (
        "model.joblib",
        "model_metadata.json",
        "artifact_manifest.json",
    )
    if not force and any(
        (artifact_directory / name).exists() for name in protected_names
    ):
        raise FileExistsError(
            f"frozen model already exists in {artifact_directory}; refusing to overwrite"
        )
    artifact_directory.mkdir(parents=True, exist_ok=True)

    entries_by_id = {entry.run_id: entry for entry in entries}
    training_rows, training_quality = _normal_feature_rows(
        results_root, [entries_by_id[run_id] for run_id in splits["train"]]
    )
    validation_rows, validation_quality = _normal_feature_rows(
        results_root, [entries_by_id[run_id] for run_id in splits["validation"]]
    )
    write_csv_atomic(
        artifact_directory / "training_features.csv",
        FEATURE_EXPORT_COLUMNS,
        training_rows,
    )
    write_csv_atomic(
        artifact_directory / "validation_features.csv",
        FEATURE_EXPORT_COLUMNS,
        validation_rows,
    )
    quality_columns = (
        "run_id",
        "scheduled_windows",
        "usable_normal_windows",
        "feature_coverage",
        "ok_windows",
        "insufficient_data_windows",
        "observation_gap_windows",
        "invalid_feature_windows",
    )
    write_csv_atomic(
        artifact_directory / "training_data_quality.csv",
        quality_columns,
        training_quality + validation_quality,
    )

    fit_started = time.perf_counter()
    artifact = fit_isolation_forest(
        training_rows,
        validation_rows,
        model_version=model_version,
        train_run_ids=splits["train"],
        validation_run_ids=splits["validation"],
    )
    fit_seconds = time.perf_counter() - fit_started
    save_started = time.perf_counter()
    model_path, metadata_path = save_artifact(artifact, artifact_directory)
    save_seconds = time.perf_counter() - save_started

    reloaded = load_artifact(artifact_directory, trusted=True)
    consistency_rows = validation_rows[: min(32, len(validation_rows))]
    in_memory_scores = score_feature_rows(artifact, consistency_rows)
    reloaded_scores = score_feature_rows(reloaded, consistency_rows)
    if len(in_memory_scores) != len(reloaded_scores) or any(
        left.predicted_label != right.predicted_label
        or not math.isclose(left.score, right.score, rel_tol=0.0, abs_tol=1e-12)
        or not math.isclose(left.threshold, right.threshold, rel_tol=0.0, abs_tol=1e-12)
        for left, right in zip(in_memory_scores, reloaded_scores, strict=True)
    ):
        raise RuntimeError("reloaded model does not reproduce validation scores")

    frozen_split_path = artifact_directory / "split_manifest.json"
    shutil.copyfile(split_path, frozen_split_path)
    manifest = {
        "schema_version": "1.0",
        "model_version": artifact.metadata.model_version,
        "train_run_ids": list(splits["train"]),
        "validation_run_ids": list(splits["validation"]),
        "test_run_ids": list(splits["test"]),
        "training_row_count": len(training_rows),
        "validation_row_count": len(validation_rows),
        "fit_duration_seconds": fit_seconds,
        "artifact_save_duration_seconds": save_seconds,
        "fit_and_save_duration_seconds": fit_seconds + save_seconds,
        "threshold": artifact.metadata.threshold,
        "threshold_quantile": artifact.metadata.threshold_quantile,
        "reload_score_consistency": True,
        "reload_score_consistency_rows": len(consistency_rows),
        "files": {
            "model.joblib": sha256_file(model_path),
            "model_metadata.json": sha256_file(metadata_path),
            "split_manifest.json": sha256_file(frozen_split_path),
            "training_features.csv": sha256_file(
                artifact_directory / "training_features.csv"
            ),
            "validation_features.csv": sha256_file(
                artifact_directory / "validation_features.csv"
            ),
            "training_data_quality.csv": sha256_file(
                artifact_directory / "training_data_quality.csv"
            ),
        },
    }
    write_json_atomic(artifact_directory / "artifact_manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--model-version", default="iforest-ch4-v1")
    parser.add_argument("--force", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    summary = train_campaign_model(
        arguments.results_dir,
        model_version=arguments.model_version,
        force=arguments.force,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
