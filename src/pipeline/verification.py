"""Read-only replay verification for the online Chapter 4 feature/ML path."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from src.experiments.artifacts import write_json_atomic
from src.experiments.planning import RunPlanEntry, load_run_plan
from src.features import (
    FEATURE_NAMES,
    FEATURE_SCHEMA_VERSION,
    ObservationGap,
    ScenarioPhase,
    TelemetryObservation,
    build_scheduled_windows,
)
from src.ml import load_artifact, score_feature_rows
from src.pipeline.common import (
    connect_read_only,
    find_run_database,
    load_json_object,
    parse_utc,
    query_records,
    table_exists,
)

EXACT_FEATURE_FIELDS = (
    "run_id",
    "collection_session_id",
    "device_id",
    "window_id",
    "window_index",
    "feature_schema_version",
    "observed_message_count",
    "feature_status",
    "exclusion_reason",
    "message_count",
    "peak_messages_per_second",
    "ground_truth_label",
    "phase_id",
    "is_target_device",
)
FLOAT_FEATURE_FIELDS = (
    "window_start_unix",
    "window_end_unix",
    "message_rate_hz",
    "payload_mean_bytes",
    "payload_std_bytes",
    "interarrival_mean_ms",
    "interarrival_std_ms",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _same_float(expected: object, actual: object, tolerance: float) -> bool:
    if expected is None or actual is None:
        return expected is None and actual is None
    try:
        left = float(expected)
        right = float(actual)
    except (TypeError, ValueError):
        return False
    return math.isfinite(left) and math.isfinite(right) and math.isclose(
        left, right, rel_tol=tolerance, abs_tol=tolerance
    )


def _scenario_phase(
    connection: Any,
    entry: RunPlanEntry,
    *,
    run_start_unix: float,
    run_end_unix: float,
) -> tuple[ScenarioPhase | None, bool]:
    if entry.scenario == "normal":
        return None, True
    if not table_exists(connection, "scenario_events"):
        return None, False
    rows = query_records(
        connection,
        """
        SELECT scenario_run_id, device_id, event_type, event_at
        FROM scenario_events
        WHERE scenario = ? AND device_id = ?
        ORDER BY event_at, id
        """,
        (entry.scenario, entry.target_device),
    )
    run_ids = {str(row["scenario_run_id"]) for row in rows}
    if len(run_ids) != 1:
        return None, False
    scenario_run_id = next(iter(run_ids))
    starts = [row for row in rows if row["event_type"] == "scenario_started"]
    ends = [row for row in rows if row["event_type"] == "scenario_ended"]
    if len(starts) != 1 or len(ends) != 1:
        return None, False
    start = parse_utc(str(starts[0]["event_at"])).timestamp()
    end = parse_utc(str(ends[0]["event_at"])).timestamp()
    if not run_start_unix <= start < end <= run_end_unix:
        return None, False
    return (
        ScenarioPhase(
            phase_id=scenario_run_id,
            target_device_id=str(starts[0]["device_id"]),
            start_unix=start,
            end_unix=end,
        ),
        True,
    )


def _observation_gaps(
    database: Path, *, run_start_unix: float, run_end_unix: float
) -> dict[str, tuple[ObservationGap, ...]]:
    """Recreate online gap inputs from the collector's persisted loss sidecar."""

    from src.collector.runtime_state import load_runtime_state, runtime_state_path

    state = load_runtime_state(runtime_state_path(database))
    if state is None:
        return {}
    grouped: dict[str, list[ObservationGap]] = {}
    for event in state.loss_events:
        try:
            received_at_unix = float(event["received_at_unix"])
        except (KeyError, TypeError, ValueError):
            continue
        if (
            event.get("message_type") != "telemetry"
            or not run_start_unix <= received_at_unix < run_end_unix
        ):
            continue
        device_id = str(event.get("device_id") or "")
        if not device_id:
            continue
        reason = str(event.get("reason") or "collector_message_loss")
        grouped.setdefault(device_id, []).append(
            ObservationGap(
                start_unix=received_at_unix,
                end_unix=received_at_unix + 1e-6,
                reason=f"collector_{reason}",
            )
        )
    return {key: tuple(value) for key, value in grouped.items()}


def _append_mismatch(
    mismatches: list[dict[str, Any]],
    *,
    category: str,
    identifier: str,
    field: str,
    expected: object,
    actual: object,
    maximum: int,
) -> None:
    if len(mismatches) < maximum:
        mismatches.append(
            {
                "category": category,
                "identifier": identifier,
                "field": field,
                "expected": expected,
                "actual": actual,
            }
        )


def _safe_output(results_root: Path, output_path: str | Path | None, run_id: str) -> Path:
    destination = (
        results_root / "evidence" / f"t10-replay-{run_id}.json"
        if output_path is None
        else Path(output_path)
    )
    if not destination.is_absolute():
        destination = results_root / destination
    destination = destination.resolve()
    try:
        destination.relative_to(results_root.resolve())
    except ValueError as exc:
        raise ValueError("verification output must remain inside the results directory") from exc
    return destination


def verify_run_replay(
    results_directory: str | Path,
    run_id: str,
    *,
    float_tolerance: float = 1e-9,
    output_path: str | Path | None = None,
    max_mismatch_examples: int = 200,
) -> dict[str, Any]:
    """Rebuild features and predictions from raw evidence and compare every row."""

    if not math.isfinite(float_tolerance) or float_tolerance < 0:
        raise ValueError("float_tolerance must be finite and non-negative")
    if max_mismatch_examples < 1:
        raise ValueError("max_mismatch_examples must be positive")
    results_root = Path(results_directory).resolve()
    entries = {entry.run_id: entry for entry in load_run_plan(results_root / "run_plan.csv")}
    entry = entries.get(run_id)
    if entry is None:
        raise ValueError(f"run ID is not present in the frozen plan: {run_id}")
    if entry.split != "test":
        raise ValueError("replay prediction verification requires a test-split run")
    run_directory = results_root / "runs" / run_id
    manifest = load_json_object(run_directory / "run_manifest.json")
    if manifest.get("run_id") != run_id:
        raise ValueError("run manifest identity does not match the requested run")
    if manifest.get("status") != "completed":
        raise ValueError("run must be completed before replay verification")
    collection_session_id = str(manifest.get("collection_session_id") or "")
    if not collection_session_id:
        raise ValueError("run manifest is missing collection_session_id")
    run_start_text = str(manifest.get("run_start_utc") or "")
    if not run_start_text:
        raise ValueError("run manifest is missing run_start_utc")
    run_start_unix = parse_utc(run_start_text).timestamp()
    duration = float(manifest.get("effective_duration_seconds", entry.duration_seconds))
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError("effective run duration must be finite and positive")
    run_end_unix = run_start_unix + duration

    database = find_run_database(run_directory)
    with connect_read_only(database) as connection:
        for required_table in (
            "mqtt_observations",
            "feature_windows",
            "window_predictions",
        ):
            if not table_exists(connection, required_table):
                raise ValueError(f"required table is missing: {required_table}")
        raw_rows = query_records(
            connection,
            """
            SELECT id, device_id, received_at_unix, payload_size_bytes,
                   session_id, message_type, payload_valid, duplicate, retain
            FROM mqtt_observations
            WHERE received_at_unix >= ? AND received_at_unix < ?
            ORDER BY received_at_unix, id
            """,
            (run_start_unix, run_end_unix),
        )
        observations = [
            TelemetryObservation(
                observation_id=int(row["id"]),
                device_id=str(row["device_id"]),
                received_at_unix=float(row["received_at_unix"]),
                payload_size_bytes=int(row["payload_size_bytes"]),
                session_id=None if row["session_id"] is None else str(row["session_id"]),
                message_type=str(row["message_type"]),
                payload_valid=bool(row["payload_valid"]),
                duplicate=bool(row["duplicate"]),
                retain=bool(row["retain"]),
            )
            for row in raw_rows
        ]
        phase, phase_complete = _scenario_phase(
            connection,
            entry,
            run_start_unix=run_start_unix,
            run_end_unix=run_end_unix,
        )
        persisted_features = {
            str(row["window_id"]): row
            for row in query_records(connection, "SELECT * FROM feature_windows")
        }
        persisted_predictions = {
            str(row["window_id"]): row
            for row in query_records(connection, "SELECT * FROM window_predictions")
        }

    gaps = _observation_gaps(
        database, run_start_unix=run_start_unix, run_end_unix=run_end_unix
    )
    rebuilt_windows = build_scheduled_windows(
        run_id=run_id,
        collection_session_id=collection_session_id,
        device_ids=tuple(f"iot-{index:03d}" for index in range(1, entry.device_count + 1)),
        run_start_unix=run_start_unix,
        run_end_unix=run_end_unix,
        observations=observations,
        scenario_phase=phase,
        phase_log_complete=phase_complete,
        observation_gaps=gaps,
    )
    rebuilt_features = {window.window_id: window.as_record() for window in rebuilt_windows}
    scoreable_windows = [window for window in rebuilt_windows if window.can_score]
    artifact = load_artifact(results_root / "artifacts", trusted=True)
    offline_scores = (
        score_feature_rows(
            artifact,
            [window.features.as_dict() for window in scoreable_windows if window.features],
        )
        if scoreable_windows
        else []
    )
    expected_predictions = dict(
        zip(
            (window.window_id for window in scoreable_windows),
            offline_scores,
            strict=True,
        )
    )

    mismatches: list[dict[str, Any]] = []
    mismatch_counts = {
        "missing_feature_rows": 0,
        "extra_feature_rows": 0,
        "feature_field_mismatches": 0,
        "missing_prediction_rows": 0,
        "extra_prediction_rows": 0,
        "prediction_field_mismatches": 0,
    }
    missing_features = sorted(set(rebuilt_features) - set(persisted_features))
    extra_features = sorted(set(persisted_features) - set(rebuilt_features))
    mismatch_counts["missing_feature_rows"] = len(missing_features)
    mismatch_counts["extra_feature_rows"] = len(extra_features)
    for identifier, expected, actual in (
        *((identifier, "present", "missing") for identifier in missing_features),
        *((identifier, "absent", "present") for identifier in extra_features),
    ):
        _append_mismatch(
            mismatches,
            category="feature",
            identifier=identifier,
            field="row",
            expected=expected,
            actual=actual,
            maximum=max_mismatch_examples,
        )

    for identifier in sorted(set(rebuilt_features) & set(persisted_features)):
        expected = rebuilt_features[identifier]
        actual = persisted_features[identifier]
        for field in EXACT_FEATURE_FIELDS:
            left, right = expected.get(field), actual.get(field)
            if field == "is_target_device":
                left, right = bool(left), bool(right)
            if left != right:
                mismatch_counts["feature_field_mismatches"] += 1
                _append_mismatch(
                    mismatches,
                    category="feature",
                    identifier=identifier,
                    field=field,
                    expected=left,
                    actual=right,
                    maximum=max_mismatch_examples,
                )
        for field in FLOAT_FEATURE_FIELDS:
            left, right = expected.get(field), actual.get(field)
            if not _same_float(left, right, float_tolerance):
                mismatch_counts["feature_field_mismatches"] += 1
                _append_mismatch(
                    mismatches,
                    category="feature",
                    identifier=identifier,
                    field=field,
                    expected=left,
                    actual=right,
                    maximum=max_mismatch_examples,
                )

    missing_predictions = sorted(set(expected_predictions) - set(persisted_predictions))
    extra_predictions = sorted(set(persisted_predictions) - set(expected_predictions))
    mismatch_counts["missing_prediction_rows"] = len(missing_predictions)
    mismatch_counts["extra_prediction_rows"] = len(extra_predictions)
    for identifier, expected, actual in (
        *((identifier, "present", "missing") for identifier in missing_predictions),
        *((identifier, "absent", "present") for identifier in extra_predictions),
    ):
        _append_mismatch(
            mismatches,
            category="prediction",
            identifier=identifier,
            field="row",
            expected=expected,
            actual=actual,
            maximum=max_mismatch_examples,
        )

    rebuilt_by_id = {window.window_id: window for window in scoreable_windows}
    for identifier in sorted(set(expected_predictions) & set(persisted_predictions)):
        expected_score = expected_predictions[identifier]
        actual = persisted_predictions[identifier]
        window = rebuilt_by_id[identifier]
        for field, expected_value in (
            ("run_id", run_id),
            ("collection_session_id", collection_session_id),
            ("device_id", window.device_id),
            ("predicted_label", expected_score.predicted_label),
            ("model_version", artifact.metadata.model_version),
            ("feature_schema_version", FEATURE_SCHEMA_VERSION),
            ("inference_status", "scored"),
        ):
            if actual.get(field) != expected_value:
                mismatch_counts["prediction_field_mismatches"] += 1
                _append_mismatch(
                    mismatches,
                    category="prediction",
                    identifier=identifier,
                    field=field,
                    expected=expected_value,
                    actual=actual.get(field),
                    maximum=max_mismatch_examples,
                )
        for field, expected_value in (
            ("anomaly_score", expected_score.score),
            ("decision_threshold", expected_score.threshold),
        ):
            if not _same_float(expected_value, actual.get(field), float_tolerance):
                mismatch_counts["prediction_field_mismatches"] += 1
                _append_mismatch(
                    mismatches,
                    category="prediction",
                    identifier=identifier,
                    field=field,
                    expected=expected_value,
                    actual=actual.get(field),
                    maximum=max_mismatch_examples,
                )

    mismatch_total = sum(mismatch_counts.values())
    report = {
        "schema_version": "1.0",
        "verified_at_utc": _utc_now(),
        "status": "pass" if mismatch_total == 0 else "fail",
        "run_id": run_id,
        "collection_session_id": collection_session_id,
        "model_version": artifact.metadata.model_version,
        "model_sha256": artifact.metadata.model_sha256,
        "float_tolerance": float_tolerance,
        "phase_log_complete": phase_complete,
        "counts": {
            "raw_rows_in_run": len(raw_rows),
            "scheduled_windows": len(rebuilt_windows),
            "persisted_feature_rows": len(persisted_features),
            "offline_scoreable_windows": len(expected_predictions),
            "persisted_prediction_rows": len(persisted_predictions),
        },
        "mismatch_count": mismatch_total,
        "mismatch_counts": mismatch_counts,
        "mismatch_examples": mismatches,
        "mismatch_examples_truncated": mismatch_total > len(mismatches),
    }
    destination = _safe_output(results_root, output_path, run_id)
    write_json_atomic(destination, report)
    report["evidence_path"] = destination.relative_to(results_root).as_posix()
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--float-tolerance", type=float, default=1e-9)
    parser.add_argument("--output")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        report = verify_run_replay(
            arguments.results_dir,
            arguments.run_id,
            float_tolerance=arguments.float_tolerance,
            output_path=arguments.output,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"status": "error", "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
