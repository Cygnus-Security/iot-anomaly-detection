"""Deterministic CSV exports from completed, run-isolated SQLite databases."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from src.experiments.artifacts import PHASE_COLUMNS, write_csv_atomic
from src.experiments.planning import RunPlanEntry, load_run_plan, phase_schedule
from src.pipeline.common import (
    connect_read_only,
    find_run_database,
    load_json_object,
    query_records,
    table_columns,
    table_exists,
)

PREDICTION_COLUMNS = (
    "window_id",
    "run_id",
    "collection_session_id",
    "device_id",
    "window_end_utc",
    "prediction_recorded_at_utc",
    "inference_call_id",
    "score",
    "threshold",
    "predicted_label",
    "inference_status",
    "model_version",
    "feature_schema_version",
    "inference_duration_ms",
    "window_processing_delay_ms",
)

TIMING_COLUMNS = (
    "run_id",
    "collection_session_id",
    "model_version",
    "inference_call_id",
    "window_ids_json",
    "batch_size",
    "inference_duration_ms",
    "measured_at_utc",
)


def _export_table(
    connection: Any,
    table: str,
    destination: Path,
    *,
    order_by: str,
) -> int:
    columns = table_columns(connection, table)
    if not columns:
        raise ValueError(f"required table is missing: {table}")
    rows = query_records(
        connection, f'SELECT * FROM "{table}" ORDER BY {order_by}'
    )
    write_csv_atomic(destination, columns, rows)
    return len(rows)


def _prediction_rows(connection: Any) -> list[dict[str, Any]]:
    if not table_exists(connection, "window_predictions"):
        return []
    rows = query_records(
        connection,
        "SELECT * FROM window_predictions ORDER BY run_id, device_id, window_end_utc",
    )
    call_durations = {
        str(row["inference_call_id"]): row["inference_duration_ms"]
        for row in (
            query_records(connection, "SELECT * FROM inference_calls")
            if table_exists(connection, "inference_calls")
            else []
        )
    }
    return [
        {
            "window_id": row["window_id"],
            "run_id": row["run_id"],
            "collection_session_id": row["collection_session_id"],
            "device_id": row["device_id"],
            "window_end_utc": row["window_end_utc"],
            "prediction_recorded_at_utc": row.get("predicted_at_utc"),
            "inference_call_id": row.get("inference_call_id", ""),
            "score": row.get("anomaly_score"),
            "threshold": row.get("decision_threshold"),
            "predicted_label": row.get("predicted_label"),
            "inference_status": row.get("inference_status", "scored"),
            "model_version": row.get("model_version"),
            "feature_schema_version": row.get("feature_schema_version"),
            "inference_duration_ms": call_durations.get(
                str(row.get("inference_call_id", "")), ""
            ),
            "window_processing_delay_ms": row.get("processing_delay_ms"),
        }
        for row in rows
    ]


def _timing_rows(connection: Any) -> list[dict[str, Any]]:
    table = next(
        (
            name
            for name in ("inference_calls", "analytics_inference_calls")
            if table_exists(connection, name)
        ),
        None,
    )
    if table is None:
        return []
    rows = query_records(
        connection,
        f'SELECT * FROM "{table}" ORDER BY measured_at_utc, inference_call_id',
    )
    return [{name: row.get(name, "") for name in TIMING_COLUMNS} for row in rows]


def _phase_rows(
    connection: Any, entry: RunPlanEntry, manifest: dict[str, Any]
) -> list[dict[str, Any]]:
    rows = []
    started = ended = None
    if table_exists(connection, "scenario_events") and entry.target_device:
        events = query_records(
            connection,
            """
            SELECT scenario_run_id, event_type, event_at
            FROM scenario_events
            WHERE device_id = ? AND scenario = ?
            ORDER BY event_at, id
            """,
            (entry.target_device, entry.scenario),
        )
        starts = [row for row in events if row["event_type"] == "scenario_started"]
        ends = [row for row in events if row["event_type"] == "scenario_ended"]
        if (
            len(starts) == 1
            and len(ends) == 1
            and starts[0].get("scenario_run_id") == ends[0].get("scenario_run_id")
        ):
            started = starts[0]["event_at"]
            ended = ends[0]["event_at"]
    run_start = manifest.get("run_start_utc")
    run_end = manifest.get("run_end_utc")
    for phase in phase_schedule(entry):
        row = phase.to_mapping()
        if entry.scenario == "normal":
            actual_start, actual_end = run_start, run_end
        elif phase.phase_name == "baseline_before":
            actual_start, actual_end = run_start, started
        elif phase.phase_name == "anomaly":
            actual_start, actual_end = started, ended
        else:
            actual_start, actual_end = ended, run_end
        row.update(
            {
                "actual_start_utc": actual_start or "",
                "actual_end_utc": actual_end or "",
                "status": "completed" if actual_start and actual_end else "incomplete",
            }
        )
        rows.append(row)
    return rows


def export_run(results_directory: str | Path, run_id: str) -> dict[str, int]:
    """Export one completed run without copying a live SQLite file."""

    results_root = Path(results_directory).resolve()
    entries = {entry.run_id: entry for entry in load_run_plan(results_root / "run_plan.csv")}
    if run_id not in entries:
        raise ValueError(f"run ID is not present in the frozen plan: {run_id}")
    entry = entries[run_id]
    run_directory = results_root / "runs" / run_id
    manifest = load_json_object(run_directory / "run_manifest.json")
    if manifest.get("run_id") != run_id:
        raise ValueError(f"run manifest identity mismatch for {run_id}")
    if manifest.get("status") != "completed":
        raise ValueError(f"run is not completed: {run_id}")
    database = find_run_database(run_directory)
    counts: dict[str, int] = {}
    with connect_read_only(database) as connection:
        counts["messages"] = _export_table(
            connection,
            "mqtt_observations",
            run_directory / "messages.csv",
            order_by="received_at_unix, id",
        )
        counts["feature_windows"] = _export_table(
            connection,
            "feature_windows",
            run_directory / "feature_windows.csv",
            order_by="device_id, window_index",
        )
        predictions = _prediction_rows(connection)
        write_csv_atomic(
            run_directory / "predictions.csv", PREDICTION_COLUMNS, predictions
        )
        counts["predictions"] = len(predictions)

        alert_columns = table_columns(connection, "anomaly_alerts")
        alerts = (
            query_records(connection, "SELECT * FROM anomaly_alerts ORDER BY detected_at_utc, id")
            if alert_columns
            else []
        )
        write_csv_atomic(
            run_directory / "alerts.csv",
            alert_columns
            or (
                "id",
                "window_id",
                "run_id",
                "collection_session_id",
                "detected_at_utc",
                "device_id",
                "anomaly_score",
                "decision_threshold",
                "model_version",
                "reason",
            ),
            alerts,
        )
        counts["alerts"] = len(alerts)

        timing = _timing_rows(connection)
        write_csv_atomic(run_directory / "timing_samples.csv", TIMING_COLUMNS, timing)
        counts["timing_samples"] = len(timing)
        phases = _phase_rows(connection, entry, manifest)
        write_csv_atomic(run_directory / "scenario_phases.csv", PHASE_COLUMNS, phases)
        counts["scenario_phases"] = len(phases)
    return counts


def export_campaign(results_directory: str | Path) -> dict[str, dict[str, int]]:
    root = Path(results_directory).resolve()
    return {
        entry.run_id: export_run(root, entry.run_id)
        for entry in load_run_plan(root / "run_plan.csv")
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--run-id", help="export only one run")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    summary = (
        {arguments.run_id: export_run(arguments.results_dir, arguments.run_id)}
        if arguments.run_id
        else export_campaign(arguments.results_dir)
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
