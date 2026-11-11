"""Auditable scheduled-window evaluation for the frozen Chapter 4 campaign."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from src.experiments.artifacts import write_csv_atomic, write_json_atomic
from src.experiments.planning import RunPlanEntry, load_run_plan, load_split_manifest
from src.features import FEATURE_NAMES, ScenarioPhase, classify_ground_truth
from src.ml import evaluate_event_detection
from src.pipeline.common import (
    connect_read_only,
    find_run_database,
    format_utc,
    load_json_object,
    parse_utc,
    percentile,
    query_records,
    table_exists,
)

SCENARIO_CODES = {
    "normal": "K1",
    "message_burst": "K2",
    "abnormal_interval": "K3",
    "abnormal_payload": "K4",
}

EVALUATION_COLUMNS = (
    "split",
    "scenario",
    "scenario_code",
    "run_id",
    "collection_session_id",
    "device_id",
    "window_id",
    "window_start_utc",
    "window_end_utc",
    "model_version",
    "prediction_recorded_at_utc",
    "inference_call_id",
    "window_processing_delay_ms",
    "inference_duration_ms",
    "score",
    "threshold",
    "predicted_label",
    "inference_status",
    "ground_truth_label",
    "phase_id",
    "is_target_device",
    "is_control_device",
    "feature_status",
    "observed_message_count",
    *FEATURE_NAMES,
    "exclusion_reason",
    "included_in_primary_metrics",
)

METRIC_COLUMNS = (
    "scope",
    "run_id",
    "scenario_code",
    "scenario",
    "scheduled_windows",
    "valid_feature_windows",
    "scored_windows",
    "eligible_windows",
    "normal_windows",
    "anomaly_windows",
    "transition_windows",
    "unknown_windows",
    "tp",
    "fp",
    "tn",
    "fn",
    "precision",
    "precision_reason",
    "recall",
    "recall_reason",
    "f1",
    "f1_reason",
    "fpr",
    "fpr_reason",
    "score_coverage",
    "evaluation_coverage",
    "processing_delay_median_ms",
    "processing_delay_p95_ms",
    "inference_call_count",
    "inference_duration_median_ms",
    "inference_duration_p95_ms",
)

QUALITY_COLUMNS = (
    "run_id",
    "split",
    "scenario_code",
    "received_messages",
    "valid_telemetry_messages",
    "canonical_telemetry_messages",
    "duplicate_messages",
    "invalid_messages",
    "non_telemetry_messages",
    "scheduled_windows",
    "valid_feature_windows",
    "scored_windows",
    "eligible_windows",
    "missing_feature_record",
    "observation_gap",
    "insufficient_data",
    "invalid_feature",
    "inference_not_scored",
    "unknown_ground_truth",
    "transition_window",
)


@dataclass(frozen=True, slots=True)
class ActualEvent:
    run_id: str
    phase_id: str
    device_id: str
    start_utc: str
    end_utc: str


def _actual_event(connection: Any, entry: RunPlanEntry) -> ActualEvent | None:
    if entry.scenario == "normal" or not table_exists(connection, "scenario_events"):
        return None
    events = query_records(
        connection,
        """
        SELECT scenario_run_id, device_id, event_type, event_at
        FROM scenario_events
        WHERE device_id = ? AND scenario = ?
        ORDER BY event_at, id
        """,
        (entry.target_device, entry.scenario),
    )
    starts = [row for row in events if row["event_type"] == "scenario_started"]
    ends = [row for row in events if row["event_type"] == "scenario_ended"]
    if len(starts) != 1 or len(ends) != 1:
        return None
    start, end = starts[0], ends[0]
    if start["scenario_run_id"] != end["scenario_run_id"]:
        return None
    if parse_utc(str(end["event_at"])) <= parse_utc(str(start["event_at"])):
        return None
    return ActualEvent(
        run_id=entry.run_id,
        phase_id=str(start["scenario_run_id"]),
        device_id=str(start["device_id"]),
        start_utc=str(start["event_at"]),
        end_utc=str(end["event_at"]),
    )


def _prediction_map(connection: Any) -> dict[str, dict[str, Any]]:
    if not table_exists(connection, "window_predictions"):
        return {}
    return {
        str(row["window_id"]): row
        for row in query_records(connection, "SELECT * FROM window_predictions")
    }


def _primary_exclusion(
    feature_status: str,
    inference_status: str,
    ground_truth_label: str,
) -> str:
    if feature_status == "missing_record":
        return "missing_feature_record"
    if feature_status == "observation_gap":
        return "observation_gap"
    if feature_status == "insufficient_data":
        return "insufficient_data"
    if feature_status != "ok":
        return "invalid_feature"
    if inference_status != "scored":
        return "inference_not_scored"
    if ground_truth_label == "unknown":
        return "unknown_ground_truth"
    if ground_truth_label == "transition":
        return "transition_window"
    return ""


def _run_rows(
    results_root: Path, entry: RunPlanEntry
) -> tuple[list[dict[str, Any]], ActualEvent | None, dict[str, int]]:
    run_directory = results_root / "runs" / entry.run_id
    manifest = load_json_object(run_directory / "run_manifest.json")
    if manifest.get("run_id") != entry.run_id:
        raise ValueError(f"run manifest identity mismatch for {entry.run_id}")
    if manifest.get("status") != "completed":
        raise ValueError(f"run is not completed: {entry.run_id}")
    if not manifest.get("run_start_utc"):
        raise ValueError(f"run_start_utc is missing for {entry.run_id}")
    run_start = parse_utc(str(manifest["run_start_utc"]))
    database = find_run_database(run_directory)
    with connect_read_only(database) as connection:
        if not table_exists(connection, "feature_windows"):
            raise ValueError(f"feature_windows is missing for {entry.run_id}")
        features = {
            str(row["window_id"]): row
            for row in query_records(connection, "SELECT * FROM feature_windows")
        }
        predictions = _prediction_map(connection)
        inference_durations = {
            str(row["inference_call_id"]): row["inference_duration_ms"]
            for row in (
                query_records(connection, "SELECT * FROM inference_calls")
                if table_exists(connection, "inference_calls")
                else []
            )
        }
        event = _actual_event(connection, entry)
        if table_exists(connection, "mqtt_observations"):
            raw = dict(
                connection.execute(
                    """
                    SELECT
                        COUNT(*) AS received_messages,
                        SUM(CASE WHEN message_type = 'telemetry'
                                  AND payload_valid = 1 THEN 1 ELSE 0 END)
                            AS valid_telemetry_messages,
                        SUM(CASE WHEN message_type = 'telemetry'
                                  AND payload_valid = 1
                                  AND duplicate = 0 AND retain = 0
                                  AND session_id IS NOT NULL THEN 1 ELSE 0 END)
                            AS canonical_telemetry_messages,
                        SUM(CASE WHEN duplicate = 1 THEN 1 ELSE 0 END)
                            AS duplicate_messages,
                        SUM(CASE WHEN payload_valid = 0 THEN 1 ELSE 0 END)
                            AS invalid_messages,
                        SUM(CASE WHEN message_type <> 'telemetry' THEN 1 ELSE 0 END)
                            AS non_telemetry_messages
                    FROM mqtt_observations
                    """
                ).fetchone()
            )
            raw_quality = {name: int(value or 0) for name, value in raw.items()}
        else:
            raw_quality = {
                "received_messages": 0,
                "valid_telemetry_messages": 0,
                "canonical_telemetry_messages": 0,
                "duplicate_messages": 0,
                "invalid_messages": 0,
                "non_telemetry_messages": 0,
            }
    expected_session = str(manifest.get("collection_session_id", ""))
    for source_name, source_rows in (("feature", features.values()), ("prediction", predictions.values())):
        run_ids = {str(row.get("run_id", "")) for row in source_rows}
        sessions = {str(row.get("collection_session_id", "")) for row in source_rows}
        if run_ids and run_ids != {entry.run_id}:
            raise ValueError(
                f"{source_name} run identity mismatch for {entry.run_id}: {sorted(run_ids)}"
            )
        if sessions and sessions != {expected_session}:
            raise ValueError(
                f"{source_name} collection session mismatch for {entry.run_id}: {sorted(sessions)}"
            )

    phase = None
    if event is not None:
        phase = ScenarioPhase(
            phase_id=event.phase_id,
            target_device_id=event.device_id,
            start_unix=parse_utc(event.start_utc).timestamp(),
            end_unix=parse_utc(event.end_utc).timestamp(),
        )
    per_device = int((entry.duration_seconds - 20) // 5) + 1
    output: list[dict[str, Any]] = []
    for device_index in range(1, entry.device_count + 1):
        device_id = f"iot-{device_index:03d}"
        for window_index in range(per_device):
            window_id = f"{entry.run_id}:{device_id}:{window_index:06d}"
            start = run_start + timedelta(seconds=window_index * 5)
            end = start + timedelta(seconds=20)
            feature = features.get(window_id, {})
            prediction = predictions.get(window_id, {})
            feature_status = str(feature.get("feature_status", "missing_record"))
            if feature:
                truth_label = str(feature.get("ground_truth_label", "unknown"))
                phase_id = feature.get("phase_id") or ""
                is_target = bool(feature.get("is_target_device", False))
                start_text = str(feature.get("window_start_utc", format_utc(start)))
                end_text = str(feature.get("window_end_utc", format_utc(end)))
            else:
                truth = classify_ground_truth(
                    device_id=device_id,
                    window_start_unix=start.timestamp(),
                    window_end_unix=end.timestamp(),
                    run_start_unix=run_start.timestamp(),
                    run_end_unix=(run_start + timedelta(seconds=entry.duration_seconds)).timestamp(),
                    scenario_phase=phase,
                    phase_log_complete=(entry.scenario == "normal" or phase is not None),
                )
                truth_label = truth.label
                phase_id = truth.phase_id or ""
                is_target = truth.is_target_device
                start_text, end_text = format_utc(start), format_utc(end)
            if prediction:
                inference_status = str(prediction.get("inference_status", "scored"))
            elif entry.split in {"train", "validation"}:
                inference_status = "not_required"
            elif not manifest.get("model_version"):
                inference_status = "model_not_available"
            else:
                inference_status = "missing_prediction"
            exclusion = _primary_exclusion(
                feature_status, inference_status, truth_label
            )
            included = (
                entry.split == "test"
                and not exclusion
                and truth_label in {"normal", "anomaly"}
            )
            row: dict[str, Any] = {
                "split": entry.split,
                "scenario": entry.scenario,
                "scenario_code": SCENARIO_CODES[entry.scenario],
                "run_id": entry.run_id,
                "collection_session_id": feature.get(
                    "collection_session_id", manifest.get("collection_session_id", "")
                ),
                "device_id": device_id,
                "window_id": window_id,
                "window_start_utc": start_text,
                "window_end_utc": end_text,
                "model_version": prediction.get("model_version", ""),
                "prediction_recorded_at_utc": prediction.get("predicted_at_utc", ""),
                "inference_call_id": prediction.get("inference_call_id", ""),
                "window_processing_delay_ms": prediction.get("processing_delay_ms", ""),
                "inference_duration_ms": inference_durations.get(
                    str(prediction.get("inference_call_id", "")), ""
                ),
                "score": prediction.get("anomaly_score", ""),
                "threshold": prediction.get("decision_threshold", ""),
                "predicted_label": prediction.get("predicted_label", ""),
                "inference_status": inference_status,
                "ground_truth_label": truth_label,
                "phase_id": phase_id,
                "is_target_device": int(is_target),
                "is_control_device": int(entry.scenario != "normal" and not is_target),
                "feature_status": feature_status,
                "observed_message_count": feature.get("observed_message_count", ""),
                "exclusion_reason": exclusion,
                "included_in_primary_metrics": int(included),
            }
            for name in FEATURE_NAMES:
                row[name] = feature.get(name, "")
            output.append(row)
    unexpected = sorted(set(features) - {row["window_id"] for row in output})
    if unexpected:
        raise ValueError(
            f"unexpected feature window IDs for {entry.run_id}: {unexpected[:3]}"
        )
    unexpected_predictions = sorted(
        set(predictions) - {row["window_id"] for row in output}
    )
    if unexpected_predictions:
        raise ValueError(
            f"unexpected prediction window IDs for {entry.run_id}: "
            f"{unexpected_predictions[:3]}"
        )
    return output, event, raw_quality


def _quality_row(
    entry: RunPlanEntry,
    rows: Sequence[Mapping[str, Any]],
    raw_quality: Mapping[str, int],
) -> dict[str, Any]:
    exclusions = Counter(str(row["exclusion_reason"]) for row in rows)
    return {
        "run_id": entry.run_id,
        "split": entry.split,
        "scenario_code": SCENARIO_CODES[entry.scenario],
        **raw_quality,
        "scheduled_windows": len(rows),
        "valid_feature_windows": sum(row["feature_status"] == "ok" for row in rows),
        "scored_windows": sum(row["inference_status"] == "scored" for row in rows),
        "eligible_windows": sum(bool(row["included_in_primary_metrics"]) for row in rows),
        "missing_feature_record": exclusions["missing_feature_record"],
        "observation_gap": exclusions["observation_gap"],
        "insufficient_data": exclusions["insufficient_data"],
        "invalid_feature": exclusions["invalid_feature"],
        "inference_not_scored": exclusions["inference_not_scored"],
        "unknown_ground_truth": exclusions["unknown_ground_truth"],
        "transition_window": exclusions["transition_window"],
    }


def _ratio(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else numerator / denominator


def _metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    scope: str,
    run_id: str = "",
    scenario_code: str = "",
    scenario: str = "",
) -> dict[str, Any]:
    eligible = [row for row in rows if bool(row["included_in_primary_metrics"])]
    tp = sum(row["ground_truth_label"] == "anomaly" and row["predicted_label"] == "anomaly" for row in eligible)
    fp = sum(row["ground_truth_label"] == "normal" and row["predicted_label"] == "anomaly" for row in eligible)
    tn = sum(row["ground_truth_label"] == "normal" and row["predicted_label"] == "normal" for row in eligible)
    fn = sum(row["ground_truth_label"] == "anomaly" and row["predicted_label"] == "normal" for row in eligible)
    no_anomaly_class = tp + fn == 0
    precision = _ratio(tp, tp + fp)
    recall = None if no_anomaly_class else _ratio(tp, tp + fn)
    f1 = None if no_anomaly_class else _ratio(2 * tp, 2 * tp + fp + fn)
    fpr = _ratio(fp, fp + tn)
    truth = Counter(str(row["ground_truth_label"]) for row in rows)
    delays = [float(row["window_processing_delay_ms"]) for row in rows if row["inference_status"] == "scored" and row["window_processing_delay_ms"] != ""]
    call_durations: dict[str, float] = {}
    for row in rows:
        if row["inference_status"] != "scored" or row["inference_duration_ms"] == "":
            continue
        call_id = str(row["inference_call_id"] or row["window_id"])
        call_durations.setdefault(call_id, float(row["inference_duration_ms"]))
    scheduled = len(rows)
    scored = sum(row["inference_status"] == "scored" for row in rows)
    return {
        "scope": scope,
        "run_id": run_id,
        "scenario_code": scenario_code,
        "scenario": scenario,
        "scheduled_windows": scheduled,
        "valid_feature_windows": sum(row["feature_status"] == "ok" for row in rows),
        "scored_windows": scored,
        "eligible_windows": len(eligible),
        "normal_windows": truth["normal"],
        "anomaly_windows": truth["anomaly"],
        "transition_windows": truth["transition"],
        "unknown_windows": truth["unknown"],
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": precision,
        "precision_reason": "" if precision is not None else "no_predicted_anomaly_windows",
        "recall": recall,
        "recall_reason": "" if recall is not None else "no_actual_anomaly_windows",
        "f1": f1,
        "f1_reason": "" if f1 is not None else "no_actual_anomaly_windows",
        "fpr": fpr,
        "fpr_reason": "" if fpr is not None else "no_actual_normal_windows",
        "score_coverage": _ratio(scored, scheduled),
        "evaluation_coverage": _ratio(len(eligible), scheduled),
        "processing_delay_median_ms": percentile(delays, 0.5),
        "processing_delay_p95_ms": percentile(delays, 0.95),
        "inference_call_count": len(call_durations),
        "inference_duration_median_ms": percentile(call_durations.values(), 0.5),
        "inference_duration_p95_ms": percentile(call_durations.values(), 0.95),
    }


def _event_result(
    entry: RunPlanEntry,
    event: ActualEvent | None,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    if entry.scenario == "normal":
        return None
    if event is None:
        planned_start = ""
        planned_end = ""
        return {
            "run_id": entry.run_id,
            "device_id": entry.target_device or "",
            "anomaly_start_utc": planned_start,
            "anomaly_end_utc": planned_end,
            "deadline_utc": "",
            "status": "not_evaluable",
            "first_detection_utc": "",
            "latency_seconds": "",
            "reason": "missing_or_inconsistent_scenario_events",
        }
    start, end = parse_utc(event.start_utc), parse_utc(event.end_utc)
    overlapping = [
        row
        for row in rows
        if row["device_id"] == event.device_id
        and max(parse_utc(str(row["window_start_utc"])), start)
        < min(parse_utc(str(row["window_end_utc"])), end)
    ]
    valid_input = [row for row in overlapping if row["feature_status"] == "ok"]
    scored = [row for row in valid_input if row["inference_status"] == "scored"]
    if not valid_input:
        evaluable = False
        reason = "no_valid_feature_window_overlaps_event"
    elif not scored:
        evaluable = False
        reason = "no_scored_window_overlaps_event"
    else:
        evaluable = True
        reason = None
    result = evaluate_event_detection(
        {
            "run_id": entry.run_id,
            "device_id": event.device_id,
            "anomaly_start_utc": event.start_utc,
            "anomaly_end_utc": event.end_utc,
        },
        rows,
        grace_seconds=20.0,
        evaluable=evaluable,
        not_evaluable_reason=reason,
    ).as_dict()
    return result


def evaluate_campaign(results_directory: str | Path) -> dict[str, Any]:
    """Create evaluation tables and figures exclusively from exported evidence."""

    results_root = Path(results_directory).resolve()
    entries = load_run_plan(results_root / "run_plan.csv")
    splits = load_split_manifest(results_root / "split_manifest.json")
    if tuple(entry.run_id for entry in entries if entry.split == "test") != splits["test"]:
        raise ValueError("test split and run plan order differ")
    all_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    quality_rows: list[dict[str, Any]] = []
    for entry in entries:
        rows, event, raw_quality = _run_rows(results_root, entry)
        all_rows.extend(rows)
        quality_rows.append(_quality_row(entry, rows, raw_quality))
        event_result = _event_result(entry, event, rows)
        if event_result is not None:
            event_rows.append(event_result)
    write_csv_atomic(results_root / "evaluation_windows.csv", EVALUATION_COLUMNS, all_rows)
    write_csv_atomic(
        results_root / "data_quality_by_run.csv", QUALITY_COLUMNS, quality_rows
    )

    test_rows = [row for row in all_rows if row["split"] == "test"]
    by_run = []
    for entry in entries:
        if entry.split != "test":
            continue
        rows = [row for row in test_rows if row["run_id"] == entry.run_id]
        by_run.append(
            _metrics(
                rows,
                scope="run",
                run_id=entry.run_id,
                scenario_code=SCENARIO_CODES[entry.scenario],
                scenario=entry.scenario,
            )
        )
    write_csv_atomic(results_root / "metrics_by_run.csv", METRIC_COLUMNS, by_run)

    by_scenario = []
    for scenario, code in SCENARIO_CODES.items():
        rows = [row for row in test_rows if row["scenario"] == scenario]
        by_scenario.append(
            _metrics(rows, scope="scenario", scenario_code=code, scenario=scenario)
        )
    controls = [
        row
        for row in test_rows
        if row["scenario"] != "normal" and bool(row["is_control_device"])
    ]
    by_scenario.append(
        _metrics(
            controls,
            scope="control_devices",
            scenario_code="K2-K4-controls",
            scenario="control_devices_in_abnormal_runs",
        )
    )
    by_scenario.append(
        _metrics(test_rows, scope="all_test", scenario_code="ALL", scenario="all_test")
    )
    write_csv_atomic(
        results_root / "metrics_by_scenario.csv", METRIC_COLUMNS, by_scenario
    )

    event_columns = (
        "run_id",
        "device_id",
        "anomaly_start_utc",
        "anomaly_end_utc",
        "deadline_utc",
        "status",
        "first_detection_utc",
        "latency_seconds",
        "reason",
    )
    write_csv_atomic(results_root / "event_detection.csv", event_columns, event_rows)
    detected = [row for row in event_rows if row["status"] == "detected"]
    evaluable_count = sum(
        row["status"] in {"detected", "not_detected"} for row in event_rows
    )
    summary = {
        "schema_version": "1.0",
        "scheduled_windows": len(all_rows),
        "test_scheduled_windows": len(test_rows),
        "test_evaluated_windows": sum(
            bool(row["included_in_primary_metrics"]) for row in test_rows
        ),
        "data_quality_run_count": len(quality_rows),
        "planned_events": len(event_rows),
        "detected_events": len(detected),
        "not_detected_events": sum(row["status"] == "not_detected" for row in event_rows),
        "not_evaluable_events": sum(row["status"] == "not_evaluable" for row in event_rows),
        "event_detection_rate": _ratio(len(detected), evaluable_count),
        "event_latency_median_seconds": percentile(
            (float(row["latency_seconds"]) for row in detected), 0.5
        ),
        "event_latency_min_seconds": min(
            (float(row["latency_seconds"]) for row in detected), default=None
        ),
        "event_latency_max_seconds": max(
            (float(row["latency_seconds"]) for row in detected), default=None
        ),
    }
    write_json_atomic(results_root / "evaluation_summary.json", summary)
    from src.pipeline.figures import generate_figures

    summary["figures"] = generate_figures(results_root, test_rows, by_scenario)
    write_json_atomic(results_root / "evaluation_summary.json", summary)
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", default="results")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    print(json.dumps(evaluate_campaign(arguments.results_dir), sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
