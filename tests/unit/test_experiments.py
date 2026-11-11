from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from src.experiments.artifacts import (
    _safe_run_path,
    expected_window_count,
    initialize_campaign,
    redact_secrets,
    update_run_manifest,
    write_csv_atomic,
    write_json_atomic,
)
from src.experiments.planning import (
    CANONICAL_RUN_IDS,
    PlanValidationError,
    load_run_plan,
    load_split_manifest,
    phase_schedule,
    validate_split_manifest,
)

PLAN_PATH = Path("experiments/run_plan.csv")
SPLIT_PATH = Path("experiments/split_manifest.json")


def test_canonical_plan_and_split_are_complete_and_disjoint() -> None:
    entries = load_run_plan(PLAN_PATH)
    splits = load_split_manifest(SPLIT_PATH)
    validate_split_manifest(splits, entries)

    assert tuple(entry.run_id for entry in entries) == CANONICAL_RUN_IDS
    assert len(entries) == 13
    assert sum(entry.duration_seconds for entry in entries) == 47 * 60
    assert len(splits["train"]) == 2
    assert len(splits["validation"]) == 1
    assert len(splits["test"]) == 10
    assert {entry.simulation_seed for entry in entries} == {
        101,
        102,
        201,
        301,
        401,
        402,
        403,
        501,
        502,
        503,
        601,
        602,
        603,
    }


def test_phase_schedule_matches_normal_and_three_phase_runs() -> None:
    entries = {entry.run_id: entry for entry in load_run_plan(PLAN_PATH)}

    normal = phase_schedule(entries["train-normal-01"])
    assert [
        (
            phase.phase_name,
            phase.scenario,
            phase.ground_truth_label,
            phase.planned_start_offset_seconds,
            phase.planned_end_offset_seconds,
        )
        for phase in normal
    ] == [("normal", "normal", "normal", 0, 300)]

    k3 = phase_schedule(entries["test-k3-02"])
    assert [
        (
            phase.phase_name,
            phase.scenario,
            phase.ground_truth_label,
            phase.target_device,
            phase.planned_start_offset_seconds,
            phase.planned_end_offset_seconds,
        )
        for phase in k3
    ] == [
        ("baseline_before", "normal", "normal", "iot-002", 0, 60),
        ("anomaly", "abnormal_interval", "anomaly", "iot-002", 60, 120),
        ("baseline_after", "normal", "normal", "iot-002", 120, 180),
    ]


def test_expected_window_count_uses_full_windows_after_warmup() -> None:
    assert expected_window_count(19, device_count=12) == 0
    assert expected_window_count(20, device_count=12) == 12
    assert expected_window_count(180, device_count=12) == 396
    assert expected_window_count(300, device_count=12) == 684
    assert sum(
        expected_window_count(entry.duration_seconds, device_count=entry.device_count)
        for entry in load_run_plan(PLAN_PATH)
    ) == 6300

    with pytest.raises(ValueError, match="non-negative"):
        expected_window_count(-1)


def test_split_validation_rejects_overlap_and_missing_runs() -> None:
    entries = load_run_plan(PLAN_PATH)
    splits = load_split_manifest(SPLIT_PATH)

    overlapped = {name: list(run_ids) for name, run_ids in splits.items()}
    overlapped["test"].append("train-normal-01")
    with pytest.raises(PlanValidationError, match="more than one split"):
        validate_split_manifest(overlapped, entries)

    missing = {name: list(run_ids) for name, run_ids in splits.items()}
    missing["test"].remove("test-k4-03")
    with pytest.raises(PlanValidationError, match="manifest and plan differ"):
        validate_split_manifest(missing, entries)


def test_secret_redaction_and_atomic_writers(tmp_path: Path) -> None:
    value = {
        "mqtt": {
            "username": "collector",
            "password": "do-not-store",
            "nested": [{"api_key": "also-secret"}],
        },
        "device_count": 12,
    }
    redacted = redact_secrets(value)
    assert redacted["mqtt"]["username"] == "collector"
    assert redacted["mqtt"]["password"] == "[REDACTED]"
    assert redacted["mqtt"]["nested"][0]["api_key"] == "[REDACTED]"

    json_path = write_json_atomic(tmp_path / "record.json", value)
    assert json.loads(json_path.read_text(encoding="utf-8")) == redacted

    csv_path = write_csv_atomic(
        tmp_path / "record.csv",
        ("run_id", "status"),
        ({"run_id": "test-k2-01", "status": "planned"},),
    )
    with csv_path.open(encoding="utf-8", newline="") as handle:
        assert list(csv.DictReader(handle)) == [
            {"run_id": "test-k2-01", "status": "planned"}
        ]


def test_campaign_initialization_creates_traceable_non_overwriting_layout(
    tmp_path: Path,
) -> None:
    root = initialize_campaign(
        plan_path=PLAN_PATH,
        split_path=SPLIT_PATH,
        results_root=tmp_path / "results",
        repository_root=Path.cwd(),
        effective_config={
            "device_count": 12,
            "operator_password": "must-be-redacted",
        },
    )

    campaign = json.loads((root / "campaign_manifest.json").read_text())
    environment = json.loads((root / "environment.json").read_text())
    assert campaign["run_count"] == 13
    assert campaign["total_planned_duration_seconds"] == 47 * 60
    assert campaign["expected_scheduled_windows"] == 6300
    assert environment["effective_config"]["operator_password"] == "[REDACTED]"
    assert (root / "figures").is_dir()
    assert (root / "screenshots").is_dir()
    assert len(list((root / "runs").iterdir())) == 13

    run_root = root / "runs" / "test-k2-01"
    manifest = json.loads((run_root / "run_manifest.json").read_text())
    assert manifest["status"] == "planned"
    assert manifest["collection_session_id"]
    assert manifest["run_configuration"]["base_interval_seconds"] == 5.0
    assert manifest["run_configuration"]["interval_jitter_seconds"] == 0.5
    assert manifest["run_configuration"]["scenario_parameters"] == {
        "target_rate_hz": 5.0
    }
    with (run_root / "scenario_phases.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        phases = list(csv.DictReader(handle))
    assert [phase["phase_name"] for phase in phases] == [
        "baseline_before",
        "anomaly",
        "baseline_after",
    ]

    with pytest.raises(FileExistsError, match="not empty"):
        initialize_campaign(
            plan_path=PLAN_PATH,
            split_path=SPLIT_PATH,
            results_root=root,
            repository_root=Path.cwd(),
        )


def test_run_paths_and_status_transitions_are_safe(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsafe run ID"):
        _safe_run_path(tmp_path, "../escape")

    root = initialize_campaign(
        plan_path=PLAN_PATH,
        split_path=SPLIT_PATH,
        results_root=tmp_path / "results",
        repository_root=Path.cwd(),
    )
    manifest_path = root / "runs" / "test-k4-03" / "run_manifest.json"
    running = update_run_manifest(
        manifest_path,
        status="running",
        changes={"run_start_utc": "2026-11-11T08:00:00.000000Z"},
    )
    assert running["status"] == "running"
    completed = update_run_manifest(
        manifest_path,
        status="completed",
        changes={"run_end_utc": "2026-11-11T08:03:00.000000Z"},
    )
    assert completed["status"] == "completed"

    with pytest.raises(ValueError, match="invalid run status transition"):
        update_run_manifest(manifest_path, status="running")
    with pytest.raises(ValueError, match="identity fields"):
        update_run_manifest(
            manifest_path,
            status="completed",
            changes={"run_id": "tampered"},
        )
