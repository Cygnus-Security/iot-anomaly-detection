from __future__ import annotations

import csv
import json
import sqlite3
import subprocess
from pathlib import Path
from typing import Mapping, Sequence

import pytest

from src.experiments.artifacts import prepare_run_directory, update_run_manifest
from src.experiments.planning import RunPlanEntry, load_run_plan
from src.experiments.runner import (
    CampaignRunError,
    CampaignRunner,
    ComposeClient,
    _dry_run_summary,
    _write_actual_phases,
    effective_timing,
    prepare_run_attempt,
)


def _entry(run_id: str) -> RunPlanEntry:
    return next(item for item in load_run_plan("experiments/run_plan.csv") if item.run_id == run_id)


class FakeClock:
    def __init__(self, wall: float = 1_700_000_000.0) -> None:
        self.wall_value = wall
        self.monotonic_value = 0.0

    def wall(self) -> float:
        return self.wall_value

    def monotonic(self) -> float:
        return self.monotonic_value

    def sleep(self, seconds: float) -> None:
        self.wall_value += seconds
        self.monotonic_value += seconds


def test_effective_timing_preserves_plan_and_labels_smoke_runs() -> None:
    normal = _entry("train-normal-01")
    anomaly = _entry("test-k2-01")

    exact = effective_timing(anomaly, None)
    assert (exact.total_seconds, exact.baseline_before_seconds) == (180.0, 60.0)
    assert not exact.smoke_mode

    compressed_normal = effective_timing(normal, 2.5)
    assert compressed_normal.total_seconds == 2.5
    assert compressed_normal.baseline_before_seconds == 2.5
    assert compressed_normal.anomaly_seconds == 0
    assert compressed_normal.smoke_mode

    compressed_anomaly = effective_timing(anomaly, 9)
    assert (
        compressed_anomaly.baseline_before_seconds,
        compressed_anomaly.anomaly_seconds,
        compressed_anomaly.baseline_after_seconds,
    ) == (3.0, 3.0, 3.0)

    with pytest.raises(ValueError, match="at least three"):
        effective_timing(anomaly, 2)


def test_dry_run_summary_is_deterministic_and_has_no_side_effects(tmp_path: Path) -> None:
    entries = (_entry("test-k3-02"),)
    summary = _dry_run_summary(entries, 12)
    assert summary == [
        {
            "run_id": "test-k3-02",
            "split": "test",
            "scenario": "abnormal_interval",
            "seed": 502,
            "target_device": "iot-002",
            "timing": {
                "total_seconds": 12,
                "baseline_before_seconds": 4,
                "anomaly_seconds": 4,
                "baseline_after_seconds": 4,
            },
            "database": "results/runs/test-k3-02/run.db",
        }
    ]
    assert list(tmp_path.iterdir()) == []


def test_retry_archives_failed_evidence_and_completed_runs_are_immutable(
    tmp_path: Path,
) -> None:
    entry = _entry("test-k4-01")
    run_directory = prepare_run_directory(tmp_path, entry, code_version="abc123")
    manifest_path = run_directory / "run_manifest.json"
    update_run_manifest(
        manifest_path,
        status="running",
        changes={"run_start_utc": "2026-01-01T00:00:00Z"},
    )
    update_run_manifest(
        manifest_path,
        status="failed",
        changes={"failure_reason": "intentional"},
    )
    (run_directory / "run.db").write_bytes(b"prior-attempt")
    (run_directory / "runtime.log").write_text("evidence\n", encoding="utf-8")
    (run_directory / "orchestration_events.csv").write_text(
        "event\nrun_start\n", encoding="utf-8"
    )

    with pytest.raises(CampaignRunError, match="--retry-failed"):
        prepare_run_attempt(run_directory, entry, retry_failed=False)

    manifest = prepare_run_attempt(run_directory, entry, retry_failed=True)
    assert manifest is not None
    updated = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert updated["status"] == "planned"
    assert updated["attempt_number"] == 2
    assert updated["attempt_history"][0]["failure_reason"] == "intentional"
    archived = list((run_directory / "attempts").glob("attempt-01-*"))
    assert len(archived) == 1
    assert (archived[0] / "run.db").read_bytes() == b"prior-attempt"
    assert (archived[0] / "orchestration_events.csv").is_file()
    assert not (run_directory / "run.db").exists()
    with (run_directory / "scenario_phases.csv").open(encoding="utf-8", newline="") as handle:
        assert {row["status"] for row in csv.DictReader(handle)} == {"planned"}

    update_run_manifest(manifest_path, status="running")
    update_run_manifest(manifest_path, status="completed")
    assert prepare_run_attempt(run_directory, entry, retry_failed=True) is None


def test_compose_client_uses_fixed_project_file_and_environment(tmp_path: Path) -> None:
    calls: list[tuple[list[str], Mapping[str, str]]] = []

    def fake_runner(
        arguments: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
        timeout: float | None,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, timeout, check
        calls.append((list(arguments), env))
        return subprocess.CompletedProcess(arguments, 0, "ok\n", "")

    client = ComposeClient(
        repository_root=tmp_path,
        compose_file=tmp_path / "compose.yml",
        project_name="chapter4",
        environment={"SIMULATION_SEED": "401"},
        command_runner=fake_runner,
        runtime_log=tmp_path / "runtime.log",
    )
    client.run(["up", "-d", "simulator"])
    assert calls[0][0] == [
        "docker",
        "compose",
        "--project-name",
        "chapter4",
        "--file",
        str(tmp_path / "compose.yml"),
        "up",
        "-d",
        "simulator",
    ]
    assert calls[0][1]["SIMULATION_SEED"] == "401"
    assert "exit=0" in (tmp_path / "runtime.log").read_text(encoding="utf-8")


def test_actual_phase_file_uses_broker_event_boundaries(tmp_path: Path) -> None:
    entry = _entry("test-k2-01")
    run_directory = prepare_run_directory(tmp_path, entry, code_version="abc123")
    _write_actual_phases(
        run_directory,
        entry,
        run_start=1000.0,
        run_end=1180.0,
        scenario_bounds=(1060.25, 1120.25),
    )
    with (run_directory / "scenario_phases.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["status"] for row in rows] == ["completed"] * 3
    assert rows[0]["actual_end_utc"] == rows[1]["actual_start_utc"]
    assert rows[1]["actual_end_utc"] == rows[2]["actual_start_utc"]


def test_runner_completes_a_normal_smoke_attempt_with_injected_runtime(
    tmp_path: Path,
) -> None:
    entry = _entry("train-normal-01")
    results_root = tmp_path / "results"
    run_directory = prepare_run_directory(results_root, entry, code_version="abc123")
    database_path = run_directory / "run.db"
    ready_path = run_directory / "analytics.ready.json"

    def initialize_raw_database() -> None:
        with sqlite3.connect(database_path) as connection:
            connection.executescript(
                """
                CREATE TABLE mqtt_observations (
                    device_id TEXT, message_type TEXT, payload_valid INTEGER,
                    duplicate INTEGER, retain INTEGER
                );
                CREATE TABLE feature_windows (window_id TEXT);
                CREATE TABLE window_predictions (window_id TEXT);
                CREATE TABLE anomaly_alerts (id INTEGER);
                """
            )
            connection.executemany(
                "INSERT INTO mqtt_observations VALUES (?, 'telemetry', 1, 0, 0)",
                [(f"iot-{index:03d}",) for index in range(1, 13)],
            )

    def fake_runner(
        arguments: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
        timeout: float | None,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, env, timeout, check
        argv = list(arguments)
        if "up" in argv and argv[-1] == "simulator" and not database_path.exists():
            initialize_raw_database()
        if "up" in argv and argv[-1] == "analytics":
            ready_path.write_text('{"ready":true}\n', encoding="utf-8")
        stdout = ""
        if "logs" in argv:
            stdout = "collector | clean shutdown\n"
        return subprocess.CompletedProcess(arguments, 0, stdout, "")

    runner = CampaignRunner(
        repository_root=tmp_path,
        results_root=tmp_path / "results",
        compose_file=tmp_path / "docker-compose.yml",
        smoke_duration_seconds=1.0,
        settle_grace_seconds=0.0,
        command_runner=fake_runner,
        clock=FakeClock(),
    )
    assert runner.run_entry(entry) == "completed"
    manifest = json.loads((run_directory / "run_manifest.json").read_text())
    assert manifest["status"] == "completed"
    assert manifest["smoke_mode"] is True
    assert manifest["observed_counts"]["telemetry_messages"] == 12
    assert "orchestration_events.csv" in manifest["output_files"]
    assert (run_directory / "resource_samples.csv").is_file()
    assert (run_directory / "orchestration_events.csv").is_file()
    assert not (run_directory / "timing_samples.csv").exists()


def test_container_paths_follow_the_compose_results_mount(tmp_path: Path) -> None:
    runner = CampaignRunner(
        repository_root=tmp_path,
        results_root=tmp_path / "results" / "smoke",
        compose_file=tmp_path / "docker-compose.yml",
    )
    assert runner._container_results_root() == "/app/results/smoke"
    assert (
        runner._container_path(tmp_path / "results" / "smoke" / "artifacts")
        == "/app/results/smoke/artifacts"
    )
    runner.results_root = tmp_path / "outside-results"
    with pytest.raises(CampaignRunError, match="--results-dir"):
        runner._container_results_root()
