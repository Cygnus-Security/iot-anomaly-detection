from __future__ import annotations

import csv
import json
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Mapping, Sequence

import pytest

from src.experiments import runner as runner_module
from src.experiments.artifacts import prepare_run_directory, update_run_manifest
from src.experiments.planning import RunPlanEntry, load_run_plan
from src.experiments.runner import (
    CampaignRunError,
    CampaignRunner,
    ComposeClient,
    _dry_run_summary,
    _validate_collector_state,
    _write_integrity_report,
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
    for name in (
        "run.db-journal",
        "analytics-container.log",
        "integrity-report.json",
        "timing_samples.csv",
    ):
        (run_directory / name).write_text(f"{name}\n", encoding="utf-8")
    original_session = json.loads(manifest_path.read_text(encoding="utf-8"))[
        "collection_session_id"
    ]

    with pytest.raises(CampaignRunError, match="--retry-failed"):
        prepare_run_attempt(run_directory, entry, retry_failed=False)

    manifest = prepare_run_attempt(
        run_directory,
        entry,
        retry_failed=True,
        code_version="def456",
        worktree_dirty=False,
    )
    assert manifest is not None
    updated = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert updated["status"] == "planned"
    assert updated["attempt_number"] == 2
    assert updated["attempt_history"][0]["failure_reason"] == "intentional"
    assert updated["attempt_history"][0]["code_version"] == "abc123"
    assert updated["attempt_history"][0]["collection_session_id"] == original_session
    assert updated["attempt_history"][0]["archive_directory"].startswith(
        "attempts/attempt-01-"
    )
    assert updated["collection_session_id"] != original_session
    assert updated["code_version"] == "def456"
    assert updated["worktree_dirty"] is False
    archived = list((run_directory / "attempts").glob("attempt-01-*"))
    assert len(archived) == 1
    assert (archived[0] / "run.db").read_bytes() == b"prior-attempt"
    assert (archived[0] / "orchestration_events.csv").is_file()
    for name in (
        "run.db-journal",
        "analytics-container.log",
        "integrity-report.json",
        "timing_samples.csv",
    ):
        assert (archived[0] / name).is_file()
    assert not (run_directory / "run.db").exists()
    with (run_directory / "scenario_phases.csv").open(encoding="utf-8", newline="") as handle:
        assert {row["status"] for row in csv.DictReader(handle)} == {"planned"}

    update_run_manifest(manifest_path, status="running")
    update_run_manifest(manifest_path, status="completed")
    assert prepare_run_attempt(run_directory, entry, retry_failed=True) is None


def test_running_attempt_requires_explicit_retry_flag(tmp_path: Path) -> None:
    entry = _entry("train-normal-01")
    run_directory = prepare_run_directory(tmp_path, entry, code_version="abc123")
    update_run_manifest(run_directory / "run_manifest.json", status="running")

    with pytest.raises(CampaignRunError, match="--retry-failed"):
        prepare_run_attempt(run_directory, entry, retry_failed=False)


def test_integrity_gate_rejects_a_corrupt_snapshot(tmp_path: Path) -> None:
    database = tmp_path / "run.db"
    database.write_bytes(b"not-a-sqlite-database")
    report = tmp_path / "integrity-report.json"

    with pytest.raises(CampaignRunError, match="integrity"):
        _write_integrity_report(
            database,
            report,
            container_check={"status": "ok", "integrity_check": ["ok"]},
        )

    assert json.loads(report.read_text(encoding="utf-8"))["status"] == "failed"


def test_collector_state_gate_requires_drained_lossless_shutdown(tmp_path: Path) -> None:
    path = tmp_path / "run.db.collector-state.json"
    clean = {
        "schema_version": "1.0",
        "accepting_messages": False,
        "queue_depth": 0,
        "oldest_pending_received_at_unix": None,
        "last_enqueued_at_unix": 100.0,
        "last_persisted_received_at_unix": 100.0,
        "dropped_message_count": 0,
        "loss_events": [],
    }
    path.write_text(json.dumps(clean) + "\n", encoding="utf-8")
    assert _validate_collector_state(path)["queue_depth"] == 0

    path.write_text(
        json.dumps({**clean, "dropped_message_count": 1}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(CampaignRunError, match="message loss"):
        _validate_collector_state(path)


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
    runtime_directory = tmp_path / "runtime-volume"
    runtime_directory.mkdir()
    runtime_database = runtime_directory / "run.db"
    runtime_state = runtime_directory / "run.db.collector-state.json"
    clock = FakeClock()
    calls: list[list[str]] = []

    def initialize_raw_database() -> None:
        with sqlite3.connect(runtime_database) as connection:
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
        runtime_state.write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "accepting_messages": False,
                    "queue_depth": 0,
                    "oldest_pending_received_at_unix": None,
                    "last_enqueued_at_unix": 1_700_000_000.0,
                    "last_persisted_received_at_unix": 1_700_000_000.0,
                    "dropped_message_count": 0,
                    "loss_events": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )

    def fake_runner(
        arguments: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
        timeout: float | None,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, timeout, check
        argv = list(arguments)
        calls.append(argv)
        if "up" in argv and argv[-1] == "simulator" and not runtime_database.exists():
            initialize_raw_database()
        if "up" in argv and argv[-1] == "analytics":
            ready_path.write_text('{"ready":true}\n', encoding="utf-8")
        if argv[:2] == ["docker", "inspect"]:
            state = (
                {"Status": "running", "ExitCode": 0}
                if clock.monotonic() < 1.0
                else {"Status": "exited", "ExitCode": 0}
            )
            return subprocess.CompletedProcess(arguments, 0, json.dumps(state) + "\n", "")
        if argv[:2] == ["docker", "cp"]:
            name = argv[2].rsplit("/", 1)[-1]
            source = runtime_directory / name
            if not source.exists():
                return subprocess.CompletedProcess(arguments, 1, "", "not found")
            shutil.copy2(source, Path(argv[3]))
            return subprocess.CompletedProcess(arguments, 0, "", "")
        stdout = ""
        if "ps" in argv and argv[-1] == "analytics":
            stdout = "analytics-id\n"
        elif "ps" in argv and argv[-1] == "collector":
            stdout = "collector-id\n"
        elif "exec" in argv and argv[-4:-1] == ["collector", "python", "-c"]:
            assert env["SQLITE_PATH"] == "/app/experiment-data/run.db"
            stdout = "12\n"
        elif "run" in argv and argv[-4:-1] == ["collector", "python", "-c"]:
            stdout = json.dumps(
                {"checkpoint": [[0, 0, 0]], "integrity_check": ["ok"]}
            ) + "\n"
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
        clock=clock,
    )
    assert runner.run_entry(entry) == "completed"
    manifest = json.loads((run_directory / "run_manifest.json").read_text())
    assert manifest["status"] == "completed"
    assert manifest["smoke_mode"] is True
    assert manifest["observed_counts"]["telemetry_messages"] == 12
    assert "orchestration_events.csv" in manifest["output_files"]
    assert (run_directory / "resource_samples.csv").is_file()
    assert (run_directory / "orchestration_events.csv").is_file()
    assert (run_directory / "integrity-report.json").is_file()
    assert json.loads((run_directory / "integrity-report.json").read_text())[
        "status"
    ] == "ok"
    stop_index = next(index for index, call in enumerate(calls) if "stop" in call)
    copy_index = next(index for index, call in enumerate(calls) if call[:2] == ["docker", "cp"])
    assert stop_index < copy_index
    assert any(
        "logs" in call
        and "--profile" in call
        and "experiment" in call
        and "tools" in call
        for call in calls
    )
    assert any("down" in call and "--volumes" in call for call in calls)
    assert not (run_directory / "timing_samples.csv").exists()


def test_keyboard_interrupt_marks_attempt_aborted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = _entry("train-normal-01")
    results_root = tmp_path / "results"
    run_directory = prepare_run_directory(results_root, entry, code_version="abc123")

    class InterruptClock(FakeClock):
        def sleep(self, seconds: float) -> None:
            del seconds
            raise KeyboardInterrupt

    def fake_runner(
        arguments: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
        timeout: float | None,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, env, timeout, check
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr(runner_module, "_remove_runtime_project", lambda compose: None)
    monkeypatch.setattr(runner_module, "_stop_runtime_services", lambda compose: None)
    monkeypatch.setattr(
        runner_module, "_capture_analytics_log", lambda compose, destination: None
    )
    monkeypatch.setattr(runner_module, "_wait_for_analytics_ready", lambda *a, **k: None)
    monkeypatch.setattr(runner_module, "_wait_for_devices", lambda *a, **k: None)
    monkeypatch.setattr(runner_module, "_assert_analytics_running", lambda compose: None)
    monkeypatch.setattr(runner_module, "_resource_sample", lambda *a, **k: [])
    monkeypatch.setattr(
        runner_module,
        "_validate_runtime_volume",
        lambda compose: {"status": "ok", "integrity_check": ["ok"]},
    )
    monkeypatch.setattr(
        runner_module, "_copy_runtime_artifacts", lambda *a, **k: False
    )

    runner = CampaignRunner(
        repository_root=tmp_path,
        results_root=results_root,
        compose_file=tmp_path / "docker-compose.yml",
        smoke_duration_seconds=1.0,
        command_runner=fake_runner,
        clock=InterruptClock(),
    )
    with pytest.raises(KeyboardInterrupt):
        runner.run_entry(entry)

    manifest = json.loads((run_directory / "run_manifest.json").read_text())
    assert manifest["status"] == "aborted"
    assert manifest["failure_reason"].startswith("KeyboardInterrupt")


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
