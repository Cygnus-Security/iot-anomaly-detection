"""Resumable Docker Compose runner for the frozen Chapter 4 campaign.

The runner deliberately keeps orchestration outside the application containers.
Every run receives a fresh SQLite database and an immutable collection-session
identifier from its prepared manifest.  Failed/interrupted attempts are archived
before an explicit retry; completed evidence is never overwritten.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import shutil
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from src.experiments.artifacts import (
    PHASE_COLUMNS,
    expected_window_count,
    initialize_campaign,
    update_run_manifest,
    write_csv_atomic,
    write_json_atomic,
)
from src.experiments.planning import RunPlanEntry, load_run_plan, phase_schedule

MODEL_FILES = ("model.joblib", "model_metadata.json")
RESOURCE_COLUMNS = (
    "recorded_at_utc",
    "container_id",
    "name",
    "cpu_percent",
    "memory_usage",
    "memory_percent",
    "net_io",
    "block_io",
    "pids",
)
ORCHESTRATION_COLUMNS = (
    "event",
    "planned_offset_seconds",
    "actual_offset_seconds",
    "drift_ms",
    "recorded_at_utc",
)


class CampaignRunError(RuntimeError):
    """Raised when a live run cannot produce trustworthy evidence."""


def utc_from_unix(value: float) -> str:
    return (
        datetime.fromtimestamp(value, tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def unix_from_utc(value: str) -> float:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


@dataclass(frozen=True, slots=True)
class EffectiveTiming:
    total_seconds: float
    baseline_before_seconds: float
    anomaly_seconds: float
    baseline_after_seconds: float
    smoke_mode: bool


def effective_timing(
    entry: RunPlanEntry, smoke_duration_seconds: float | None
) -> EffectiveTiming:
    """Return exact plan timing, or an explicitly labelled compressed smoke run."""

    if smoke_duration_seconds is None:
        return EffectiveTiming(
            total_seconds=float(entry.duration_seconds),
            baseline_before_seconds=float(entry.baseline_before_seconds),
            anomaly_seconds=float(entry.anomaly_seconds),
            baseline_after_seconds=float(entry.baseline_after_seconds),
            smoke_mode=False,
        )
    if smoke_duration_seconds <= 0:
        raise ValueError("smoke duration must be positive")
    if entry.scenario == "normal":
        return EffectiveTiming(
            total_seconds=smoke_duration_seconds,
            baseline_before_seconds=smoke_duration_seconds,
            anomaly_seconds=0.0,
            baseline_after_seconds=0.0,
            smoke_mode=True,
        )
    if smoke_duration_seconds < 3:
        raise ValueError("abnormal smoke runs require at least three seconds")
    third = smoke_duration_seconds / 3.0
    return EffectiveTiming(
        total_seconds=smoke_duration_seconds,
        baseline_before_seconds=third,
        anomaly_seconds=third,
        baseline_after_seconds=smoke_duration_seconds - (2.0 * third),
        smoke_mode=True,
    )


class SystemClock:
    def wall(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _default_command_runner(
    arguments: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    timeout: float | None,
    check: bool,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(arguments),
        cwd=cwd,
        env=dict(env),
        timeout=timeout,
        check=check,
        capture_output=True,
        text=True,
    )


@dataclass(slots=True)
class ComposeClient:
    repository_root: Path
    compose_file: Path
    project_name: str
    environment: dict[str, str]
    command_runner: CommandRunner = _default_command_runner
    runtime_log: Path | None = None

    def arguments(self, arguments: Sequence[str]) -> list[str]:
        return [
            "docker",
            "compose",
            "--project-name",
            self.project_name,
            "--file",
            str(self.compose_file),
            *arguments,
        ]

    def run(
        self,
        arguments: Sequence[str],
        *,
        check: bool = True,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = self.arguments(arguments)
        started = time.time()
        try:
            completed = self.command_runner(
                command,
                cwd=self.repository_root,
                env=self.environment,
                timeout=timeout,
                check=check,
            )
        except subprocess.CalledProcessError as exc:
            self._log_command(command, exc.returncode, exc.stdout or "", exc.stderr or "")
            raise
        self._log_command(
            command,
            completed.returncode,
            completed.stdout or "",
            completed.stderr or "",
            elapsed_seconds=time.time() - started,
        )
        return completed

    def _log_command(
        self,
        command: Sequence[str],
        returncode: int,
        stdout: str,
        stderr: str,
        *,
        elapsed_seconds: float | None = None,
    ) -> None:
        if self.runtime_log is None:
            return
        self.runtime_log.parent.mkdir(parents=True, exist_ok=True)
        elapsed = "" if elapsed_seconds is None else f" elapsed={elapsed_seconds:.3f}s"
        with self.runtime_log.open("a", encoding="utf-8") as handle:
            handle.write(
                f"[{utc_from_unix(time.time())}] $ {shlex.join(command)}"
                f" exit={returncode}{elapsed}\n"
            )
            if stdout:
                handle.write(stdout.rstrip() + "\n")
            if stderr:
                handle.write(stderr.rstrip() + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise CampaignRunError(f"expected JSON object: {path}")
    return value


def _phase_rows(entry: RunPlanEntry) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase in phase_schedule(entry):
        row = phase.to_mapping()
        row.update(actual_start_utc="", actual_end_utc="", status="planned")
        rows.append(row)
    return rows


def _archive_retry_evidence(run_directory: Path, manifest: dict[str, Any]) -> None:
    """Move mutable evidence aside and preserve the prior manifest/phase plan."""

    attempt_number = int(manifest.get("attempt_number", 1))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    archive = run_directory / "attempts" / f"attempt-{attempt_number:02d}-{stamp}"
    archive.mkdir(parents=True, exist_ok=False)
    for name in (
        "run.db",
        "run.db-wal",
        "run.db-shm",
        "run.db.collector-state.json",
        "analytics.ready.json",
        "runtime.log",
        "resource_samples.csv",
        "orchestration_events.csv",
        "messages.csv",
        "feature_windows.csv",
        "predictions.csv",
        "alerts.csv",
    ):
        source = run_directory / name
        if source.exists():
            shutil.move(str(source), archive / name)
    for name in ("run_manifest.json", "scenario_phases.csv"):
        source = run_directory / name
        if source.exists():
            shutil.copy2(source, archive / name)


def prepare_run_attempt(
    run_directory: Path,
    entry: RunPlanEntry,
    *,
    retry_failed: bool,
) -> dict[str, Any] | None:
    """Prepare a pending attempt; return ``None`` for a completed run."""

    manifest_path = run_directory / "run_manifest.json"
    manifest = _read_json(manifest_path)
    status = str(manifest.get("status"))
    if status == "completed":
        return None
    if status in {"failed", "aborted"} and not retry_failed:
        raise CampaignRunError(
            f"{entry.run_id} is {status}; pass --retry-failed to archive and retry"
        )
    stale_files = any(
        (run_directory / name).exists()
        for name in (
            "run.db",
            "run.db-wal",
            "run.db-shm",
            "run.db.collector-state.json",
            "analytics.ready.json",
        )
    )
    if status == "planned" and stale_files:
        raise CampaignRunError(
            f"planned run {entry.run_id} contains stale runtime evidence"
        )
    if status in {"running", "failed", "aborted"}:
        prior = {
            "attempt_number": int(manifest.get("attempt_number", 1)),
            "status": status,
            "run_start_utc": manifest.get("run_start_utc"),
            "run_end_utc": manifest.get("run_end_utc"),
            "failure_reason": manifest.get("failure_reason"),
            "archived_at_utc": utc_from_unix(time.time()),
        }
        _archive_retry_evidence(run_directory, manifest)
        history = list(manifest.get("attempt_history", []))
        history.append(prior)
        manifest.update(
            status="planned",
            attempt_number=prior["attempt_number"] + 1,
            attempt_history=history,
            run_start_utc=None,
            run_end_utc=None,
            failure_reason=None,
        )
        write_json_atomic(manifest_path, manifest)
        write_csv_atomic(
            run_directory / "scenario_phases.csv", PHASE_COLUMNS, _phase_rows(entry)
        )
    else:
        manifest.setdefault("attempt_number", 1)
        manifest.setdefault("attempt_history", [])
        write_json_atomic(manifest_path, manifest)
    return manifest


def _wait_for_devices(
    database_path: Path,
    *,
    device_count: int,
    timeout_seconds: float,
    clock: SystemClock,
) -> None:
    deadline = clock.monotonic() + timeout_seconds
    last_error = "database not created"
    while clock.monotonic() < deadline:
        if database_path.is_file():
            try:
                with sqlite3.connect(database_path, timeout=2.0) as connection:
                    observed = int(
                        connection.execute(
                            """
                            SELECT COUNT(DISTINCT device_id)
                            FROM mqtt_observations
                            WHERE message_type = 'telemetry'
                              AND payload_valid = 1
                              AND duplicate = 0
                              AND retain = 0
                            """
                        ).fetchone()[0]
                    )
                if observed == device_count:
                    return
                last_error = f"observed {observed}/{device_count} devices"
            except sqlite3.Error as exc:
                last_error = str(exc)
        clock.sleep(min(0.5, max(0.0, deadline - clock.monotonic())))
    raise CampaignRunError(f"device readiness timed out: {last_error}")


def _wait_for_file(
    path: Path, *, timeout_seconds: float, clock: SystemClock
) -> None:
    deadline = clock.monotonic() + timeout_seconds
    while clock.monotonic() < deadline:
        if path.is_file():
            return
        clock.sleep(min(0.25, max(0.0, deadline - clock.monotonic())))
    raise CampaignRunError(f"readiness file timed out: {path}")


def _resource_sample(
    compose: ComposeClient, *, recorded_at: float
) -> list[dict[str, str]]:
    identifiers = [
        line.strip()
        for line in compose.run(["ps", "-q"], check=False, timeout=15).stdout.splitlines()
        if line.strip()
    ]
    if not identifiers:
        return []
    command = ["docker", "stats", "--no-stream", "--format", "{{json .}}", *identifiers]
    completed = compose.command_runner(
        command,
        cwd=compose.repository_root,
        env=compose.environment,
        timeout=20,
        check=False,
    )
    rows: list[dict[str, str]] = []
    for line in (completed.stdout or "").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        rows.append(
            {
                "recorded_at_utc": utc_from_unix(recorded_at),
                "container_id": str(value.get("ID", "")),
                "name": str(value.get("Name", "")),
                "cpu_percent": str(value.get("CPUPerc", "")),
                "memory_usage": str(value.get("MemUsage", "")),
                "memory_percent": str(value.get("MemPerc", "")),
                "net_io": str(value.get("NetIO", "")),
                "block_io": str(value.get("BlockIO", "")),
                "pids": str(value.get("PIDs", "")),
            }
        )
    return rows


def _orchestration_row(
    event: str, planned: float, actual: float, run_start: float
) -> dict[str, Any]:
    offset = actual - run_start
    return {
        "event": event,
        "planned_offset_seconds": f"{planned:.6f}",
        "actual_offset_seconds": f"{offset:.6f}",
        "drift_ms": f"{(offset - planned) * 1000.0:.3f}",
        "recorded_at_utc": utc_from_unix(actual),
    }


def _actual_scenario_bounds(database_path: Path, entry: RunPlanEntry) -> tuple[float, float]:
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            """
            SELECT event_type, event_at
            FROM scenario_events
            WHERE scenario = ? AND device_id = ?
            ORDER BY event_at, id
            """,
            (entry.scenario, entry.target_device),
        ).fetchall()
    started = next(
        (
            unix_from_utc(str(row[1]))
            for row in rows
            if row[0] == "scenario_started"
        ),
        None,
    )
    ended = next(
        (
            unix_from_utc(str(row[1]))
            for row in reversed(rows)
            if row[0] == "scenario_ended"
        ),
        None,
    )
    if started is None or ended is None or not started < ended:
        raise CampaignRunError("scenario start/end evidence is incomplete")
    return started, ended


def _write_actual_phases(
    run_directory: Path,
    entry: RunPlanEntry,
    *,
    run_start: float,
    run_end: float,
    scenario_bounds: tuple[float, float] | None,
) -> None:
    phases = _phase_rows(entry)
    if entry.scenario == "normal":
        phases[0].update(
            actual_start_utc=utc_from_unix(run_start),
            actual_end_utc=utc_from_unix(run_end),
            status="completed",
        )
    else:
        if scenario_bounds is None:
            raise CampaignRunError("abnormal run requires actual scenario bounds")
        scenario_start, scenario_end = scenario_bounds
        phases[0].update(
            actual_start_utc=utc_from_unix(run_start),
            actual_end_utc=utc_from_unix(scenario_start),
            status="completed",
        )
        phases[1].update(
            actual_start_utc=utc_from_unix(scenario_start),
            actual_end_utc=utc_from_unix(scenario_end),
            status="completed",
        )
        phases[2].update(
            actual_start_utc=utc_from_unix(scenario_end),
            actual_end_utc=utc_from_unix(run_end),
            status="completed",
        )
    write_csv_atomic(run_directory / "scenario_phases.csv", PHASE_COLUMNS, phases)


def _verify_run_database(
    database_path: Path,
    *,
    entry: RunPlanEntry,
    timing: EffectiveTiming,
    model_required: bool,
) -> dict[str, int]:
    expected = expected_window_count(
        timing.total_seconds, device_count=entry.device_count
    )
    with sqlite3.connect(database_path) as connection:
        counts = {
            "telemetry_messages": int(
                connection.execute(
                    "SELECT COUNT(*) FROM mqtt_observations WHERE message_type='telemetry'"
                ).fetchone()[0]
            ),
            "feature_windows": int(
                connection.execute("SELECT COUNT(*) FROM feature_windows").fetchone()[0]
            ),
            "predictions": int(
                connection.execute("SELECT COUNT(*) FROM window_predictions").fetchone()[0]
            ),
            "alerts": int(
                connection.execute("SELECT COUNT(*) FROM anomaly_alerts").fetchone()[0]
            ),
        }
    if counts["telemetry_messages"] <= 0:
        raise CampaignRunError("run contains no telemetry")
    if counts["feature_windows"] != expected:
        raise CampaignRunError(
            f"feature-window count mismatch: expected {expected}, got {counts['feature_windows']}"
        )
    if model_required and expected and counts["predictions"] <= 0:
        raise CampaignRunError("test run contains no persisted predictions")
    return counts


def _wait_for_analytics_completion(
    database_path: Path,
    *,
    entry: RunPlanEntry,
    timing: EffectiveTiming,
    model_required: bool,
    timeout_seconds: float,
    clock: SystemClock,
) -> None:
    """Wait for Analytics' final watermark-safe pass instead of assuming timing."""

    expected = expected_window_count(
        timing.total_seconds, device_count=entry.device_count
    )
    deadline = clock.monotonic() + timeout_seconds
    last_state = "derived tables unavailable"
    while clock.monotonic() < deadline:
        try:
            with sqlite3.connect(database_path, timeout=2.0) as connection:
                feature_count = int(
                    connection.execute("SELECT COUNT(*) FROM feature_windows").fetchone()[0]
                )
                prediction_count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM window_predictions"
                    ).fetchone()[0]
                )
            last_state = (
                f"feature_windows={feature_count}/{expected}, "
                f"predictions={prediction_count}"
            )
            predictions_ready = (
                not model_required or expected == 0 or prediction_count > 0
            )
            if feature_count == expected and predictions_ready:
                return
        except sqlite3.Error as exc:
            last_state = str(exc)
        clock.sleep(min(0.25, max(0.0, deadline - clock.monotonic())))
    raise CampaignRunError(f"analytics finalization timed out: {last_state}")


@dataclass(slots=True)
class CampaignRunner:
    repository_root: Path
    results_root: Path
    compose_file: Path
    project_name: str = "iot-ch4"
    startup_timeout_seconds: float = 120.0
    settle_grace_seconds: float = 3.0
    build: bool = False
    smoke_duration_seconds: float | None = None
    retry_failed: bool = False
    model_directory: Path | None = None
    trainer_command: tuple[str, ...] | None = None
    command_runner: CommandRunner = _default_command_runner
    clock: SystemClock = SystemClock()

    def _model_ready(self) -> bool:
        return bool(
            self.model_directory
            and all((self.model_directory / name).is_file() for name in MODEL_FILES)
        )

    def _ensure_model(self, environment: Mapping[str, str]) -> None:
        if self._model_ready():
            return
        if self.trainer_command:
            self.command_runner(
                self.trainer_command,
                cwd=self.repository_root,
                env=environment,
                timeout=None,
                check=True,
            )
        if not self._model_ready():
            expected = self.model_directory or self.results_root / "artifacts"
            raise CampaignRunError(
                f"test runs require trusted model files under {expected}"
            )

    def _container_path(self, host_path: Path) -> str:
        mount_root = (self.repository_root / "results").resolve()
        try:
            relative = host_path.resolve().relative_to(mount_root)
        except ValueError as exc:
            raise CampaignRunError(
                f"live result paths must be inside the Compose mount {mount_root}"
            ) from exc
        return "/app/results/" + relative.as_posix()

    def _container_results_root(self) -> str:
        mount_root = (self.repository_root / "results").resolve()
        try:
            relative = self.results_root.resolve().relative_to(mount_root)
        except ValueError as exc:
            raise CampaignRunError(
                f"--results-dir must be {mount_root} or one of its subdirectories"
            ) from exc
        return (
            "/app/results"
            if str(relative) == "."
            else "/app/results/" + relative.as_posix()
        )

    def run_entry(self, entry: RunPlanEntry) -> str:
        run_directory = self.results_root / "runs" / entry.run_id
        manifest = prepare_run_attempt(
            run_directory, entry, retry_failed=self.retry_failed
        )
        if manifest is None:
            return "skipped_completed"
        timing = effective_timing(entry, self.smoke_duration_seconds)
        if entry.split == "test":
            self._ensure_model(os.environ.copy())

        database_path = run_directory / "run.db"
        ready_path = run_directory / "analytics.ready.json"
        runtime_log = run_directory / "runtime.log"
        resource_rows: list[dict[str, str]] = []
        orchestration_rows: list[dict[str, Any]] = []
        collection_session_id = str(manifest["collection_session_id"])
        container_results_root = self._container_results_root()
        environment = os.environ.copy()
        environment.update(
            {
                "SQLITE_PATH": (
                    f"{container_results_root}/runs/{entry.run_id}/run.db"
                ),
                "SIMULATION_SEED": str(entry.simulation_seed),
                "EXPERIMENT_RUN_ID": entry.run_id,
                "COLLECTION_SESSION_ID": collection_session_id,
                "RUN_SCENARIO": entry.scenario,
                "DEVICE_COUNT": str(entry.device_count),
                "ANALYTICS_READY_FILE": (
                    f"{container_results_root}/runs/{entry.run_id}/analytics.ready.json"
                ),
            }
        )
        if entry.split == "test" and self.model_directory is not None:
            environment["MODEL_DIRECTORY"] = self._container_path(self.model_directory)
        else:
            environment.pop("MODEL_DIRECTORY", None)
        compose = ComposeClient(
            repository_root=self.repository_root,
            compose_file=self.compose_file,
            project_name=self.project_name,
            environment=environment,
            command_runner=self.command_runner,
            runtime_log=runtime_log,
        )
        model_version: str | None = None
        if entry.split == "test" and self.model_directory is not None:
            metadata = _read_json(self.model_directory / "model_metadata.json")
            model_version = str(metadata.get("model_version") or "") or None

        started_manifest = False
        completed = False
        try:
            compose.run(["down", "--remove-orphans"], check=False, timeout=60)
            up_base = [
                "up",
                "-d",
                "--wait",
                "--wait-timeout",
                str(int(self.startup_timeout_seconds)),
            ]
            if self.build:
                up_base.append("--build")
            compose.run(
                [*up_base, "mosquitto", "collector"],
                timeout=self.startup_timeout_seconds + 60,
            )
            # Collector readiness precedes the measured interval. Clients start
            # after the real run timestamp so their deterministic 0-5 second
            # initial spread remains part of the collected experiment.
            run_start = self.clock.wall()
            start_monotonic = self.clock.monotonic()
            run_end = run_start + timing.total_seconds
            environment["RUN_START_UNIX"] = f"{run_start:.6f}"
            environment["RUN_END_UNIX"] = f"{run_end:.6f}"
            compose.environment = environment
            update_run_manifest(
                run_directory / "run_manifest.json",
                status="running",
                changes={
                    "run_start_utc": utc_from_unix(run_start),
                    "effective_duration_seconds": timing.total_seconds,
                    "smoke_mode": timing.smoke_mode,
                    "attempt_number": int(manifest.get("attempt_number", 1)),
                    "model_version": model_version,
                    "output_files": list(
                        dict.fromkeys(
                            [
                                *manifest.get("output_files", []),
                                "orchestration_events.csv",
                                "run.db.collector-state.json",
                            ]
                        )
                    ),
                },
            )
            started_manifest = True
            orchestration_rows.append(
                _orchestration_row("run_start", 0.0, run_start, run_start)
            )
            compose.run(
                ["--profile", "experiment", "up", "-d", "analytics"],
                timeout=self.startup_timeout_seconds,
            )
            _wait_for_file(
                ready_path,
                timeout_seconds=self.startup_timeout_seconds,
                clock=self.clock,
            )
            analytics_ready = self.clock.wall()
            orchestration_rows.append(
                _orchestration_row(
                    "analytics_ready", 0.0, analytics_ready, run_start
                )
            )
            compose.run(
                [
                    "up",
                    "-d",
                    "--wait",
                    "--wait-timeout",
                    str(int(self.startup_timeout_seconds)),
                    "simulator",
                ],
                timeout=self.startup_timeout_seconds + 30,
            )
            _wait_for_devices(
                database_path,
                device_count=entry.device_count,
                timeout_seconds=self.startup_timeout_seconds,
                clock=self.clock,
            )
            devices_ready = self.clock.wall()
            orchestration_rows.append(
                _orchestration_row("devices_ready", 0.0, devices_ready, run_start)
            )

            next_sample = start_monotonic
            scenario_deadline = start_monotonic + timing.baseline_before_seconds
            end_deadline = start_monotonic + timing.total_seconds
            scenario_triggered = entry.scenario == "normal"
            while True:
                now_mono = self.clock.monotonic()
                if not scenario_triggered and now_mono >= scenario_deadline:
                    compose.run(
                        [
                            "--profile",
                            "tools",
                            "run",
                            "--rm",
                            "scenario",
                            "--device-id",
                            str(entry.target_device),
                            "--scenario",
                            entry.scenario,
                            "--duration-seconds",
                            f"{timing.anomaly_seconds:.6f}",
                            "--seed",
                            str(entry.simulation_seed),
                        ],
                        timeout=max(30.0, timing.anomaly_seconds + 20.0),
                    )
                    triggered_at = self.clock.wall()
                    orchestration_rows.append(
                        _orchestration_row(
                            "scenario_acknowledged",
                            timing.baseline_before_seconds,
                            triggered_at,
                            run_start,
                        )
                    )
                    scenario_triggered = True
                    now_mono = self.clock.monotonic()
                if now_mono >= next_sample:
                    resource_rows.extend(
                        _resource_sample(compose, recorded_at=self.clock.wall())
                    )
                    while next_sample <= now_mono:
                        next_sample += 1.0
                if now_mono >= end_deadline:
                    break
                wake_at = min(end_deadline, next_sample)
                if not scenario_triggered:
                    wake_at = min(wake_at, scenario_deadline)
                self.clock.sleep(max(0.0, wake_at - self.clock.monotonic()))

            actual_run_end = self.clock.wall()
            orchestration_rows.append(
                _orchestration_row(
                    "run_end", timing.total_seconds, actual_run_end, run_start
                )
            )
            self.clock.sleep(self.settle_grace_seconds)
            _wait_for_analytics_completion(
                database_path,
                entry=entry,
                timing=timing,
                model_required=entry.split == "test",
                timeout_seconds=self.startup_timeout_seconds,
                clock=self.clock,
            )
            scenario_bounds = (
                None
                if entry.scenario == "normal"
                else _actual_scenario_bounds(database_path, entry)
            )
            _write_actual_phases(
                run_directory,
                entry,
                run_start=run_start,
                run_end=actual_run_end,
                scenario_bounds=scenario_bounds,
            )
            counts = _verify_run_database(
                database_path,
                entry=entry,
                timing=timing,
                model_required=entry.split == "test",
            )
            write_csv_atomic(
                run_directory / "resource_samples.csv",
                RESOURCE_COLUMNS,
                resource_rows,
            )
            write_csv_atomic(
                run_directory / "orchestration_events.csv",
                ORCHESTRATION_COLUMNS,
                orchestration_rows,
            )
            update_run_manifest(
                run_directory / "run_manifest.json",
                status="completed",
                changes={
                    "run_end_utc": utc_from_unix(actual_run_end),
                    "observed_counts": counts,
                    "failure_reason": None,
                },
            )
            completed = True
            return "completed"
        except Exception as exc:
            if started_manifest:
                update_run_manifest(
                    run_directory / "run_manifest.json",
                    status="failed",
                    changes={
                        "run_end_utc": utc_from_unix(self.clock.wall()),
                        "failure_reason": f"{type(exc).__name__}: {exc}",
                    },
                )
            else:
                current = _read_json(run_directory / "run_manifest.json")
                if current.get("status") == "planned":
                    update_run_manifest(
                        run_directory / "run_manifest.json",
                        status="failed",
                        changes={
                            "run_end_utc": utc_from_unix(self.clock.wall()),
                            "failure_reason": f"{type(exc).__name__}: {exc}",
                        },
                    )
            raise
        finally:
            # Cleanup must never hide the primary run failure. Compose command
            # output, including service logs, is already appended to runtime.log.
            for cleanup_arguments, cleanup_timeout in (
                (
                    [
                        "stop",
                        "--timeout",
                        "15",
                        "analytics",
                        "simulator",
                        "collector",
                    ],
                    45,
                ),
                (["logs", "--no-color", "--timestamps"], 60),
                (["down", "--remove-orphans"], 60),
            ):
                try:
                    compose.run(
                        cleanup_arguments, check=False, timeout=cleanup_timeout
                    )
                except (OSError, subprocess.SubprocessError):
                    pass
            if not completed:
                if resource_rows:
                    write_csv_atomic(
                        run_directory / "resource_samples.csv",
                        RESOURCE_COLUMNS,
                        resource_rows,
                    )
                if orchestration_rows:
                    write_csv_atomic(
                        run_directory / "orchestration_events.csv",
                        ORCHESTRATION_COLUMNS,
                        orchestration_rows,
                    )


def _select_entries(
    entries: Sequence[RunPlanEntry], run_ids: Sequence[str]
) -> tuple[RunPlanEntry, ...]:
    if not run_ids:
        return tuple(entries)
    requested = set(run_ids)
    known = {entry.run_id for entry in entries}
    unknown = sorted(requested - known)
    if unknown:
        raise CampaignRunError(f"unknown run IDs: {unknown}")
    return tuple(entry for entry in entries if entry.run_id in requested)


def _dry_run_summary(
    entries: Sequence[RunPlanEntry], smoke_duration_seconds: float | None
) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for entry in entries:
        timing = effective_timing(entry, smoke_duration_seconds)
        summary.append(
            {
                "run_id": entry.run_id,
                "split": entry.split,
                "scenario": entry.scenario,
                "seed": entry.simulation_seed,
                "target_device": entry.target_device,
                "timing": {
                    "total_seconds": timing.total_seconds,
                    "baseline_before_seconds": timing.baseline_before_seconds,
                    "anomaly_seconds": timing.anomaly_seconds,
                    "baseline_after_seconds": timing.baseline_after_seconds,
                },
                "database": f"results/runs/{entry.run_id}/run.db",
            }
        )
    return summary


def _refresh_campaign_manifest(
    results_root: Path, entries: Sequence[RunPlanEntry]
) -> dict[str, Any]:
    """Derive campaign status from immutable per-run manifests."""

    path = results_root / "campaign_manifest.json"
    campaign = _read_json(path)
    status_counts: dict[str, int] = {}
    completed_ids: list[str] = []
    failed_ids: list[str] = []
    for entry in entries:
        manifest = _read_json(
            results_root / "runs" / entry.run_id / "run_manifest.json"
        )
        status = str(manifest.get("status", "unknown"))
        status_counts[status] = status_counts.get(status, 0) + 1
        if status == "completed":
            completed_ids.append(entry.run_id)
        elif status in {"failed", "aborted"}:
            failed_ids.append(entry.run_id)
    if len(completed_ids) == len(entries):
        status = "completed"
    elif failed_ids:
        status = "failed"
    elif completed_ids or status_counts.get("running", 0):
        status = "running"
    else:
        status = "planned"
    campaign.update(
        {
            "status": status,
            "updated_at_utc": utc_from_unix(time.time()),
            "status_counts": status_counts,
            "completed_run_ids": completed_ids,
            "failed_run_ids": failed_ids,
        }
    )
    write_json_atomic(path, campaign)
    return campaign


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", default="experiments/run_plan.csv")
    parser.add_argument("--split", default="experiments/split_manifest.json")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--compose-file", default="docker-compose.yml")
    parser.add_argument("--project-name", default="iot-ch4")
    parser.add_argument("--run-id", action="append", default=[])
    parser.add_argument("--smoke-duration-seconds", type=float)
    parser.add_argument("--startup-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--settle-grace-seconds", type=float, default=3.0)
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--model-directory")
    parser.add_argument(
        "--trainer-command",
        help="Optional argv string run once when the first test needs a model.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    repository_root = Path.cwd().resolve()
    plan_path = (repository_root / arguments.plan).resolve()
    split_path = (repository_root / arguments.split).resolve()
    results_root = (repository_root / arguments.results_dir).resolve()
    entries = _select_entries(load_run_plan(plan_path), arguments.run_id)
    if arguments.dry_run:
        print(json.dumps(_dry_run_summary(entries, arguments.smoke_duration_seconds), indent=2))
        return 0
    if not (results_root / "campaign_manifest.json").is_file():
        initialize_campaign(
            plan_path=plan_path,
            split_path=split_path,
            results_root=results_root,
            repository_root=repository_root,
        )
    model_directory = (
        Path(arguments.model_directory).resolve()
        if arguments.model_directory
        else results_root / "artifacts"
    )
    runner = CampaignRunner(
        repository_root=repository_root,
        results_root=results_root,
        compose_file=(repository_root / arguments.compose_file).resolve(),
        project_name=arguments.project_name,
        startup_timeout_seconds=arguments.startup_timeout_seconds,
        settle_grace_seconds=arguments.settle_grace_seconds,
        build=arguments.build,
        smoke_duration_seconds=arguments.smoke_duration_seconds,
        retry_failed=arguments.retry_failed,
        model_directory=model_directory,
        trainer_command=(
            None
            if not arguments.trainer_command
            else tuple(shlex.split(arguments.trainer_command))
        ),
    )
    failures: list[str] = []
    all_entries = load_run_plan(plan_path)
    _refresh_campaign_manifest(results_root, all_entries)
    for entry in entries:
        try:
            outcome = runner.run_entry(entry)
            print(f"{entry.run_id}: {outcome}")
        except Exception as exc:  # retain the exact failed-run evidence
            failures.append(entry.run_id)
            print(f"{entry.run_id}: failed: {exc}")
            _refresh_campaign_manifest(results_root, all_entries)
            if not arguments.continue_on_error:
                break
        else:
            _refresh_campaign_manifest(results_root, all_entries)
    if failures:
        print(f"Campaign stopped with failed runs: {', '.join(failures)}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
