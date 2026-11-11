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
import uuid
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
from src.collector.runtime_state import load_runtime_state
from src.ml import load_artifact

MODEL_FILES = ("model.joblib", "model_metadata.json", "artifact_manifest.json")
LIVE_DATABASE_PATH = "/app/experiment-data/run.db"
COMPOSE_PROFILES = ("--profile", "experiment", "--profile", "tools")
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

    def run_external(
        self,
        arguments: Sequence[str],
        *,
        check: bool = True,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run and log a Docker command which is not a Compose subcommand."""

        command = list(arguments)
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


def _archive_retry_evidence(run_directory: Path, manifest: dict[str, Any]) -> Path:
    """Move mutable evidence aside and preserve the prior manifest/phase plan."""

    attempt_number = int(manifest.get("attempt_number", 1))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    archive = run_directory / "attempts" / f"attempt-{attempt_number:02d}-{stamp}"
    archive.mkdir(parents=True, exist_ok=False)
    for name in (
        "run.db",
        "run.db-wal",
        "run.db-shm",
        "run.db-journal",
        "run.db.collector-state.json",
        "analytics.ready.json",
        "analytics-container.log",
        "integrity-report.json",
        "runtime.log",
        "resource_samples.csv",
        "timing_samples.csv",
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
    recovered_volume = run_directory / "runtime-volume-recovery"
    if recovered_volume.exists():
        shutil.move(str(recovered_volume), archive / recovered_volume.name)
    return archive


def prepare_run_attempt(
    run_directory: Path,
    entry: RunPlanEntry,
    *,
    retry_failed: bool,
    code_version: str | None = None,
    worktree_dirty: bool | None = None,
) -> dict[str, Any] | None:
    """Prepare a pending attempt; return ``None`` for a completed run."""

    manifest_path = run_directory / "run_manifest.json"
    manifest = _read_json(manifest_path)
    status = str(manifest.get("status"))
    if status == "completed":
        return None
    if status in {"running", "failed", "aborted"} and not retry_failed:
        raise CampaignRunError(
            f"{entry.run_id} is {status}; pass --retry-failed to archive and retry"
        )
    stale_files = any(
        (run_directory / name).exists()
        for name in (
            "run.db",
            "run.db-wal",
            "run.db-shm",
            "run.db-journal",
            "run.db.collector-state.json",
            "analytics.ready.json",
            "analytics-container.log",
            "integrity-report.json",
            "runtime-volume-recovery",
        )
    )
    if status == "planned" and stale_files:
        raise CampaignRunError(
            f"planned run {entry.run_id} contains stale runtime evidence"
        )
    if status in {"running", "failed", "aborted"}:
        prior: dict[str, Any] = {
            "attempt_number": int(manifest.get("attempt_number", 1)),
            "status": status,
            "run_start_utc": manifest.get("run_start_utc"),
            "run_end_utc": manifest.get("run_end_utc"),
            "failure_reason": manifest.get("failure_reason"),
            "code_version": manifest.get("code_version"),
            "worktree_dirty": manifest.get("worktree_dirty"),
            "collection_session_id": manifest.get("collection_session_id"),
            "model_version": manifest.get("model_version"),
            "runtime_volume_preserved": manifest.get("runtime_volume_preserved"),
            "runtime_volume_project": manifest.get("runtime_volume_project"),
            "runtime_volume_logical_name": manifest.get(
                "runtime_volume_logical_name"
            ),
            "archived_at_utc": utc_from_unix(time.time()),
        }
        archive = _archive_retry_evidence(run_directory, manifest)
        prior["archive_directory"] = str(archive.relative_to(run_directory))
        history = list(manifest.get("attempt_history", []))
        history.append(prior)
        manifest.update(
            status="planned",
            attempt_number=prior["attempt_number"] + 1,
            attempt_history=history,
            collection_session_id=str(uuid.uuid4()),
            run_start_utc=None,
            run_end_utc=None,
            failure_reason=None,
            model_version=None,
            runtime_volume_preserved=False,
            runtime_volume_project=None,
            runtime_volume_logical_name=None,
        )
        if code_version is not None:
            manifest["code_version"] = code_version
        if worktree_dirty is not None:
            manifest["worktree_dirty"] = worktree_dirty
        write_json_atomic(manifest_path, manifest)
        write_csv_atomic(
            run_directory / "scenario_phases.csv", PHASE_COLUMNS, _phase_rows(entry)
        )
    else:
        manifest.setdefault("attempt_number", 1)
        manifest.setdefault("attempt_history", [])
        if code_version is not None:
            manifest["code_version"] = code_version
        if worktree_dirty is not None:
            manifest["worktree_dirty"] = worktree_dirty
        write_json_atomic(manifest_path, manifest)
    return manifest


def _wait_for_devices(
    compose: ComposeClient,
    *,
    device_count: int,
    timeout_seconds: float,
    clock: SystemClock,
) -> None:
    """Wait using SQLite inside the Linux container, never from the macOS host."""

    query = (
        "import os,sqlite3;"
        "c=sqlite3.connect(os.environ['SQLITE_PATH'],timeout=2);"
        "print(c.execute(\"SELECT COUNT(DISTINCT device_id) "
        "FROM mqtt_observations WHERE message_type='telemetry' "
        "AND payload_valid=1 AND duplicate=0 AND retain=0\").fetchone()[0]);"
        "c.close()"
    )
    deadline = clock.monotonic() + timeout_seconds
    last_error = "collector query has not completed"
    while clock.monotonic() < deadline:
        completed = compose.run(
            [*COMPOSE_PROFILES, "exec", "-T", "collector", "python", "-c", query],
            check=False,
            timeout=10,
        )
        if completed.returncode == 0:
            try:
                observed = int((completed.stdout or "").strip().splitlines()[-1])
            except (IndexError, ValueError):
                last_error = "collector query returned no integer count"
            else:
                if observed == device_count:
                    return
                last_error = f"observed {observed}/{device_count} devices"
        else:
            last_error = (completed.stderr or completed.stdout or "query failed").strip()
        clock.sleep(min(0.5, max(0.0, deadline - clock.monotonic())))
    raise CampaignRunError(f"device readiness timed out: {last_error}")


def _analytics_state(compose: ComposeClient) -> dict[str, Any]:
    container = compose.run(
        [*COMPOSE_PROFILES, "ps", "--all", "-q", "analytics"],
        check=False,
        timeout=15,
    )
    identifiers = [line.strip() for line in (container.stdout or "").splitlines() if line.strip()]
    if len(identifiers) != 1:
        raise CampaignRunError("Analytics container is missing or ambiguous")
    inspected = compose.run_external(
        ["docker", "inspect", "--format", "{{json .State}}", identifiers[0]],
        check=False,
        timeout=15,
    )
    if inspected.returncode != 0:
        raise CampaignRunError(
            "Analytics state inspection failed: "
            + (inspected.stderr or inspected.stdout or "unknown error").strip()
        )
    try:
        state = json.loads((inspected.stdout or "").strip())
    except json.JSONDecodeError as exc:
        raise CampaignRunError("Analytics state inspection returned invalid JSON") from exc
    if not isinstance(state, dict):
        raise CampaignRunError("Analytics state inspection returned a non-object")
    return state


def _assert_analytics_running(compose: ComposeClient) -> None:
    state = _analytics_state(compose)
    status = str(state.get("Status", "unknown"))
    if status != "running":
        code = state.get("ExitCode")
        raise CampaignRunError(
            f"Analytics exited before run completion (status={status}, exit_code={code})"
        )


def _wait_for_analytics_ready(
    compose: ComposeClient,
    ready_path: Path,
    *,
    timeout_seconds: float,
    clock: SystemClock,
) -> None:
    deadline = clock.monotonic() + timeout_seconds
    while clock.monotonic() < deadline:
        if ready_path.is_file():
            return
        _assert_analytics_running(compose)
        clock.sleep(min(0.25, max(0.0, deadline - clock.monotonic())))
    raise CampaignRunError(f"readiness file timed out: {ready_path}")


def _wait_for_analytics_exit(
    compose: ComposeClient,
    *,
    timeout_seconds: float,
    clock: SystemClock,
) -> None:
    deadline = clock.monotonic() + timeout_seconds
    while clock.monotonic() < deadline:
        state = _analytics_state(compose)
        status = str(state.get("Status", "unknown"))
        if status == "exited":
            code = int(state.get("ExitCode", -1))
            if code != 0:
                raise CampaignRunError(f"Analytics exited with code {code}")
            return
        if status not in {"running", "created", "restarting"}:
            raise CampaignRunError(f"Analytics entered unexpected state {status}")
        clock.sleep(min(0.25, max(0.0, deadline - clock.monotonic())))
    raise CampaignRunError("Analytics did not exit after its final watermark-safe pass")


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
        for line in compose.run(
            [*COMPOSE_PROFILES, "ps", "-q"], check=False, timeout=15
        ).stdout.splitlines()
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


def _git_source_state(repository_root: Path) -> tuple[str | None, bool | None]:
    """Return the exact source revision used by a new attempt when available."""

    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None, None
    return (commit or None), bool(status.strip())


def _stop_runtime_services(
    compose: ComposeClient, *, require_clean_exit: bool = False
) -> None:
    stopped = compose.run(
        [
            *COMPOSE_PROFILES,
            "stop",
            "--timeout",
            "15",
            "analytics",
            "simulator",
            "collector",
        ],
        check=False,
        timeout=45,
    )
    if stopped.returncode != 0:
        raise CampaignRunError(
            "Compose could not stop the SQLite runtime services: "
            + (stopped.stderr or stopped.stdout or "unknown error").strip()
        )
    for service in ("analytics", "simulator", "collector"):
        listed = compose.run(
            [*COMPOSE_PROFILES, "ps", "--all", "-q", service],
            check=False,
            timeout=15,
        )
        if listed.returncode != 0:
            raise CampaignRunError(f"Could not verify stopped service {service}")
        for identifier in (
            line.strip()
            for line in (listed.stdout or "").splitlines()
            if line.strip()
        ):
            inspected = compose.run_external(
                ["docker", "inspect", "--format", "{{json .State}}", identifier],
                check=False,
                timeout=15,
            )
            if inspected.returncode != 0:
                raise CampaignRunError(f"Could not inspect stopped service {service}")
            try:
                state = json.loads((inspected.stdout or "").strip())
            except json.JSONDecodeError as exc:
                raise CampaignRunError(
                    f"Invalid state returned for stopped service {service}"
                ) from exc
            status = str(state.get("Status", "unknown")) if isinstance(state, dict) else "unknown"
            if not isinstance(state, dict) or bool(state.get("Running")) or status not in {
                "exited",
                "dead",
            }:
                raise CampaignRunError(
                    f"Service {service} is still active after Compose stop"
                )
            if require_clean_exit and int(state.get("ExitCode", -1)) != 0:
                raise CampaignRunError(
                    f"Service {service} exited uncleanly with code "
                    f"{state.get('ExitCode')}"
                )


def _capture_analytics_log(compose: ComposeClient, destination: Path) -> None:
    completed = compose.run(
        [
            *COMPOSE_PROFILES,
            "logs",
            "--no-color",
            "--timestamps",
            "analytics",
        ],
        check=False,
        timeout=60,
    )
    output = (completed.stdout or "") + (completed.stderr or "")
    if output.strip():
        destination.write_text(output, encoding="utf-8")


def _collector_container_id(compose: ComposeClient) -> str | None:
    completed = compose.run(
        [*COMPOSE_PROFILES, "ps", "--all", "-q", "collector"],
        check=False,
        timeout=15,
    )
    identifiers = [line.strip() for line in (completed.stdout or "").splitlines() if line.strip()]
    return identifiers[0] if len(identifiers) == 1 else None


def _validate_runtime_volume(compose: ComposeClient) -> dict[str, Any]:
    """Checkpoint and validate the stopped Linux-local volume before copying it."""

    script = (
        "import json,os,sqlite3;"
        "c=sqlite3.connect(os.environ['SQLITE_PATH'],timeout=30);"
        "checkpoint=c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall();"
        "integrity=[r[0] for r in c.execute('PRAGMA integrity_check').fetchall()];"
        "c.close();"
        "print(json.dumps({'checkpoint':checkpoint,'integrity_check':integrity},sort_keys=True));"
        "checkpoint_clear=all(int(r[0])==0 for r in checkpoint);"
        "raise SystemExit(0 if integrity==['ok'] and checkpoint_clear else 2)"
    )
    completed = compose.run(
        [
            *COMPOSE_PROFILES,
            "run",
            "--rm",
            "--no-deps",
            "collector",
            "python",
            "-c",
            script,
        ],
        check=False,
        timeout=90,
    )
    output_lines = [line for line in (completed.stdout or "").splitlines() if line.strip()]
    details: dict[str, Any] = {
        "status": "failed" if completed.returncode else "ok",
        "returncode": completed.returncode,
    }
    if output_lines:
        try:
            parsed = json.loads(output_lines[-1])
        except json.JSONDecodeError:
            details["stdout"] = completed.stdout.strip()
        else:
            if isinstance(parsed, dict):
                details.update(parsed)
    if completed.stderr:
        details["stderr"] = completed.stderr.strip()
    return details


def _copy_runtime_artifacts(
    compose: ComposeClient,
    run_directory: Path,
    *,
    required: bool,
) -> bool:
    """Copy a closed runtime DB from the named volume through its stopped owner."""

    container_id = _collector_container_id(compose)
    if container_id is None:
        if required:
            raise CampaignRunError("Collector container is unavailable for DB snapshot")
        return False
    copied_database = False
    for name, mandatory in (
        ("run.db", True),
        ("run.db-wal", False),
        ("run.db-shm", False),
        ("run.db-journal", False),
        ("run.db.collector-state.json", True),
    ):
        destination = run_directory / name
        if destination.exists():
            copied_database = copied_database or name == "run.db"
            continue
        completed = compose.run_external(
            [
                "docker",
                "cp",
                f"{container_id}:/app/experiment-data/{name}",
                str(destination),
            ],
            check=False,
            timeout=60,
        )
        if completed.returncode == 0:
            copied_database = copied_database or name == "run.db"
        elif mandatory and required:
            raise CampaignRunError(
                "Failed to copy runtime database: "
                + (completed.stderr or completed.stdout or "unknown error").strip()
            )
    return copied_database


def _validate_collector_state(path: Path) -> dict[str, Any]:
    """Require a clean, lossless Collector shutdown before accepting a run."""

    snapshot = load_runtime_state(path)
    if snapshot is None:
        raise CampaignRunError("Collector runtime-state sidecar is missing or invalid")
    state = {
        "accepting_messages": snapshot.accepting_messages,
        "queue_depth": snapshot.queue_depth,
        "oldest_pending_received_at_unix": snapshot.oldest_pending_received_at_unix,
        "last_enqueued_at_unix": snapshot.last_enqueued_at_unix,
        "last_persisted_received_at_unix": snapshot.last_persisted_received_at_unix,
        "dropped_message_count": snapshot.dropped_message_count,
        "loss_event_count": len(snapshot.loss_events),
    }
    if snapshot.accepting_messages:
        raise CampaignRunError("Collector was still accepting messages at snapshot time")
    if snapshot.queue_depth != 0 or snapshot.oldest_pending_received_at_unix is not None:
        raise CampaignRunError("Collector queue was not drained before snapshot")
    if snapshot.dropped_message_count != 0 or snapshot.loss_events:
        raise CampaignRunError("Collector reported message loss; run evidence is rejected")
    return state


def _write_integrity_report(
    database_path: Path,
    report_path: Path,
    *,
    container_check: Mapping[str, Any] | None,
    collector_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "checked_at_utc": utc_from_unix(time.time()),
        "database": database_path.name,
        "database_size_bytes": database_path.stat().st_size,
        "host_sqlite_version": sqlite3.sqlite_version,
        "container_check": dict(container_check or {}),
        "collector_state": dict(collector_state or {}),
    }
    try:
        uri = f"file:{database_path.resolve()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=30.0) as connection:
            quick = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
            integrity = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
        report["quick_check"] = quick
        report["integrity_check"] = integrity
        report["status"] = "ok" if quick == ["ok"] and integrity == ["ok"] else "failed"
    except sqlite3.Error as exc:
        report.update(status="failed", error=f"{type(exc).__name__}: {exc}")
    write_json_atomic(report_path, report)
    if report["status"] != "ok" or (
        container_check is not None and container_check.get("status") != "ok"
    ):
        raise CampaignRunError("SQLite integrity validation failed; run evidence is rejected")
    return report


def _remove_runtime_project(compose: ComposeClient) -> None:
    removed = compose.run(
        [*COMPOSE_PROFILES, "down", "--remove-orphans", "--volumes"],
        check=False,
        timeout=60,
    )
    if removed.returncode != 0:
        raise CampaignRunError(
            "Compose could not remove the prior runtime project and volume: "
            + (removed.stderr or removed.stdout or "unknown error").strip()
        )


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
        if self.model_directory is None or not all(
            (self.model_directory / name).is_file() for name in MODEL_FILES
        ):
            return False
        try:
            artifact = load_artifact(self.model_directory, trusted=True)
            manifest = _read_json(self.model_directory / "artifact_manifest.json")
        except (OSError, ValueError, RuntimeError, CampaignRunError):
            return False
        return str(manifest.get("model_version") or "") == artifact.metadata.model_version

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
        existing = _read_json(run_directory / "run_manifest.json")
        existing_status = str(existing.get("status"))
        source_commit, worktree_dirty = _git_source_state(self.repository_root)

        # A retry can only archive evidence after any stale writer has stopped.
        # If a previous runner died before snapshotting its named volume, recover
        # that closed DB first. Explicit --retry-failed is required before this
        # cleanup can affect a possibly still-active `running` attempt.
        if existing_status in {"running", "failed", "aborted"} and self.retry_failed:
            stale_environment = os.environ.copy()
            stale_environment["SQLITE_PATH"] = LIVE_DATABASE_PATH
            stale_compose = ComposeClient(
                repository_root=self.repository_root,
                compose_file=self.compose_file,
                project_name=self.project_name,
                environment=stale_environment,
                command_runner=self.command_runner,
                runtime_log=run_directory / "runtime.log",
            )
            _stop_runtime_services(stale_compose)
            _capture_analytics_log(
                stale_compose, run_directory / "analytics-container.log"
            )
            if _collector_container_id(stale_compose):
                recovery_directory = run_directory / "runtime-volume-recovery"
                recovery_directory.mkdir(parents=True, exist_ok=True)
                stale_check = _validate_runtime_volume(stale_compose)
                _copy_runtime_artifacts(
                    stale_compose, recovery_directory, required=True
                )
                try:
                    recovered_state = _validate_collector_state(
                        recovery_directory / "run.db.collector-state.json"
                    )
                except CampaignRunError as exc:
                    recovered_state = {"validation_error": str(exc)}
                try:
                    _write_integrity_report(
                        recovery_directory / "run.db",
                        recovery_directory / "integrity-report.json",
                        container_check=stale_check,
                        collector_state=recovered_state,
                    )
                except CampaignRunError:
                    # Invalid evidence remains valuable for provenance. It is
                    # copied before the live volume is removed and is never an
                    # eligible model/evaluation input.
                    pass
            stale_compose.run(
                [*COMPOSE_PROFILES, "logs", "--no-color", "--timestamps"],
                check=False,
                timeout=60,
            )
            _remove_runtime_project(stale_compose)

        manifest = prepare_run_attempt(
            run_directory,
            entry,
            retry_failed=self.retry_failed,
            code_version=source_commit,
            worktree_dirty=worktree_dirty,
        )
        if manifest is None:
            return "skipped_completed"
        timing = effective_timing(entry, self.smoke_duration_seconds)

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
                # SQLite WAL remains wholly inside the Docker Linux VM. The
                # stopped, checkpointed DB is copied to results after each run.
                "SQLITE_PATH": LIVE_DATABASE_PATH,
                "COLLECTOR_STATE_FILE": f"{LIVE_DATABASE_PATH}.collector-state.json",
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

        started_manifest = False
        completed = False
        runtime_started = False
        runtime_stopped = False
        runtime_snapshot_verified = False
        container_check: dict[str, Any] | None = None
        try:
            if entry.split == "test":
                self._ensure_model(environment)
                if self.model_directory is None:  # defensive; _ensure_model raises
                    raise CampaignRunError("test run has no model directory")
                metadata = _read_json(self.model_directory / "model_metadata.json")
                model_version = str(metadata.get("model_version") or "") or None
            _remove_runtime_project(compose)
            up_base = [
                "up",
                "-d",
                "--wait",
                "--wait-timeout",
                str(int(self.startup_timeout_seconds)),
            ]
            if self.build:
                up_base.append("--build")
            runtime_started = True
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
                                "run.db",
                                "integrity-report.json",
                                "analytics-container.log",
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
            _wait_for_analytics_ready(
                compose,
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
                compose,
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
                if now_mono < end_deadline:
                    _assert_analytics_running(compose)
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
            _wait_for_analytics_exit(
                compose,
                timeout_seconds=self.startup_timeout_seconds,
                clock=self.clock,
            )
            _stop_runtime_services(compose, require_clean_exit=True)
            runtime_stopped = True
            container_check = _validate_runtime_volume(compose)
            _copy_runtime_artifacts(compose, run_directory, required=True)
            collector_state = _validate_collector_state(
                run_directory / "run.db.collector-state.json"
            )
            _write_integrity_report(
                database_path,
                run_directory / "integrity-report.json",
                container_check=container_check,
                collector_state=collector_state,
            )
            runtime_snapshot_verified = True
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
        except KeyboardInterrupt:
            current = _read_json(run_directory / "run_manifest.json")
            if current.get("status") in {"planned", "running"}:
                update_run_manifest(
                    run_directory / "run_manifest.json",
                    status="aborted",
                    changes={
                        "run_end_utc": utc_from_unix(self.clock.wall()),
                        "failure_reason": "KeyboardInterrupt: campaign interrupted",
                    },
                )
            raise
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
            # Preserve a closed snapshot even for failed/aborted attempts, but
            # never allow cleanup failures to hide the primary exception.
            if runtime_started and not runtime_stopped:
                try:
                    _stop_runtime_services(compose)
                    runtime_stopped = True
                except (CampaignRunError, OSError, subprocess.SubprocessError):
                    pass
            if runtime_started and runtime_stopped and not runtime_snapshot_verified:
                try:
                    container_check = _validate_runtime_volume(compose)
                    _copy_runtime_artifacts(compose, run_directory, required=True)
                    collector_state = _validate_collector_state(
                        run_directory / "run.db.collector-state.json"
                    )
                    _write_integrity_report(
                        database_path,
                        run_directory / "integrity-report.json",
                        container_check=container_check,
                        collector_state=collector_state,
                    )
                    runtime_snapshot_verified = True
                except (CampaignRunError, OSError, subprocess.SubprocessError):
                    pass
            try:
                _capture_analytics_log(
                    compose, run_directory / "analytics-container.log"
                )
                compose.run(
                    [*COMPOSE_PROFILES, "logs", "--no-color", "--timestamps"],
                    check=False,
                    timeout=60,
                )
            except (OSError, subprocess.SubprocessError):
                pass
            if not runtime_started or runtime_snapshot_verified:
                try:
                    _remove_runtime_project(compose)
                except (CampaignRunError, OSError, subprocess.SubprocessError):
                    pass
            elif (run_directory / "run_manifest.json").is_file():
                current = _read_json(run_directory / "run_manifest.json")
                current_status = str(current.get("status"))
                if current_status in {"failed", "aborted"}:
                    update_run_manifest(
                        run_directory / "run_manifest.json",
                        status=current_status,
                        changes={
                            "runtime_volume_preserved": True,
                            "runtime_volume_project": self.project_name,
                            "runtime_volume_logical_name": "experiment_data",
                        },
                    )
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
    try:
        for entry in entries:
            try:
                outcome = runner.run_entry(entry)
                print(f"{entry.run_id}: {outcome}")
            except Exception as exc:  # retain the exact failed-run evidence
                failures.append(entry.run_id)
                print(f"{entry.run_id}: failed: {exc}")
                if not arguments.continue_on_error:
                    break
    finally:
        # KeyboardInterrupt is a BaseException: run_entry marks it aborted and
        # this final refresh keeps the campaign manifest consistent.
        _refresh_campaign_manifest(results_root, all_entries)
    if failures:
        print(f"Campaign stopped with failed runs: {', '.join(failures)}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
