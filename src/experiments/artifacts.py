"""Safe, traceable result-artifact handling for Chapter 4 campaigns."""

from __future__ import annotations

import csv
import importlib.metadata
import json
import os
import platform
import re
import sqlite3
import subprocess
import sys
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from src.experiments.planning import (
    RUN_PLAN_COLUMNS,
    PhasePlan,
    RunPlanEntry,
    load_run_plan,
    load_split_manifest,
    phase_schedule,
    validate_split_manifest,
)

RESULT_FILE_NAMES = (
    "run_manifest.json",
    "messages.csv",
    "scenario_phases.csv",
    "feature_windows.csv",
    "predictions.csv",
    "alerts.csv",
    "resource_samples.csv",
    "timing_samples.csv",
    "runtime.log",
)

PHASE_COLUMNS = (
    "phase_id",
    "run_id",
    "phase_name",
    "scenario",
    "ground_truth_label",
    "target_device",
    "planned_start_offset_seconds",
    "planned_end_offset_seconds",
    "actual_start_utc",
    "actual_end_utc",
    "status",
)

SECRET_KEY_PATTERN = re.compile(
    r"(?:password|passwd|secret|token|api[_-]?key|private[_-]?key|credential)",
    re.IGNORECASE,
)

STATUS_TRANSITIONS: dict[str, set[str]] = {
    "planned": {"running", "failed", "aborted"},
    "running": {"completed", "failed", "aborted"},
    "completed": set(),
    "failed": set(),
    "aborted": set(),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def expected_window_count(
    duration_seconds: float,
    *,
    device_count: int = 12,
    window_seconds: float = 20.0,
    stride_seconds: float = 5.0,
) -> int:
    """Return scheduled full windows across devices after the warm-up period."""

    if duration_seconds < 0:
        raise ValueError("duration_seconds must be non-negative")
    if device_count < 1:
        raise ValueError("device_count must be positive")
    if window_seconds <= 0 or stride_seconds <= 0:
        raise ValueError("window and stride must be positive")
    if duration_seconds < window_seconds:
        return 0
    per_device = int((duration_seconds - window_seconds) // stride_seconds) + 1
    return per_device * device_count


def redact_secrets(value: Any) -> Any:
    """Recursively redact values whose key names can contain credentials."""

    if isinstance(value, Mapping):
        return {
            str(key): (
                "[REDACTED]"
                if SECRET_KEY_PATTERN.search(str(key))
                else redact_secrets(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    if isinstance(value, tuple):
        return [redact_secrets(item) for item in value]
    return value


def _atomic_target(destination: Path) -> tuple[Any, Path]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        delete=False,
    )
    return handle, Path(handle.name)


def write_json_atomic(destination: str | Path, value: Any) -> Path:
    """Write UTF-8 JSON by atomic replacement within the destination directory."""

    path = Path(destination)
    handle, temporary = _atomic_target(path)
    try:
        with handle:
            json.dump(
                redact_secrets(value),
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return path


def write_csv_atomic(
    destination: str | Path,
    columns: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> Path:
    """Write an RFC 4180-compatible UTF-8 CSV by atomic replacement."""

    path = Path(destination)
    handle, temporary = _atomic_target(path)
    try:
        with handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=list(columns),
                extrasaction="raise",
                lineterminator="\r\n",
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return path


def _command_output(arguments: Sequence[str], cwd: Path | None = None) -> str | None:
    try:
        completed = subprocess.run(
            arguments,
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    return completed.stdout.strip() or completed.stderr.strip() or None


def _distribution_version(distribution: str) -> str | None:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


def _physical_memory_bytes() -> int | None:
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        page_count = int(os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, TypeError, ValueError):
        return None
    return page_size * page_count


def _docker_runtime() -> dict[str, Any] | None:
    rendered = _command_output(
        (
            "docker",
            "info",
            "--format",
            (
                '{"server_version":{{json .ServerVersion}},'
                '"operating_system":{{json .OperatingSystem}},'
                '"architecture":{{json .Architecture}},'
                '"logical_cpu_count":{{.NCPU}},'
                '"memory_bytes":{{.MemTotal}}}'
            ),
        )
    )
    if not rendered:
        return None
    try:
        decoded = json.loads(rendered)
    except json.JSONDecodeError:
        return {"capture_error": "docker info returned non-JSON output"}
    return decoded if isinstance(decoded, dict) else None


def _docker_image(image: str) -> dict[str, Any] | None:
    rendered = _command_output(
        (
            "docker",
            "image",
            "inspect",
            image,
            "--format",
            '{"id":{{json .Id}},"created":{{json .Created}}}',
        )
    )
    if not rendered:
        return None
    try:
        decoded = json.loads(rendered)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def capture_environment(
    *,
    repository_root: str | Path = ".",
    effective_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Capture reproducibility metadata without enumerating process environment."""

    root = Path(repository_root).resolve()
    commit = _command_output(("git", "rev-parse", "HEAD"), root)
    git_status = _command_output(("git", "status", "--porcelain"), root)
    packages = {
        name: _distribution_version(name)
        for name in (
            "numpy",
            "scipy",
            "paho-mqtt",
            "pandas",
            "streamlit",
            "scikit-learn",
            "joblib",
            "matplotlib",
        )
    }
    return {
        "schema_version": "1.0",
        "captured_at_utc": utc_now(),
        "machine": {
            "hostname": platform.node(),
            "os": platform.platform(),
            "architecture": platform.machine(),
            "processor": platform.processor() or None,
            "logical_cpu_count": os.cpu_count(),
            "physical_memory_bytes": _physical_memory_bytes(),
        },
        "runtime": {
            "python": sys.version.split()[0],
            "python_implementation": platform.python_implementation(),
            "sqlite": sqlite3.sqlite_version,
            "packages": packages,
            "docker": _command_output(("docker", "--version")),
            "docker_compose": _command_output(("docker", "compose", "version")),
            "docker_runtime": _docker_runtime(),
            "docker_images": {
                "application": _docker_image("iot-anomaly-detection:mvp"),
                "mosquitto": _docker_image("iot-anomaly-mosquitto:2.1.2"),
            },
        },
        "source": {
            "repository_root": str(root),
            "commit": commit or "unknown",
            "worktree_dirty": bool(git_status),
        },
        "effective_config": redact_secrets(dict(effective_config or {})),
    }


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Traceability record created before a run starts."""

    schema_version: str
    run_id: str
    collection_session_id: str
    split: str
    scenario: str
    simulation_seed: int
    duration_seconds: int
    device_count: int
    target_device: str | None
    run_configuration: dict[str, Any]
    code_version: str
    status: str
    created_at_utc: str
    run_start_utc: str | None
    run_end_utc: str | None
    window_seconds: int
    stride_seconds: int
    min_messages: int
    feature_schema_version: str
    model_version: str | None
    failure_reason: str | None
    output_files: tuple[str, ...]

    @classmethod
    def planned(cls, entry: RunPlanEntry, *, code_version: str) -> "RunManifest":
        scenario_parameters: dict[str, Any]
        if entry.scenario == "message_burst":
            scenario_parameters = {"target_rate_hz": 5.0}
        elif entry.scenario == "abnormal_interval":
            scenario_parameters = {
                "short_interval_seconds": 1.0,
                "long_interval_seconds": 9.0,
            }
        elif entry.scenario == "abnormal_payload":
            scenario_parameters = {"diagnostic_bytes": 1024}
        else:
            scenario_parameters = {}
        return cls(
            schema_version="1.0",
            run_id=entry.run_id,
            collection_session_id=str(uuid.uuid4()),
            split=entry.split,
            scenario=entry.scenario,
            simulation_seed=entry.simulation_seed,
            duration_seconds=entry.duration_seconds,
            device_count=entry.device_count,
            target_device=entry.target_device,
            run_configuration={
                "mqtt_protocol": "3.1.1",
                "telemetry_qos": 1,
                "telemetry_retained": False,
                "max_payload_bytes": 4096,
                "base_interval_seconds": 5.0,
                "interval_jitter_seconds": 0.5,
                "initial_phase_spread_seconds": 5.0,
                "scenario_parameters": scenario_parameters,
            },
            code_version=code_version,
            status="planned",
            created_at_utc=utc_now(),
            run_start_utc=None,
            run_end_utc=None,
            window_seconds=20,
            stride_seconds=5,
            min_messages=3,
            feature_schema_version="1.0",
            model_version=None,
            failure_reason=None,
            output_files=RESULT_FILE_NAMES,
        )

    def to_mapping(self) -> dict[str, Any]:
        value = asdict(self)
        value["output_files"] = list(self.output_files)
        return value


def _safe_run_path(results_root: Path, run_id: str) -> Path:
    from src.experiments.planning import RUN_ID_PATTERN

    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"unsafe run ID: {run_id!r}")
    runs_root = (results_root / "runs").resolve()
    candidate = (runs_root / run_id).resolve()
    if candidate.parent != runs_root:
        raise ValueError("run directory escapes results/runs")
    return candidate


def _phase_rows(phases: Sequence[PhasePlan]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase in phases:
        row = phase.to_mapping()
        row.update(
            {
                "actual_start_utc": "",
                "actual_end_utc": "",
                "status": "planned",
            }
        )
        rows.append(row)
    return rows


def prepare_run_directory(
    results_root: str | Path,
    entry: RunPlanEntry,
    *,
    code_version: str,
) -> Path:
    """Create a new run directory and refuse to overwrite previous evidence."""

    root = Path(results_root).resolve()
    run_path = _safe_run_path(root, entry.run_id)
    if run_path.exists() and any(run_path.iterdir()):
        raise FileExistsError(f"run results already exist: {run_path}")
    run_path.mkdir(parents=True, exist_ok=True)
    manifest = RunManifest.planned(entry, code_version=code_version)
    write_json_atomic(run_path / "run_manifest.json", manifest.to_mapping())
    write_csv_atomic(
        run_path / "scenario_phases.csv",
        PHASE_COLUMNS,
        _phase_rows(phase_schedule(entry)),
    )
    return run_path


def update_run_manifest(
    manifest_path: str | Path,
    *,
    status: str,
    changes: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Advance a run status without permitting identity mutation or rollback."""

    path = Path(manifest_path)
    with path.open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    current = manifest.get("status")
    if current not in STATUS_TRANSITIONS:
        raise ValueError(f"unknown current run status: {current!r}")
    if status != current and status not in STATUS_TRANSITIONS[current]:
        raise ValueError(f"invalid run status transition: {current} -> {status}")
    updates = dict(changes or {})
    protected = {"run_id", "collection_session_id", "schema_version", "split"}
    if protected.intersection(updates):
        raise ValueError("run identity fields cannot be changed")
    manifest.update(updates)
    manifest["status"] = status
    write_json_atomic(path, manifest)
    return manifest


def initialize_campaign(
    *,
    plan_path: str | Path,
    split_path: str | Path,
    results_root: str | Path,
    repository_root: str | Path = ".",
    effective_config: Mapping[str, Any] | None = None,
) -> Path:
    """Create a new complete campaign skeleton under one empty results folder."""

    entries = load_run_plan(plan_path)
    splits = load_split_manifest(split_path)
    validate_split_manifest(splits, entries)

    root = Path(results_root).resolve()
    if root.exists() and any(root.iterdir()):
        raise FileExistsError(
            f"results directory is not empty; refusing to overwrite: {root}"
        )
    root.mkdir(parents=True, exist_ok=True)
    for name in ("runs", "figures", "screenshots", "evidence"):
        (root / name).mkdir(exist_ok=True)

    environment = capture_environment(
        repository_root=repository_root,
        effective_config=effective_config,
    )
    write_json_atomic(root / "environment.json", environment)
    write_csv_atomic(
        root / "run_plan.csv",
        RUN_PLAN_COLUMNS,
        (entry.to_mapping() for entry in entries),
    )
    write_json_atomic(
        root / "split_manifest.json",
        {"schema_version": "1.0", "splits": splits},
    )

    code_version = str(environment["source"]["commit"])
    for entry in entries:
        prepare_run_directory(root, entry, code_version=code_version)

    campaign = {
        "schema_version": "1.0",
        "created_at_utc": utc_now(),
        "status": "planned",
        "run_count": len(entries),
        "total_planned_duration_seconds": sum(
            entry.duration_seconds for entry in entries
        ),
        "expected_scheduled_windows": sum(
            expected_window_count(
                entry.duration_seconds, device_count=entry.device_count
            )
            for entry in entries
        ),
        "run_ids": [entry.run_id for entry in entries],
    }
    write_json_atomic(root / "campaign_manifest.json", campaign)
    return root
