"""Execute and record the Chapter IV T01-T16 functional evidence matrix.

The report is deliberately evidence-driven: a source file or feature name is
never sufficient for a pass.  Automated entries are backed by an executed
pytest process, while lab-only checks require explicit live evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from src.experiments.artifacts import write_csv_atomic

FUNCTIONAL_COLUMNS = (
    "test_id",
    "executed_at_utc",
    "code_version",
    "status",
    "observed_result",
    "evidence_path",
)
VALID_STATUSES = {"pass", "fail", "not_run"}


@dataclass(frozen=True, slots=True)
class FunctionalSpec:
    test_id: str
    description: str
    unit_nodes: tuple[str, ...] = ()
    integration_nodes: tuple[str, ...] = ()
    requires_live_evidence: bool = False


FUNCTIONAL_SPECS = (
    FunctionalSpec("T01", "Environment startup", requires_live_evidence=True),
    FunctionalSpec("T02", "Twelve distinct MQTT devices", requires_live_evidence=True),
    FunctionalSpec(
        "T03",
        "Schema, topic, identity, and payload-size validation",
        (
            "tests/unit/test_messages.py::test_decode_and_validate_telemetry",
            "tests/unit/test_messages.py::test_rejects_payload_topic_identity_mismatch",
            "tests/unit/test_storage.py::test_store_preserves_invalid_payload_with_validation_error",
            "tests/unit/test_storage.py::test_payload_over_limit_is_not_retained_or_registered_as_device",
        ),
    ),
    FunctionalSpec(
        "T04",
        "Broker authentication and ACL enforcement",
        integration_nodes=(
            "tests/integration/test_mqtt_flow.py::test_invalid_password_is_rejected",
            "tests/integration/test_mqtt_flow.py::test_per_device_and_collector_acls_reject_unauthorized_writes",
        ),
    ),
    FunctionalSpec(
        "T05",
        "QoS application-key deduplication",
        (
            "tests/unit/test_storage.py::test_application_duplicate_is_kept_but_excluded_from_timing",
            "tests/unit/test_storage.py::test_mqtt_dup_flag_does_not_discard_first_application_key",
            "tests/unit/test_features.py::test_only_valid_session_aware_canonical_nonretained_telemetry_enters_window",
        ),
    ),
    FunctionalSpec(
        "T06",
        "Status/retained behavior and analytics exclusion",
        (
            "tests/unit/test_storage.py::test_retained_status_does_not_regress_newer_device_timestamp",
            "tests/unit/test_features.py::test_only_valid_session_aware_canonical_nonretained_telemetry_enters_window",
        ),
        (
            "tests/integration/test_mqtt_flow.py::test_retained_status_is_delivered_to_a_late_subscriber",
        ),
    ),
    FunctionalSpec(
        "T07",
        "Half-open boundaries and hand-checkable feature math",
        (
            "tests/unit/test_features.py::test_half_open_window_and_exact_feature_math",
            "tests/unit/test_features.py::test_peak_bucket_is_anchored_to_window_start",
        ),
    ),
    FunctionalSpec(
        "T08",
        "Insufficient-data windows remain unscored",
        (
            "tests/unit/test_features.py::test_scheduled_windows_are_run_anchored_and_include_silent_devices",
            "tests/unit/test_features.py::test_only_valid_session_aware_canonical_nonretained_telemetry_enters_window",
        ),
    ),
    FunctionalSpec(
        "T09",
        "Asynchronous close and collection-loss flagging",
        (
            "tests/unit/test_async_integrity.py::test_pending_pre_boundary_message_blocks_window_until_persisted",
            "tests/unit/test_async_integrity.py::test_dropped_telemetry_marks_intersecting_window_as_observation_gap",
        ),
        requires_live_evidence=True,
    ),
    FunctionalSpec(
        "T10", "Online/offline vector and prediction equivalence", requires_live_evidence=True
    ),
    FunctionalSpec(
        "T11",
        "Trusted model loading, schema checks, and strict threshold rule",
        (
            "tests/unit/test_ml.py::test_score_rule_is_strictly_below_threshold",
            "tests/unit/test_ml.py::test_fit_save_load_and_checksum_contract",
            "tests/unit/test_ml.py::test_feature_inputs_must_be_complete_and_finite",
        ),
    ),
    FunctionalSpec(
        "T12",
        "All predictions are persisted idempotently",
        (
            "tests/unit/test_analytics.py::test_persists_all_windows_predictions_and_dashboard_alert_contract",
            "tests/unit/test_analytics.py::test_reprocessing_is_idempotent_and_does_not_duplicate_alerts",
        ),
    ),
    FunctionalSpec(
        "T13",
        "Run/session isolation and immutable completed evidence",
        (
            "tests/unit/test_campaign_runner.py::test_retry_archives_failed_evidence_and_completed_runs_are_immutable",
            "tests/unit/test_campaign_runner.py::test_runner_completes_a_normal_smoke_attempt_with_injected_runtime",
        ),
    ),
    FunctionalSpec(
        "T14",
        "Read-only dashboard over the live SQLite contract",
        (
            "tests/unit/test_dashboard_data.py::test_dashboard_queries_live_observations_without_inventing_detections",
            "tests/unit/test_dashboard_data.py::test_dashboard_exposes_all_predictions_and_actual_alert_counts",
        ),
    ),
    FunctionalSpec(
        "T15",
        "Actual scenario boundaries stay separate from model features",
        (
            "tests/unit/test_analytics.py::test_scenario_truth_uses_actual_start_and_end_events",
            "tests/unit/test_analytics.py::test_incomplete_scenario_log_marks_truth_unknown",
            "tests/unit/test_messages.py::test_telemetry_rejects_ground_truth_at_any_object_level",
        ),
    ),
    FunctionalSpec(
        "T16",
        "Disconnect, stale status, reconnect, and Last Will",
        unit_nodes=(
            "tests/unit/test_dashboard_data.py::test_dashboard_marks_stale_then_recovers_after_new_telemetry",
        ),
        integration_nodes=(
            "tests/integration/test_mqtt_flow.py::test_ungraceful_disconnect_publishes_retained_last_will",
            "tests/integration/test_mqtt_flow.py::test_disconnect_and_recovery_publish_observable_status_sequence",
        ),
    ),
)

Executor = Callable[
    [Sequence[str], Path, Mapping[str, str]], subprocess.CompletedProcess[str]
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _default_executor(
    arguments: Sequence[str], cwd: Path, environment: Mapping[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(arguments),
        cwd=cwd,
        env=dict(environment),
        capture_output=True,
        text=True,
        check=False,
    )


def _code_version(repository_root: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository_root,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return completed.stdout.strip() or "unknown"


def redact_output(value: str, environment: Mapping[str, str]) -> str:
    """Remove credential values and obvious key/value secrets from test output."""

    redacted = value
    for key, secret in environment.items():
        if (
            re.search(r"password|passwd|secret|token|api[_-]?key", key, re.I)
            and len(secret) >= 4
        ):
            redacted = redacted.replace(secret, "[REDACTED]")
    return re.sub(
        r"(?i)\b(password|passwd|secret|token|api[_-]?key)\s*[=:]\s*[^\s,;]+",
        lambda match: f"{match.group(1)}=[REDACTED]",
        redacted,
    )


def _single_line(value: object) -> str:
    return " ".join(str(value).split())


def _load_live_evidence(
    path: Path | None, *, results_root: Path
) -> dict[str, dict[str, str]]:
    if path is None:
        return {}
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("live evidence must be a JSON object keyed by T01-T16")
    output: dict[str, dict[str, str]] = {}
    known = {spec.test_id for spec in FUNCTIONAL_SPECS}
    for test_id, raw in decoded.items():
        if test_id not in known or not isinstance(raw, dict):
            raise ValueError(f"invalid live evidence entry: {test_id!r}")
        status = str(raw.get("status", ""))
        if status not in VALID_STATUSES:
            raise ValueError(f"invalid live evidence status for {test_id}: {status!r}")
        evidence_value = str(raw.get("evidence_path", "")).strip()
        evidence_path = path if not evidence_value else Path(evidence_value)
        if not evidence_path.is_absolute():
            evidence_path = (results_root / evidence_path).resolve()
        else:
            evidence_path = evidence_path.resolve()
        try:
            relative = evidence_path.relative_to(results_root.resolve())
        except ValueError as exc:
            raise ValueError(
                f"live evidence for {test_id} is outside results: {evidence_path}"
            ) from exc
        if not evidence_path.is_file():
            raise ValueError(f"live evidence file does not exist: {evidence_path}")
        output[test_id] = {
            "status": status,
            "observed_result": _single_line(raw.get("observed_result", "")),
            "evidence_path": relative.as_posix(),
        }
    return output


def run_functional_matrix(
    *,
    repository_root: str | Path,
    results_directory: str | Path,
    live_evidence_path: str | Path | None = None,
    include_integration: bool = False,
    executor: Executor = _default_executor,
    code_version: str | None = None,
) -> list[dict[str, str]]:
    """Execute available checks and write a truthful T01-T16 CSV report."""

    repository = Path(repository_root).resolve()
    results_root = Path(results_directory).resolve()
    evidence_root = results_root / "evidence" / "functional"
    evidence_root.mkdir(parents=True, exist_ok=True)
    live_path = None if live_evidence_path is None else Path(live_evidence_path).resolve()
    live = _load_live_evidence(live_path, results_root=results_root)
    version = code_version or _code_version(repository)
    executed_at = utc_now()
    rows: list[dict[str, str]] = []

    for spec in FUNCTIONAL_SPECS:
        nodes = list(spec.unit_nodes)
        integration_missing = bool(spec.integration_nodes) and not include_integration
        if include_integration:
            nodes.extend(spec.integration_nodes)
        automated_status: str | None = None
        automated_result = ""
        evidence_paths: list[str] = []
        if nodes:
            environment = os.environ.copy()
            if any(node in spec.integration_nodes for node in nodes):
                environment["RUN_MQTT_INTEGRATION"] = "1"
            command = [sys.executable, "-m", "pytest", "-q", *nodes]
            completed = executor(command, repository, environment)
            log_path = evidence_root / f"{spec.test_id.lower()}-pytest.log"
            output = "\n".join(
                part for part in (completed.stdout, completed.stderr) if part
            )
            log_path.write_text(
                redact_output(
                    f"command: {' '.join(command)}\nexit_code: {completed.returncode}\n{output}",
                    environment,
                ).rstrip()
                + "\n",
                encoding="utf-8",
            )
            automated_status = "pass" if completed.returncode == 0 else "fail"
            automated_result = (
                f"Executed {len(nodes)} pytest check(s); exit code "
                f"{completed.returncode}."
            )
            evidence_paths.append(log_path.relative_to(results_root).as_posix())

        live_entry = live.get(spec.test_id)
        if live_entry is not None:
            evidence_paths.append(live_entry["evidence_path"])

        if automated_status == "fail" or (
            live_entry is not None and live_entry["status"] == "fail"
        ):
            status = "fail"
        elif spec.requires_live_evidence and live_entry is None:
            status = "not_run"
        elif integration_missing and live_entry is None:
            status = "not_run"
        elif live_entry is not None and live_entry["status"] == "not_run":
            status = "not_run"
        elif automated_status == "pass" or (
            live_entry is not None and live_entry["status"] == "pass"
        ):
            status = "pass"
        else:
            status = "not_run"

        result_parts = [part for part in (automated_result,) if part]
        if integration_missing and live_entry is None:
            result_parts.append("Live broker integration was not executed.")
        if spec.requires_live_evidence and live_entry is None:
            result_parts.append("Required live-lab evidence was not supplied.")
        if live_entry is not None and live_entry["observed_result"]:
            result_parts.append(live_entry["observed_result"])
        if not result_parts:
            result_parts.append("No executable evidence was available.")
        rows.append(
            {
                "test_id": spec.test_id,
                "executed_at_utc": executed_at,
                "code_version": version,
                "status": status,
                "observed_result": " ".join(result_parts),
                "evidence_path": ";".join(dict.fromkeys(evidence_paths)),
            }
        )

    write_csv_atomic(results_root / "functional_tests.csv", FUNCTIONAL_COLUMNS, rows)
    return rows


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", default=".")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--live-evidence")
    parser.add_argument(
        "--include-integration",
        action="store_true",
        help="Run MQTT integration nodes; broker credentials must already be in the environment.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    rows = run_functional_matrix(
        repository_root=arguments.repository_root,
        results_directory=arguments.results_dir,
        live_evidence_path=arguments.live_evidence,
        include_integration=arguments.include_integration,
    )
    counts = {status: 0 for status in sorted(VALID_STATUSES)}
    for row in rows:
        counts[row["status"]] += 1
    print(json.dumps({"report": "functional_tests.csv", "counts": counts}, sort_keys=True))
    return 1 if counts["fail"] else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
