from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path
from typing import Mapping, Sequence

import pytest

from src.experiments.functional import (
    FUNCTIONAL_COLUMNS,
    FUNCTIONAL_SPECS,
    redact_output,
    run_functional_matrix,
)


def executor_with_failures(*failed_ids: str):
    def execute(
        arguments: Sequence[str], cwd: Path, environment: Mapping[str, str]
    ) -> subprocess.CompletedProcess[str]:
        del cwd, environment
        failed = any(value in " ".join(arguments) for value in failed_ids)
        return subprocess.CompletedProcess(
            arguments,
            1 if failed else 0,
            "failed\n" if failed else "passed\n",
            "",
        )

    return execute


def test_matrix_has_exactly_one_spec_for_t01_through_t16() -> None:
    assert [spec.test_id for spec in FUNCTIONAL_SPECS] == [
        f"T{index:02d}" for index in range(1, 17)
    ]


def test_report_marks_unexecuted_live_checks_not_run(tmp_path: Path) -> None:
    results = tmp_path / "results"
    rows = run_functional_matrix(
        repository_root=tmp_path,
        results_directory=results,
        executor=executor_with_failures(),
        code_version="abc123",
    )
    by_id = {row["test_id"]: row for row in rows}
    assert by_id["T01"]["status"] == "not_run"
    assert by_id["T03"]["status"] == "pass"
    assert by_id["T04"]["status"] == "not_run"
    assert by_id["T11"]["status"] == "pass"
    assert by_id["T16"]["status"] == "not_run"
    with (results / "functional_tests.csv").open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert tuple(reader.fieldnames or ()) == FUNCTIONAL_COLUMNS
        assert len(list(reader)) == 16


def test_live_and_integration_evidence_can_complete_the_matrix(tmp_path: Path) -> None:
    results = tmp_path / "results"
    evidence = results / "evidence" / "live.log"
    evidence.parent.mkdir(parents=True)
    evidence.write_text("verified live lab\n", encoding="utf-8")
    live_path = results / "evidence" / "live.json"
    required = {"T01", "T02", "T09", "T10", "T16"}
    live_path.write_text(
        json.dumps(
            {
                test_id: {
                    "status": "pass",
                    "observed_result": "Live condition verified.",
                    "evidence_path": "evidence/live.log",
                }
                for test_id in required
            }
        ),
        encoding="utf-8",
    )
    rows = run_functional_matrix(
        repository_root=tmp_path,
        results_directory=results,
        live_evidence_path=live_path,
        include_integration=True,
        executor=executor_with_failures(),
        code_version="abc123",
    )
    assert {row["status"] for row in rows} == {"pass"}


def test_automated_failure_cannot_be_overridden_by_live_pass(tmp_path: Path) -> None:
    results = tmp_path / "results"
    proof = results / "evidence" / "proof.log"
    proof.parent.mkdir(parents=True)
    proof.write_text("claim\n", encoding="utf-8")
    live_path = results / "evidence" / "live.json"
    live_path.write_text(
        json.dumps(
            {
                "T11": {
                    "status": "pass",
                    "observed_result": "claimed pass",
                    "evidence_path": "evidence/proof.log",
                }
            }
        ),
        encoding="utf-8",
    )
    rows = run_functional_matrix(
        repository_root=tmp_path,
        results_directory=results,
        live_evidence_path=live_path,
        executor=executor_with_failures("test_score_rule_is_strictly_below_threshold"),
        code_version="abc123",
    )
    assert next(row for row in rows if row["test_id"] == "T11")["status"] == "fail"


def test_live_evidence_must_exist_inside_results(tmp_path: Path) -> None:
    results = tmp_path / "results"
    results.mkdir()
    outside = tmp_path / "outside.log"
    outside.write_text("no\n", encoding="utf-8")
    live_path = results / "live.json"
    live_path.write_text(
        json.dumps(
            {
                "T01": {
                    "status": "pass",
                    "evidence_path": str(outside),
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="outside results"):
        run_functional_matrix(
            repository_root=tmp_path,
            results_directory=results,
            live_evidence_path=live_path,
            executor=executor_with_failures(),
        )


def test_log_redaction_removes_environment_and_inline_secrets() -> None:
    output = redact_output(
        "DEVICE_PASSWORD=super-secret token=abc123 value super-secret",
        {"DEVICE_PASSWORD": "super-secret"},
    )
    assert "super-secret" not in output
    assert "abc123" not in output
    assert output.count("[REDACTED]") >= 2
