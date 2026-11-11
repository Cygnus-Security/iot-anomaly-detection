"""Canonical Chapter 4 run plan, split, and phase-schedule contracts."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{2,63}$")
DEVICE_ID_PATTERN = re.compile(r"^iot-(00[1-9]|01[0-2])$")
SPLITS = {"train", "validation", "test"}
SCENARIOS = {
    "normal",
    "message_burst",
    "abnormal_interval",
    "abnormal_payload",
}

RUN_PLAN_COLUMNS = (
    "run_id",
    "split",
    "scenario",
    "simulation_seed",
    "duration_seconds",
    "target_device",
    "device_count",
    "baseline_before_seconds",
    "anomaly_seconds",
    "baseline_after_seconds",
)

CANONICAL_RUN_IDS = (
    "train-normal-01",
    "train-normal-02",
    "validation-normal-01",
    "test-normal-01",
    "test-k2-01",
    "test-k2-02",
    "test-k2-03",
    "test-k3-01",
    "test-k3-02",
    "test-k3-03",
    "test-k4-01",
    "test-k4-02",
    "test-k4-03",
)


class PlanValidationError(ValueError):
    """Raised when a campaign plan or split violates the frozen contract."""


def _integer(mapping: Mapping[str, Any], key: str) -> int:
    try:
        return int(mapping[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise PlanValidationError(f"{key} must be an integer") from exc


@dataclass(frozen=True, slots=True)
class RunPlanEntry:
    """One independent collection run from the frozen Chapter 4 campaign."""

    run_id: str
    split: str
    scenario: str
    simulation_seed: int
    duration_seconds: int
    target_device: str | None
    device_count: int
    baseline_before_seconds: int
    anomaly_seconds: int
    baseline_after_seconds: int

    def __post_init__(self) -> None:
        if not RUN_ID_PATTERN.fullmatch(self.run_id):
            raise PlanValidationError(f"invalid run_id: {self.run_id!r}")
        if self.split not in SPLITS:
            raise PlanValidationError(f"invalid split for {self.run_id}: {self.split}")
        if self.scenario not in SCENARIOS:
            raise PlanValidationError(
                f"invalid scenario for {self.run_id}: {self.scenario}"
            )
        if self.simulation_seed < -(2**31) or self.simulation_seed > 2**31 - 1:
            raise PlanValidationError(
                f"simulation_seed is outside signed 32-bit range for {self.run_id}"
            )
        if self.duration_seconds <= 0:
            raise PlanValidationError(
                f"duration_seconds must be positive for {self.run_id}"
            )
        if self.device_count != 12:
            raise PlanValidationError(
                f"device_count must be 12 for the frozen campaign: {self.run_id}"
            )
        phase_total = (
            self.baseline_before_seconds
            + self.anomaly_seconds
            + self.baseline_after_seconds
        )
        if phase_total != self.duration_seconds:
            raise PlanValidationError(
                f"phase durations do not sum to run duration for {self.run_id}"
            )
        if self.scenario == "normal":
            if self.target_device is not None:
                raise PlanValidationError(
                    f"normal run {self.run_id} must not have a target device"
                )
            if self.anomaly_seconds or self.baseline_after_seconds:
                raise PlanValidationError(
                    f"normal run {self.run_id} must contain one normal phase"
                )
            if self.baseline_before_seconds != self.duration_seconds:
                raise PlanValidationError(
                    f"normal run {self.run_id} must be normal for its full duration"
                )
        else:
            if self.split != "test":
                raise PlanValidationError(
                    f"abnormal run {self.run_id} must belong to the test split"
                )
            if not self.target_device or not DEVICE_ID_PATTERN.fullmatch(
                self.target_device
            ):
                raise PlanValidationError(
                    f"abnormal run {self.run_id} requires a target iot-001..iot-012"
                )
            if min(
                self.baseline_before_seconds,
                self.anomaly_seconds,
                self.baseline_after_seconds,
            ) <= 0:
                raise PlanValidationError(
                    f"abnormal run {self.run_id} requires three non-empty phases"
                )

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "RunPlanEntry":
        target = str(mapping.get("target_device", "")).strip() or None
        return cls(
            run_id=str(mapping.get("run_id", "")).strip(),
            split=str(mapping.get("split", "")).strip(),
            scenario=str(mapping.get("scenario", "")).strip(),
            simulation_seed=_integer(mapping, "simulation_seed"),
            duration_seconds=_integer(mapping, "duration_seconds"),
            target_device=target,
            device_count=_integer(mapping, "device_count"),
            baseline_before_seconds=_integer(
                mapping, "baseline_before_seconds"
            ),
            anomaly_seconds=_integer(mapping, "anomaly_seconds"),
            baseline_after_seconds=_integer(mapping, "baseline_after_seconds"),
        )

    def to_mapping(self) -> dict[str, Any]:
        row = asdict(self)
        row["target_device"] = self.target_device or ""
        return row


@dataclass(frozen=True, slots=True)
class PhasePlan:
    """One planned phase for the target device of a collection run."""

    phase_id: str
    run_id: str
    phase_name: str
    scenario: str
    ground_truth_label: str
    target_device: str | None
    planned_start_offset_seconds: int
    planned_end_offset_seconds: int

    def to_mapping(self) -> dict[str, Any]:
        row = asdict(self)
        row["target_device"] = self.target_device or ""
        return row


def phase_schedule(entry: RunPlanEntry) -> tuple[PhasePlan, ...]:
    """Return the frozen target-device phase schedule for one run."""

    if entry.scenario == "normal":
        return (
            PhasePlan(
                phase_id=f"{entry.run_id}-normal",
                run_id=entry.run_id,
                phase_name="normal",
                scenario="normal",
                ground_truth_label="normal",
                target_device=None,
                planned_start_offset_seconds=0,
                planned_end_offset_seconds=entry.duration_seconds,
            ),
        )

    before_end = entry.baseline_before_seconds
    anomaly_end = before_end + entry.anomaly_seconds
    return (
        PhasePlan(
            phase_id=f"{entry.run_id}-baseline-before",
            run_id=entry.run_id,
            phase_name="baseline_before",
            scenario="normal",
            ground_truth_label="normal",
            target_device=entry.target_device,
            planned_start_offset_seconds=0,
            planned_end_offset_seconds=before_end,
        ),
        PhasePlan(
            phase_id=f"{entry.run_id}-anomaly",
            run_id=entry.run_id,
            phase_name="anomaly",
            scenario=entry.scenario,
            ground_truth_label="anomaly",
            target_device=entry.target_device,
            planned_start_offset_seconds=before_end,
            planned_end_offset_seconds=anomaly_end,
        ),
        PhasePlan(
            phase_id=f"{entry.run_id}-baseline-after",
            run_id=entry.run_id,
            phase_name="baseline_after",
            scenario="normal",
            ground_truth_label="normal",
            target_device=entry.target_device,
            planned_start_offset_seconds=anomaly_end,
            planned_end_offset_seconds=entry.duration_seconds,
        ),
    )


def load_run_plan(path: str | Path) -> tuple[RunPlanEntry, ...]:
    plan_path = Path(path)
    with plan_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != RUN_PLAN_COLUMNS:
            raise PlanValidationError(
                "run plan columns must exactly match the frozen schema"
            )
        entries = tuple(RunPlanEntry.from_mapping(row) for row in reader)
    validate_run_plan(entries)
    return entries


def validate_run_plan(
    entries: Sequence[RunPlanEntry], *, require_canonical: bool = True
) -> None:
    if not entries:
        raise PlanValidationError("run plan is empty")
    run_ids = [entry.run_id for entry in entries]
    if len(run_ids) != len(set(run_ids)):
        raise PlanValidationError("run plan contains duplicate run IDs")
    seeds = [entry.simulation_seed for entry in entries]
    if len(seeds) != len(set(seeds)):
        raise PlanValidationError("run plan contains duplicate simulation seeds")
    if require_canonical and tuple(run_ids) != CANONICAL_RUN_IDS:
        raise PlanValidationError(
            "run plan does not match the frozen 13-run order and identifiers"
        )
    if require_canonical:
        if sum(entry.scenario == "normal" for entry in entries) != 4:
            raise PlanValidationError("canonical plan requires four normal runs")
        for scenario in (
            "message_burst",
            "abnormal_interval",
            "abnormal_payload",
        ):
            if sum(entry.scenario == scenario for entry in entries) != 3:
                raise PlanValidationError(
                    f"canonical plan requires three runs for {scenario}"
                )


def load_split_manifest(path: str | Path) -> dict[str, tuple[str, ...]]:
    with Path(path).open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict) or raw.get("schema_version") != "1.0":
        raise PlanValidationError("split manifest schema_version must be '1.0'")
    splits = raw.get("splits")
    if not isinstance(splits, dict):
        raise PlanValidationError("split manifest must contain a splits object")
    parsed: dict[str, tuple[str, ...]] = {}
    for split in SPLITS:
        value = splits.get(split)
        if not isinstance(value, list) or not all(
            isinstance(run_id, str) for run_id in value
        ):
            raise PlanValidationError(f"split {split} must be a list of run IDs")
        parsed[split] = tuple(value)
    if set(splits) != SPLITS:
        raise PlanValidationError("split manifest contains unsupported split names")
    return parsed


def validate_split_manifest(
    splits: Mapping[str, Sequence[str]], entries: Sequence[RunPlanEntry]
) -> None:
    if set(splits) != SPLITS:
        raise PlanValidationError("split names must be train, validation, and test")
    all_split_ids = [run_id for split in SPLITS for run_id in splits[split]]
    if len(all_split_ids) != len(set(all_split_ids)):
        raise PlanValidationError("a run appears in more than one split")
    plan_by_id = {entry.run_id: entry for entry in entries}
    if set(all_split_ids) != set(plan_by_id):
        missing = sorted(set(plan_by_id) - set(all_split_ids))
        unknown = sorted(set(all_split_ids) - set(plan_by_id))
        raise PlanValidationError(
            f"split manifest and plan differ; missing={missing}, unknown={unknown}"
        )
    for split, run_ids in splits.items():
        for run_id in run_ids:
            if plan_by_id[run_id].split != split:
                raise PlanValidationError(
                    f"run {run_id} is declared {plan_by_id[run_id].split}, not {split}"
                )
