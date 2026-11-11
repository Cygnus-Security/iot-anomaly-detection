"""Versioned contracts shared by Isolation Forest training and inference."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

FEATURE_SCHEMA_VERSION = "1.0"
FEATURE_NAMES = (
    "message_count",
    "message_rate_hz",
    "payload_mean_bytes",
    "payload_std_bytes",
    "interarrival_mean_ms",
    "interarrival_std_ms",
    "peak_messages_per_second",
)
WINDOW_SECONDS = 20
STRIDE_SECONDS = 5
MIN_MESSAGES = 3
DEFAULT_THRESHOLD_QUANTILE = 0.01
SCORE_DIRECTION = "lower_is_more_anomalous"


class MlContractError(ValueError):
    """Raised when ML inputs, splits, or artifacts violate the frozen contract."""


def _run_ids(value: Any, name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise MlContractError(f"{name} must be a list of run IDs")
    result = tuple(str(item).strip() for item in value)
    if not result or any(not item for item in result):
        raise MlContractError(f"{name} must contain non-empty run IDs")
    if len(result) != len(set(result)):
        raise MlContractError(f"{name} contains duplicate run IDs")
    return result


@dataclass(frozen=True, slots=True)
class SplitManifest:
    """Leakage-safe run allocation for train, validation, and final test."""

    train_run_ids: tuple[str, ...]
    validation_run_ids: tuple[str, ...]
    test_run_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        groups = {
            "train": set(self.train_run_ids),
            "validation": set(self.validation_run_ids),
            "test": set(self.test_run_ids),
        }
        for name, run_ids in groups.items():
            if not run_ids:
                raise MlContractError(f"{name} split must contain at least one run")
            if len(run_ids) != len(getattr(self, f"{name}_run_ids")):
                raise MlContractError(f"{name} split contains duplicate run IDs")
        overlaps = {
            "train/validation": groups["train"] & groups["validation"],
            "train/test": groups["train"] & groups["test"],
            "validation/test": groups["validation"] & groups["test"],
        }
        conflicting = {
            pair: sorted(run_ids) for pair, run_ids in overlaps.items() if run_ids
        }
        if conflicting:
            raise MlContractError(f"run splits overlap: {conflicting}")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SplitManifest":
        """Create and validate a manifest decoded from JSON."""

        return cls(
            train_run_ids=_run_ids(value.get("train_run_ids"), "train_run_ids"),
            validation_run_ids=_run_ids(
                value.get("validation_run_ids"), "validation_run_ids"
            ),
            test_run_ids=_run_ids(value.get("test_run_ids"), "test_run_ids"),
        )

    def as_dict(self) -> dict[str, list[str]]:
        return {
            "train_run_ids": list(self.train_run_ids),
            "validation_run_ids": list(self.validation_run_ids),
            "test_run_ids": list(self.test_run_ids),
        }
