"""Shared offline/online windowing and communication-feature implementation.

All time values in this module are collector Unix timestamps. Device-provided
timestamps and experiment labels are deliberately absent from ``FeatureVector``.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Iterable, Iterator, Literal, Mapping, Sequence

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

FeatureStatus = Literal[
    "ok",
    "insufficient_data",
    "observation_gap",
    "invalid_feature",
]
GroundTruthLabel = Literal["normal", "anomaly", "transition", "unknown"]


@dataclass(frozen=True, slots=True)
class WindowConfig:
    """Versioned window contract shared by dataset and inference paths."""

    window_seconds: float = 20.0
    stride_seconds: float = 5.0
    min_messages: int = 3
    feature_schema_version: str = FEATURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not math.isfinite(self.window_seconds) or self.window_seconds <= 0:
            raise ValueError("window_seconds must be finite and positive")
        if not math.isfinite(self.stride_seconds) or self.stride_seconds <= 0:
            raise ValueError("stride_seconds must be finite and positive")
        if self.min_messages < 2:
            raise ValueError("min_messages must be at least two for inter-arrival data")
        if self.feature_schema_version != FEATURE_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported feature schema {self.feature_schema_version!r}"
            )


@dataclass(frozen=True, slots=True)
class TelemetryObservation:
    """Minimal raw-observation fields required to construct a feature window."""

    observation_id: int
    device_id: str
    received_at_unix: float
    payload_size_bytes: int
    session_id: str | None
    message_type: str = "telemetry"
    payload_valid: bool = True
    duplicate: bool = False
    retain: bool = False

    def __post_init__(self) -> None:
        if not self.device_id:
            raise ValueError("device_id must not be empty")
        if not math.isfinite(self.received_at_unix):
            raise ValueError("received_at_unix must be finite")
        if self.payload_size_bytes < 0:
            raise ValueError("payload_size_bytes must be non-negative")

    @property
    def is_canonical_telemetry(self) -> bool:
        """Return whether this row may enter a Phase 3 feature window."""

        return (
            self.message_type == "telemetry"
            and self.payload_valid
            and not self.duplicate
            and not self.retain
            and bool(self.session_id)
        )


@dataclass(frozen=True, slots=True)
class ObservationGap:
    """Half-open receipt-time interval affected by known collection loss."""

    start_unix: float
    end_unix: float
    reason: str = "observation_gap"

    def __post_init__(self) -> None:
        if not (
            math.isfinite(self.start_unix)
            and math.isfinite(self.end_unix)
            and self.start_unix < self.end_unix
        ):
            raise ValueError("observation gap must have finite start < end")
        if not self.reason:
            raise ValueError("observation gap reason must not be empty")

    def intersects(self, start_unix: float, end_unix: float) -> bool:
        return max(self.start_unix, start_unix) < min(self.end_unix, end_unix)


@dataclass(frozen=True, slots=True)
class ScenarioPhase:
    """Actual anomaly phase for one target device in one experiment run."""

    phase_id: str
    target_device_id: str
    start_unix: float
    end_unix: float

    def __post_init__(self) -> None:
        if not self.phase_id or not self.target_device_id:
            raise ValueError("phase_id and target_device_id must not be empty")
        if not (
            math.isfinite(self.start_unix)
            and math.isfinite(self.end_unix)
            and self.start_unix < self.end_unix
        ):
            raise ValueError("scenario phase must have finite start < end")


@dataclass(frozen=True, slots=True)
class GroundTruth:
    """Evaluation metadata kept outside the model feature vector."""

    label: GroundTruthLabel
    phase_id: str | None
    is_target_device: bool


@dataclass(frozen=True, slots=True)
class FeatureVector:
    """The exact ordered feature schema consumed by Isolation Forest."""

    message_count: int
    message_rate_hz: float
    payload_mean_bytes: float
    payload_std_bytes: float
    interarrival_mean_ms: float
    interarrival_std_ms: float
    peak_messages_per_second: int

    def as_tuple(self) -> tuple[float, ...]:
        return (
            float(self.message_count),
            self.message_rate_hz,
            self.payload_mean_bytes,
            self.payload_std_bytes,
            self.interarrival_mean_ms,
            self.interarrival_std_ms,
            float(self.peak_messages_per_second),
        )

    def as_dict(self) -> dict[str, int | float]:
        return {
            "message_count": self.message_count,
            "message_rate_hz": self.message_rate_hz,
            "payload_mean_bytes": self.payload_mean_bytes,
            "payload_std_bytes": self.payload_std_bytes,
            "interarrival_mean_ms": self.interarrival_mean_ms,
            "interarrival_std_ms": self.interarrival_std_ms,
            "peak_messages_per_second": self.peak_messages_per_second,
        }


@dataclass(frozen=True, slots=True)
class FeatureWindow:
    """One scheduled device window, including rows that cannot be scored."""

    run_id: str
    collection_session_id: str
    device_id: str
    window_index: int
    window_start_unix: float
    window_end_unix: float
    observed_message_count: int
    feature_status: FeatureStatus
    exclusion_reason: str | None
    features: FeatureVector | None
    ground_truth: GroundTruth

    @property
    def window_id(self) -> str:
        return f"{self.run_id}:{self.device_id}:{self.window_index:06d}"

    @property
    def can_score(self) -> bool:
        return self.feature_status == "ok" and self.features is not None

    def as_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "run_id": self.run_id,
            "collection_session_id": self.collection_session_id,
            "device_id": self.device_id,
            "window_id": self.window_id,
            "window_index": self.window_index,
            "window_start_unix": self.window_start_unix,
            "window_end_unix": self.window_end_unix,
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "observed_message_count": self.observed_message_count,
            "feature_status": self.feature_status,
            "exclusion_reason": self.exclusion_reason,
            "ground_truth_label": self.ground_truth.label,
            "phase_id": self.ground_truth.phase_id,
            "is_target_device": self.ground_truth.is_target_device,
        }
        for name in FEATURE_NAMES:
            record[name] = None
        if self.features is not None:
            record.update(self.features.as_dict())
        return record


def scheduled_window_bounds(
    run_start_unix: float,
    run_end_unix: float,
    config: WindowConfig = WindowConfig(),
) -> Iterator[tuple[int, float, float]]:
    """Yield every complete run-anchored half-open window in stable order."""

    if not (
        math.isfinite(run_start_unix)
        and math.isfinite(run_end_unix)
        and run_start_unix <= run_end_unix
    ):
        raise ValueError("run bounds must be finite with start <= end")
    index = 0
    epsilon = 1e-9
    while True:
        start = run_start_unix + index * config.stride_seconds
        end = start + config.window_seconds
        if end > run_end_unix + epsilon:
            return
        yield index, start, end
        index += 1


def classify_ground_truth(
    *,
    device_id: str,
    window_start_unix: float,
    window_end_unix: float,
    run_start_unix: float,
    run_end_unix: float,
    scenario_phase: ScenarioPhase | None,
    phase_log_complete: bool = True,
) -> GroundTruth:
    """Assign evaluation-only labels from actual phase boundaries."""

    if (
        not phase_log_complete
        or window_start_unix < run_start_unix
        or window_end_unix > run_end_unix
        or window_start_unix >= window_end_unix
    ):
        return GroundTruth("unknown", None, False)
    if scenario_phase is None:
        return GroundTruth("normal", None, False)
    is_target = device_id == scenario_phase.target_device_id
    if not is_target:
        return GroundTruth("normal", scenario_phase.phase_id, False)
    if (
        window_start_unix >= scenario_phase.start_unix
        and window_end_unix <= scenario_phase.end_unix
    ):
        return GroundTruth("anomaly", scenario_phase.phase_id, True)
    if max(window_start_unix, scenario_phase.start_unix) < min(
        window_end_unix, scenario_phase.end_unix
    ):
        return GroundTruth("transition", scenario_phase.phase_id, True)
    return GroundTruth("normal", scenario_phase.phase_id, True)


def _feature_vector(
    observations: Sequence[TelemetryObservation],
    *,
    window_start_unix: float,
    config: WindowConfig,
) -> FeatureVector:
    count = len(observations)
    payloads = [float(row.payload_size_bytes) for row in observations]
    timestamps = [row.received_at_unix for row in observations]
    interarrivals_ms = [
        (current - previous) * 1000.0
        for previous, current in zip(timestamps, timestamps[1:], strict=False)
    ]
    buckets = Counter(
        int(math.floor(row.received_at_unix - window_start_unix))
        for row in observations
    )
    vector = FeatureVector(
        message_count=count,
        message_rate_hz=count / config.window_seconds,
        payload_mean_bytes=statistics.fmean(payloads),
        payload_std_bytes=statistics.pstdev(payloads),
        interarrival_mean_ms=statistics.fmean(interarrivals_ms),
        interarrival_std_ms=statistics.pstdev(interarrivals_ms),
        peak_messages_per_second=max(buckets.values()),
    )
    if not all(math.isfinite(value) for value in vector.as_tuple()):
        raise ValueError("feature vector contains a non-finite value")
    return vector


def build_scheduled_windows(
    *,
    run_id: str,
    collection_session_id: str,
    device_ids: Sequence[str],
    run_start_unix: float,
    run_end_unix: float,
    observations: Iterable[TelemetryObservation],
    scenario_phase: ScenarioPhase | None = None,
    phase_log_complete: bool = True,
    observation_gaps: Mapping[str, Sequence[ObservationGap]] | None = None,
    config: WindowConfig = WindowConfig(),
) -> list[FeatureWindow]:
    """Build complete scheduled rows for every configured device and window."""

    if not run_id or not collection_session_id:
        raise ValueError("run_id and collection_session_id must not be empty")
    if not device_ids or any(not value for value in device_ids):
        raise ValueError("device_ids must contain non-empty values")
    if len(set(device_ids)) != len(device_ids):
        raise ValueError("device_ids must be unique")
    if scenario_phase is not None and (
        scenario_phase.start_unix < run_start_unix
        or scenario_phase.end_unix > run_end_unix
    ):
        raise ValueError("scenario phase must be fully contained in the run")

    canonical_by_device: dict[str, list[TelemetryObservation]] = defaultdict(list)
    configured_devices = set(device_ids)
    for row in observations:
        if row.device_id in configured_devices and row.is_canonical_telemetry:
            canonical_by_device[row.device_id].append(row)
    for rows in canonical_by_device.values():
        rows.sort(key=lambda row: (row.received_at_unix, row.observation_id))

    gaps = observation_gaps or {}
    bounds = list(scheduled_window_bounds(run_start_unix, run_end_unix, config))
    output: list[FeatureWindow] = []
    for device_id in device_ids:
        rows = canonical_by_device.get(device_id, [])
        for index, start, end in bounds:
            window_rows = [
                row for row in rows if start <= row.received_at_unix < end
            ]
            truth = classify_ground_truth(
                device_id=device_id,
                window_start_unix=start,
                window_end_unix=end,
                run_start_unix=run_start_unix,
                run_end_unix=run_end_unix,
                scenario_phase=scenario_phase,
                phase_log_complete=phase_log_complete,
            )
            intersecting_gap = next(
                (
                    gap
                    for gap in gaps.get(device_id, ())
                    if gap.intersects(start, end)
                ),
                None,
            )
            status: FeatureStatus
            exclusion_reason: str | None
            features: FeatureVector | None
            if intersecting_gap is not None:
                status = "observation_gap"
                exclusion_reason = intersecting_gap.reason
                features = None
            elif len(window_rows) < config.min_messages:
                status = "insufficient_data"
                exclusion_reason = "insufficient_data"
                features = None
            else:
                try:
                    features = _feature_vector(
                        window_rows,
                        window_start_unix=start,
                        config=config,
                    )
                except (OverflowError, ValueError):
                    status = "invalid_feature"
                    exclusion_reason = "invalid_feature"
                    features = None
                else:
                    status = "ok"
                    exclusion_reason = None
            output.append(
                FeatureWindow(
                    run_id=run_id,
                    collection_session_id=collection_session_id,
                    device_id=device_id,
                    window_index=index,
                    window_start_unix=start,
                    window_end_unix=end,
                    observed_message_count=len(window_rows),
                    feature_status=status,
                    exclusion_reason=exclusion_reason,
                    features=features,
                    ground_truth=truth,
                )
            )
    return output
