from __future__ import annotations

import math

import pytest

from src.features import (
    FEATURE_NAMES,
    ObservationGap,
    ScenarioPhase,
    TelemetryObservation,
    WindowConfig,
    build_scheduled_windows,
    classify_ground_truth,
    scheduled_window_bounds,
)


def observation(
    observation_id: int,
    at: float,
    payload_size: int,
    *,
    device_id: str = "iot-001",
    session_id: str | None = "session-1",
    message_type: str = "telemetry",
    valid: bool = True,
    duplicate: bool = False,
    retain: bool = False,
) -> TelemetryObservation:
    return TelemetryObservation(
        observation_id=observation_id,
        device_id=device_id,
        received_at_unix=at,
        payload_size_bytes=payload_size,
        session_id=session_id,
        message_type=message_type,
        payload_valid=valid,
        duplicate=duplicate,
        retain=retain,
    )


def build(
    observations: list[TelemetryObservation],
    *,
    device_ids: list[str] | None = None,
    run_start: float = 100.0,
    run_end: float = 120.0,
    phase: ScenarioPhase | None = None,
    gaps: dict[str, list[ObservationGap]] | None = None,
):
    return build_scheduled_windows(
        run_id="test-run",
        collection_session_id="collection-1",
        device_ids=device_ids or ["iot-001"],
        run_start_unix=run_start,
        run_end_unix=run_end,
        observations=observations,
        scenario_phase=phase,
        observation_gaps=gaps,
    )


def test_scheduled_windows_are_run_anchored_and_include_silent_devices() -> None:
    bounds = list(scheduled_window_bounds(100.0, 130.0))
    assert bounds == [
        (0, 100.0, 120.0),
        (1, 105.0, 125.0),
        (2, 110.0, 130.0),
    ]
    devices = [f"iot-{index:03d}" for index in range(1, 13)]
    windows = build([], device_ids=devices, run_end=130.0)
    assert len(windows) == 36
    assert {window.device_id for window in windows} == set(devices)
    assert all(window.feature_status == "insufficient_data" for window in windows)
    assert all(window.observed_message_count == 0 for window in windows)


def test_half_open_window_and_exact_feature_math() -> None:
    rows = [
        observation(1, 100.0, 100),
        observation(2, 101.0, 200),
        observation(3, 102.0, 300),
        observation(4, 120.0, 9_999),
    ]
    window = build(rows)[0]
    assert window.can_score
    assert window.observed_message_count == 3
    assert window.features is not None
    assert window.features.message_count == 3
    assert window.features.message_rate_hz == 0.15
    assert window.features.payload_mean_bytes == 200.0
    assert window.features.payload_std_bytes == pytest.approx(
        math.sqrt(20_000 / 3)
    )
    assert window.features.interarrival_mean_ms == 1_000.0
    assert window.features.interarrival_std_ms == 0.0
    assert window.features.peak_messages_per_second == 1
    assert tuple(window.features.as_dict()) == FEATURE_NAMES
    assert isinstance(window.features.as_dict()["message_count"], int)


def test_peak_bucket_is_anchored_to_window_start() -> None:
    rows = [
        observation(1, 100.2, 100),
        observation(2, 100.8, 100),
        observation(3, 101.0, 100),
        observation(4, 101.999, 100),
    ]
    window = build(rows)[0]
    assert window.features is not None
    assert window.features.peak_messages_per_second == 2


def test_only_valid_session_aware_canonical_nonretained_telemetry_enters_window() -> None:
    rows = [
        observation(1, 100.0, 100),
        observation(2, 101.0, 100),
        observation(3, 102.0, 100, duplicate=True),
        observation(4, 103.0, 100, valid=False),
        observation(5, 104.0, 100, retain=True),
        observation(6, 105.0, 100, message_type="status"),
        observation(7, 106.0, 100, session_id=None),
    ]
    window = build(rows)[0]
    assert window.feature_status == "insufficient_data"
    assert window.observed_message_count == 2
    assert window.features is None
    record = window.as_record()
    assert all(record[name] is None for name in FEATURE_NAMES)


def test_known_observation_gap_overrides_an_otherwise_valid_vector() -> None:
    rows = [
        observation(1, 100.0, 100),
        observation(2, 101.0, 100),
        observation(3, 102.0, 100),
    ]
    windows = build(
        rows,
        gaps={"iot-001": [ObservationGap(110.0, 111.0, "queue_overflow")]},
    )
    assert windows[0].feature_status == "observation_gap"
    assert windows[0].exclusion_reason == "queue_overflow"
    assert windows[0].features is None


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (100.0, 120.0, "normal"),
        (120.0, 140.0, "anomaly"),
        (115.0, 135.0, "transition"),
        (130.0, 150.0, "transition"),
        (140.0, 160.0, "normal"),
    ],
)
def test_ground_truth_half_open_boundaries(
    start: float, end: float, expected: str
) -> None:
    phase = ScenarioPhase("phase-1", "iot-001", 120.0, 140.0)
    truth = classify_ground_truth(
        device_id="iot-001",
        window_start_unix=start,
        window_end_unix=end,
        run_start_unix=100.0,
        run_end_unix=180.0,
        scenario_phase=phase,
    )
    assert truth.label == expected
    assert truth.is_target_device


def test_ground_truth_controls_k1_and_incomplete_phase_log() -> None:
    phase = ScenarioPhase("phase-1", "iot-001", 120.0, 140.0)
    control = classify_ground_truth(
        device_id="iot-002",
        window_start_unix=120.0,
        window_end_unix=140.0,
        run_start_unix=100.0,
        run_end_unix=180.0,
        scenario_phase=phase,
    )
    assert control.label == "normal"
    assert not control.is_target_device

    k1 = classify_ground_truth(
        device_id="iot-001",
        window_start_unix=100.0,
        window_end_unix=120.0,
        run_start_unix=100.0,
        run_end_unix=180.0,
        scenario_phase=None,
    )
    assert k1.label == "normal"

    unknown = classify_ground_truth(
        device_id="iot-001",
        window_start_unix=120.0,
        window_end_unix=140.0,
        run_start_unix=100.0,
        run_end_unix=180.0,
        scenario_phase=phase,
        phase_log_complete=False,
    )
    assert unknown.label == "unknown"
    assert unknown.phase_id is None


def test_tied_timestamps_use_observation_id_for_deterministic_order() -> None:
    rows = [
        observation(3, 100.0, 300),
        observation(1, 100.0, 100),
        observation(2, 101.0, 200),
    ]
    first = build(rows)[0]
    second = build(list(reversed(rows)))[0]
    assert first.features == second.features
    assert first.features is not None
    assert first.features.interarrival_mean_ms == 500.0


def test_configuration_and_identity_validation_fail_closed() -> None:
    with pytest.raises(ValueError, match="window_seconds"):
        WindowConfig(window_seconds=0)
    with pytest.raises(ValueError, match="at least two"):
        WindowConfig(min_messages=1)
    with pytest.raises(ValueError, match="unique"):
        build_scheduled_windows(
            run_id="run",
            collection_session_id="collection",
            device_ids=["iot-001", "iot-001"],
            run_start_unix=0,
            run_end_unix=20,
            observations=[],
        )
    with pytest.raises(ValueError, match="fully contained"):
        build_scheduled_windows(
            run_id="run",
            collection_session_id="collection",
            device_ids=["iot-001"],
            run_start_unix=100,
            run_end_unix=120,
            observations=[],
            scenario_phase=ScenarioPhase("phase", "iot-001", 99, 110),
        )
