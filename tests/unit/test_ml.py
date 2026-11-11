from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from src.ml import (
    FEATURE_NAMES,
    FEATURE_SCHEMA_VERSION,
    MIN_MESSAGES,
    SCORE_DIRECTION,
    STRIDE_SECONDS,
    WINDOW_SECONDS,
    IsolationForestConfig,
    MlContractError,
    ModelMetadata,
    SplitManifest,
    TrainedArtifact,
    aggregate_binary_metrics,
    compute_binary_metrics,
    evaluate_event_detection,
    fit_isolation_forest,
    load_artifact,
    save_artifact,
    score_feature_rows,
)


def feature_row(offset: float) -> dict[str, float]:
    return {
        "message_count": 4 + offset,
        "message_rate_hz": 0.2 + offset * 0.001,
        "payload_mean_bytes": 370 + offset,
        "payload_std_bytes": 2 + offset * 0.01,
        "interarrival_mean_ms": 5000 + offset,
        "interarrival_std_ms": 100 + offset,
        "peak_messages_per_second": 1 + offset * 0.001,
    }


def metadata(*, threshold: float = 0.0) -> ModelMetadata:
    return ModelMetadata(
        model_version="iforest-test-v1",
        trained_at_utc="2026-11-11T00:00:00.000000Z",
        feature_schema_version=FEATURE_SCHEMA_VERSION,
        feature_names=FEATURE_NAMES,
        window_seconds=WINDOW_SECONDS,
        stride_seconds=STRIDE_SECONDS,
        min_messages=MIN_MESSAGES,
        threshold=threshold,
        threshold_quantile=0.01,
        score_direction=SCORE_DIRECTION,
        train_run_ids=("train-normal-01", "train-normal-02"),
        validation_run_ids=("validation-normal-01",),
        training_row_count=2,
        validation_row_count=1,
        config=IsolationForestConfig(),
        library_versions={"python": "test", "scikit-learn": "test"},
    )


def test_split_manifest_rejects_run_leakage() -> None:
    manifest = SplitManifest.from_mapping(
        {
            "train_run_ids": ["train-01", "train-02"],
            "validation_run_ids": ["validation-01"],
            "test_run_ids": ["test-01"],
        }
    )
    assert manifest.as_dict()["test_run_ids"] == ["test-01"]

    with pytest.raises(MlContractError, match="overlap"):
        SplitManifest(
            train_run_ids=("shared",),
            validation_run_ids=("validation-01",),
            test_run_ids=("shared",),
        )


def test_binary_metrics_filter_excluded_rows_and_handle_zero_denominators() -> None:
    rows = [
        {"ground_truth_label": "anomaly", "predicted_label": "anomaly"},
        {"ground_truth_label": "anomaly", "predicted_label": "normal"},
        {"ground_truth_label": "normal", "predicted_label": "anomaly"},
        {"ground_truth_label": "normal", "predicted_label": "normal"},
        {
            "ground_truth_label": "normal",
            "predicted_label": "anomaly",
            "included_in_primary_metrics": False,
        },
    ]
    result = compute_binary_metrics(rows)
    assert (result.tp, result.fp, result.tn, result.fn) == (1, 1, 1, 1)
    assert result.precision == 0.5
    assert result.recall == 0.5
    assert result.f1 == 0.5
    assert result.fpr == 0.5

    no_positive_prediction = compute_binary_metrics(
        [{"ground_truth_label": "anomaly", "predicted_label": "normal"}]
    )
    assert no_positive_prediction.precision is None
    assert no_positive_prediction.recall == 0.0
    assert no_positive_prediction.f1 == 0.0
    assert no_positive_prediction.fpr is None


def test_metrics_are_aggregated_from_counts_not_meaned_ratios() -> None:
    rows = [
        {
            "scenario": "message_burst",
            "ground_truth_label": "anomaly",
            "predicted_label": "anomaly",
        },
        {
            "scenario": "message_burst",
            "ground_truth_label": "normal",
            "predicted_label": "normal",
        },
        {
            "scenario": "abnormal_payload",
            "ground_truth_label": "anomaly",
            "predicted_label": "normal",
        },
    ]
    grouped = aggregate_binary_metrics(rows, group_keys=("scenario",))
    assert [row["scenario"] for row in grouped] == [
        "abnormal_payload",
        "message_burst",
    ]
    assert grouped[0]["fn"] == 1
    assert grouped[1]["tp"] == 1


def test_score_rule_is_strictly_below_threshold() -> None:
    class FakeEstimator:
        def decision_function(self, matrix: list[list[float]]) -> list[float]:
            return [row[0] for row in matrix]

    artifact = TrainedArtifact(FakeEstimator(), metadata(threshold=0.0))
    below = feature_row(0)
    below["message_count"] = -0.1
    equal = feature_row(0)
    equal["message_count"] = 0.0
    results = score_feature_rows(artifact, [below, equal])
    assert [item.predicted_label for item in results] == ["anomaly", "normal"]


def test_event_latency_requires_correct_device_intersection_and_deadline() -> None:
    start = datetime(2026, 11, 11, 1, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=60)
    event = {
        "run_id": "test-k2-01",
        "device_id": "iot-001",
        "anomaly_start_utc": start.isoformat(),
        "anomaly_end_utc": end.isoformat(),
    }
    predictions = [
        {
            "run_id": "test-k2-01",
            "device_id": "iot-002",
            "window_start_utc": start.isoformat(),
            "window_end_utc": (start + timedelta(seconds=20)).isoformat(),
            "prediction_recorded_at_utc": (start + timedelta(seconds=5)).isoformat(),
            "predicted_label": "anomaly",
            "inference_status": "scored",
        },
        {
            "run_id": "test-k2-01",
            "device_id": "iot-001",
            "window_start_utc": (start - timedelta(seconds=5)).isoformat(),
            "window_end_utc": (start + timedelta(seconds=15)).isoformat(),
            "prediction_recorded_at_utc": (start + timedelta(seconds=7)).isoformat(),
            "predicted_label": "anomaly",
            "inference_status": "scored",
        },
    ]
    result = evaluate_event_detection(event, predictions)
    assert result.status == "detected"
    assert result.latency_seconds == 7.0

    missed = evaluate_event_detection(event, predictions[:1])
    assert missed.status == "not_detected"
    assert missed.latency_seconds is None

    invalid = evaluate_event_detection(
        event,
        [],
        evaluable=False,
        not_evaluable_reason="missing phase boundary",
    )
    assert invalid.status == "not_evaluable"
    assert invalid.reason == "missing phase boundary"


def test_fit_save_load_and_checksum_contract(tmp_path) -> None:
    pytest.importorskip("sklearn")
    pytest.importorskip("joblib")
    training = [feature_row(float(index % 11)) for index in range(80)]
    validation = [feature_row(float(index % 7) + 0.25) for index in range(30)]
    artifact = fit_isolation_forest(
        training,
        validation,
        model_version="iforest-20261111-01",
        train_run_ids=("train-normal-01", "train-normal-02"),
        validation_run_ids=("validation-normal-01",),
    )
    assert math.isfinite(artifact.metadata.threshold)
    assert artifact.metadata.config == IsolationForestConfig()
    before = score_feature_rows(artifact, [feature_row(1.5)])[0]

    model_path, metadata_path = save_artifact(artifact, tmp_path / "artifact")
    assert model_path.is_file()
    assert metadata_path.is_file()
    assert len(artifact.metadata.model_sha256) == 64

    with pytest.raises(MlContractError, match="trusted=True"):
        load_artifact(tmp_path / "artifact")
    loaded = load_artifact(tmp_path / "artifact", trusted=True)
    after = score_feature_rows(loaded, [feature_row(1.5)])[0]
    assert after.score == pytest.approx(before.score)
    assert after.predicted_label == before.predicted_label

    with pytest.raises(MlContractError, match="compatibility mismatch"):
        load_artifact(tmp_path / "artifact", trusted=True, stride_seconds=10)

    with model_path.open("ab") as handle:
        handle.write(b"tampered")
    with pytest.raises(MlContractError, match="checksum"):
        load_artifact(tmp_path / "artifact", trusted=True)


def test_feature_inputs_must_be_complete_and_finite() -> None:
    class FakeEstimator:
        def decision_function(self, matrix: list[list[float]]) -> list[float]:
            return [0.0 for _ in matrix]

    artifact = TrainedArtifact(FakeEstimator(), metadata())
    incomplete = feature_row(0)
    incomplete.pop(FEATURE_NAMES[-1])
    with pytest.raises(MlContractError, match="missing feature"):
        score_feature_rows(artifact, [incomplete])

    non_finite = feature_row(0)
    non_finite[FEATURE_NAMES[0]] = math.nan
    with pytest.raises(MlContractError, match="finite"):
        score_feature_rows(artifact, [non_finite])
