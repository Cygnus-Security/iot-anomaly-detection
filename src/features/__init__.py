"""Deterministic, versioned communication-window feature extraction."""

from src.features.windows import (
    FEATURE_NAMES,
    FEATURE_SCHEMA_VERSION,
    FeatureStatus,
    FeatureVector,
    FeatureWindow,
    GroundTruth,
    GroundTruthLabel,
    ObservationGap,
    ScenarioPhase,
    TelemetryObservation,
    WindowConfig,
    build_scheduled_windows,
    classify_ground_truth,
    scheduled_window_bounds,
)

__all__ = [
    "FEATURE_NAMES",
    "FEATURE_SCHEMA_VERSION",
    "FeatureStatus",
    "FeatureVector",
    "FeatureWindow",
    "GroundTruth",
    "GroundTruthLabel",
    "ObservationGap",
    "ScenarioPhase",
    "TelemetryObservation",
    "WindowConfig",
    "build_scheduled_windows",
    "classify_ground_truth",
    "scheduled_window_bounds",
]
