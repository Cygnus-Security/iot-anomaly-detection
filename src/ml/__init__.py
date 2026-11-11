"""Versioned Isolation Forest training, inference, and evaluation primitives."""

from src.ml.contracts import (
    DEFAULT_THRESHOLD_QUANTILE,
    FEATURE_NAMES,
    FEATURE_SCHEMA_VERSION,
    MIN_MESSAGES,
    SCORE_DIRECTION,
    STRIDE_SECONDS,
    WINDOW_SECONDS,
    MlContractError,
    SplitManifest,
)
from src.ml.metrics import (
    BinaryMetrics,
    EventDetectionResult,
    aggregate_binary_metrics,
    compute_binary_metrics,
    evaluate_event_detection,
)
from src.ml.model import (
    IsolationForestConfig,
    ModelMetadata,
    ScoreResult,
    TrainedArtifact,
    fit_isolation_forest,
    load_artifact,
    save_artifact,
    score_feature_rows,
)

__all__ = [
    "DEFAULT_THRESHOLD_QUANTILE",
    "FEATURE_NAMES",
    "FEATURE_SCHEMA_VERSION",
    "MIN_MESSAGES",
    "SCORE_DIRECTION",
    "STRIDE_SECONDS",
    "WINDOW_SECONDS",
    "BinaryMetrics",
    "EventDetectionResult",
    "IsolationForestConfig",
    "MlContractError",
    "ModelMetadata",
    "ScoreResult",
    "SplitManifest",
    "TrainedArtifact",
    "aggregate_binary_metrics",
    "compute_binary_metrics",
    "evaluate_event_detection",
    "fit_isolation_forest",
    "load_artifact",
    "save_artifact",
    "score_feature_rows",
]
