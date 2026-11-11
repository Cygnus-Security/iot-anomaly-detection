"""Isolation Forest fitting, calibration, scoring, and trusted artifacts."""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from src.ml.contracts import (
    DEFAULT_THRESHOLD_QUANTILE,
    FEATURE_NAMES,
    FEATURE_SCHEMA_VERSION,
    MIN_MESSAGES,
    SCORE_DIRECTION,
    STRIDE_SECONDS,
    WINDOW_SECONDS,
    MlContractError,
)

MODEL_FILENAME = "model.joblib"
METADATA_FILENAME = "model_metadata.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _runtime_dependencies() -> tuple[Any, Any, Any, str, str]:
    try:
        import joblib
        import numpy as np
        import sklearn
        from sklearn.ensemble import IsolationForest
    except ImportError as exc:  # pragma: no cover - exercised without ML extra
        raise RuntimeError(
            "Phase 3 requires scikit-learn, NumPy, and joblib"
        ) from exc
    return joblib, np, IsolationForest, str(sklearn.__version__), str(joblib.__version__)


@dataclass(frozen=True, slots=True)
class IsolationForestConfig:
    """Frozen starting configuration from the Chapter IV experiment plan."""

    n_estimators: int = 100
    max_samples: str | int | float = "auto"
    max_features: float = 1.0
    contamination: str | float = "auto"
    bootstrap: bool = False
    random_state: int = 42

    def __post_init__(self) -> None:
        if self.n_estimators <= 0:
            raise MlContractError("n_estimators must be positive")
        if not 0 < self.max_features <= 1:
            raise MlContractError("max_features must be in (0, 1]")


@dataclass(frozen=True, slots=True)
class ModelMetadata:
    """Serializable model provenance and compatibility contract."""

    model_version: str
    trained_at_utc: str
    feature_schema_version: str
    feature_names: tuple[str, ...]
    window_seconds: int
    stride_seconds: int
    min_messages: int
    threshold: float
    threshold_quantile: float
    score_direction: str
    train_run_ids: tuple[str, ...]
    validation_run_ids: tuple[str, ...]
    training_row_count: int
    validation_row_count: int
    config: IsolationForestConfig
    library_versions: dict[str, str]
    model_sha256: str = ""

    def __post_init__(self) -> None:
        if not self.model_version.strip():
            raise MlContractError("model_version must be non-empty")
        if self.feature_names != FEATURE_NAMES:
            raise MlContractError("feature names or order do not match schema 1.0")
        if not math.isfinite(self.threshold):
            raise MlContractError("threshold must be finite")
        if not 0 <= self.threshold_quantile <= 1:
            raise MlContractError("threshold_quantile must be in [0, 1]")
        if set(self.train_run_ids) & set(self.validation_run_ids):
            raise MlContractError("training and validation runs overlap")

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["feature_names"] = list(self.feature_names)
        value["train_run_ids"] = list(self.train_run_ids)
        value["validation_run_ids"] = list(self.validation_run_ids)
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ModelMetadata":
        try:
            config_value = value["config"]
            if not isinstance(config_value, Mapping):
                raise TypeError("config must be an object")
            library_versions = value["library_versions"]
            if not isinstance(library_versions, Mapping):
                raise TypeError("library_versions must be an object")
            return cls(
                model_version=str(value["model_version"]),
                trained_at_utc=str(value["trained_at_utc"]),
                feature_schema_version=str(value["feature_schema_version"]),
                feature_names=tuple(str(item) for item in value["feature_names"]),
                window_seconds=int(value["window_seconds"]),
                stride_seconds=int(value["stride_seconds"]),
                min_messages=int(value["min_messages"]),
                threshold=float(value["threshold"]),
                threshold_quantile=float(value["threshold_quantile"]),
                score_direction=str(value["score_direction"]),
                train_run_ids=tuple(str(item) for item in value["train_run_ids"]),
                validation_run_ids=tuple(
                    str(item) for item in value["validation_run_ids"]
                ),
                training_row_count=int(value["training_row_count"]),
                validation_row_count=int(value["validation_row_count"]),
                config=IsolationForestConfig(**dict(config_value)),
                library_versions={
                    str(key): str(item) for key, item in library_versions.items()
                },
                model_sha256=str(value.get("model_sha256", "")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise MlContractError(f"invalid model metadata: {exc}") from exc


@dataclass(slots=True)
class TrainedArtifact:
    estimator: Any
    metadata: ModelMetadata


@dataclass(frozen=True, slots=True)
class ScoreResult:
    score: float
    predicted_label: str
    threshold: float
    model_version: str


def _matrix(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[list[float]], int]:
    matrix: list[list[float]] = []
    for row_index, row in enumerate(rows):
        values: list[float] = []
        for feature in FEATURE_NAMES:
            if feature not in row:
                raise MlContractError(
                    f"row {row_index} is missing feature {feature!r}"
                )
            value = row[feature]
            if isinstance(value, bool):
                raise MlContractError(
                    f"row {row_index} feature {feature!r} must be numeric"
                )
            try:
                numeric = float(value)
            except (TypeError, ValueError) as exc:
                raise MlContractError(
                    f"row {row_index} feature {feature!r} must be numeric"
                ) from exc
            if not math.isfinite(numeric):
                raise MlContractError(
                    f"row {row_index} feature {feature!r} must be finite"
                )
            values.append(numeric)
        matrix.append(values)
    if not matrix:
        raise MlContractError("at least one feature row is required")
    return matrix, len(matrix)


def fit_isolation_forest(
    training_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
    *,
    model_version: str,
    train_run_ids: Sequence[str],
    validation_run_ids: Sequence[str],
    config: IsolationForestConfig | None = None,
    threshold_quantile: float = DEFAULT_THRESHOLD_QUANTILE,
) -> TrainedArtifact:
    """Fit normal-only data and calibrate a frozen lower-tail threshold."""

    if not 0 <= threshold_quantile <= 1:
        raise MlContractError("threshold_quantile must be in [0, 1]")
    train_ids = tuple(str(item) for item in train_run_ids)
    validation_ids = tuple(str(item) for item in validation_run_ids)
    if not train_ids or not validation_ids:
        raise MlContractError("training and validation run IDs are required")
    if set(train_ids) & set(validation_ids):
        raise MlContractError("training and validation runs overlap")

    joblib, np, IsolationForest, sklearn_version, joblib_version = (
        _runtime_dependencies()
    )
    del joblib
    train_matrix, train_count = _matrix(training_rows)
    validation_matrix, validation_count = _matrix(validation_rows)
    effective_config = config or IsolationForestConfig()
    estimator = IsolationForest(**asdict(effective_config))
    estimator.fit(train_matrix)
    validation_scores = estimator.decision_function(validation_matrix)
    threshold = float(np.quantile(validation_scores, threshold_quantile))
    metadata = ModelMetadata(
        model_version=model_version,
        trained_at_utc=_utc_now(),
        feature_schema_version=FEATURE_SCHEMA_VERSION,
        feature_names=FEATURE_NAMES,
        window_seconds=WINDOW_SECONDS,
        stride_seconds=STRIDE_SECONDS,
        min_messages=MIN_MESSAGES,
        threshold=threshold,
        threshold_quantile=threshold_quantile,
        score_direction=SCORE_DIRECTION,
        train_run_ids=train_ids,
        validation_run_ids=validation_ids,
        training_row_count=train_count,
        validation_row_count=validation_count,
        config=effective_config,
        library_versions={
            "python": platform.python_version(),
            "scikit-learn": sklearn_version,
            "joblib": joblib_version,
        },
    )
    return TrainedArtifact(estimator=estimator, metadata=metadata)


def score_feature_rows(
    artifact: TrainedArtifact,
    rows: Sequence[Mapping[str, Any]],
) -> list[ScoreResult]:
    """Score rows with the artifact's explicitly calibrated threshold."""

    matrix, _ = _matrix(rows)
    scores = artifact.estimator.decision_function(matrix)
    threshold = artifact.metadata.threshold
    return [
        ScoreResult(
            score=float(score),
            predicted_label="anomaly" if float(score) < threshold else "normal",
            threshold=threshold,
            model_version=artifact.metadata.model_version,
        )
        for score in scores
    ]


def save_artifact(
    artifact: TrainedArtifact, artifact_directory: str | os.PathLike[str]
) -> tuple[Path, Path]:
    """Persist a model and JSON metadata, including the model SHA-256."""

    joblib, _, _, _, _ = _runtime_dependencies()
    directory = Path(artifact_directory)
    directory.mkdir(parents=True, exist_ok=True)
    model_path = directory / MODEL_FILENAME
    metadata_path = directory / METADATA_FILENAME
    model_temporary = directory / f".{MODEL_FILENAME}.tmp"
    metadata_temporary = directory / f".{METADATA_FILENAME}.tmp"

    joblib.dump(artifact.estimator, model_temporary)
    model_temporary.replace(model_path)
    metadata = replace(artifact.metadata, model_sha256=_sha256(model_path))
    metadata_temporary.write_text(
        json.dumps(metadata.as_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    metadata_temporary.replace(metadata_path)
    artifact.metadata = metadata
    return model_path, metadata_path


def _validate_compatibility(
    metadata: ModelMetadata,
    *,
    feature_schema_version: str,
    feature_names: tuple[str, ...],
    window_seconds: int,
    stride_seconds: int,
    min_messages: int,
) -> None:
    expected = {
        "feature_schema_version": feature_schema_version,
        "feature_names": feature_names,
        "window_seconds": window_seconds,
        "stride_seconds": stride_seconds,
        "min_messages": min_messages,
        "score_direction": SCORE_DIRECTION,
    }
    actual = {
        "feature_schema_version": metadata.feature_schema_version,
        "feature_names": metadata.feature_names,
        "window_seconds": metadata.window_seconds,
        "stride_seconds": metadata.stride_seconds,
        "min_messages": metadata.min_messages,
        "score_direction": metadata.score_direction,
    }
    mismatches = {
        key: {"expected": expected[key], "actual": actual[key]}
        for key in expected
        if expected[key] != actual[key]
    }
    if mismatches:
        raise MlContractError(f"model compatibility mismatch: {mismatches}")


def load_artifact(
    artifact_directory: str | os.PathLike[str],
    *,
    trusted: bool = False,
    feature_schema_version: str = FEATURE_SCHEMA_VERSION,
    feature_names: tuple[str, ...] = FEATURE_NAMES,
    window_seconds: int = WINDOW_SECONDS,
    stride_seconds: int = STRIDE_SECONDS,
    min_messages: int = MIN_MESSAGES,
) -> TrainedArtifact:
    """Load an explicitly trusted artifact after checksum and contract checks."""

    if not trusted:
        raise MlContractError(
            "joblib artifacts can execute code; pass trusted=True only for "
            "artifacts produced by this controlled pipeline"
        )
    joblib, _, _, _, _ = _runtime_dependencies()
    directory = Path(artifact_directory)
    model_path = directory / MODEL_FILENAME
    metadata_path = directory / METADATA_FILENAME
    if not model_path.is_file() or not metadata_path.is_file():
        raise MlContractError("model artifact or metadata is missing")
    try:
        decoded = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MlContractError(f"model metadata cannot be read: {exc}") from exc
    if not isinstance(decoded, Mapping):
        raise MlContractError("model metadata must be a JSON object")
    metadata = ModelMetadata.from_dict(decoded)
    actual_hash = _sha256(model_path)
    if not metadata.model_sha256 or actual_hash != metadata.model_sha256:
        raise MlContractError("model checksum does not match metadata")
    _validate_compatibility(
        metadata,
        feature_schema_version=feature_schema_version,
        feature_names=feature_names,
        window_seconds=window_seconds,
        stride_seconds=stride_seconds,
        min_messages=min_messages,
    )
    estimator = joblib.load(model_path)
    if not callable(getattr(estimator, "decision_function", None)):
        raise MlContractError("loaded estimator has no decision_function")
    return TrainedArtifact(estimator=estimator, metadata=metadata)
