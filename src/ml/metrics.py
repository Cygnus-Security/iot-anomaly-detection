"""Traceable window metrics and event-detection latency calculations."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

from src.ml.contracts import MlContractError

NORMAL = "normal"
ANOMALY = "anomaly"


def _ratio(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else numerator / denominator


@dataclass(frozen=True, slots=True)
class BinaryMetrics:
    evaluated_windows: int
    tp: int
    fp: int
    tn: int
    fn: int
    precision: float | None
    recall: float | None
    f1: float | None
    fpr: float | None

    def as_dict(self) -> dict[str, int | float | None]:
        return asdict(self)


def compute_binary_metrics(
    rows: Iterable[Mapping[str, Any]],
    *,
    ground_truth_key: str = "ground_truth_label",
    predicted_key: str = "predicted_label",
    included_key: str = "included_in_primary_metrics",
) -> BinaryMetrics:
    """Compute auditable binary metrics from eligible normal/anomaly windows."""

    tp = fp = tn = fn = 0
    for index, row in enumerate(rows):
        if included_key in row and not bool(row[included_key]):
            continue
        actual = row.get(ground_truth_key)
        predicted = row.get(predicted_key)
        if actual not in {NORMAL, ANOMALY}:
            raise MlContractError(
                f"row {index} has unsupported ground-truth label {actual!r}"
            )
        if predicted not in {NORMAL, ANOMALY}:
            raise MlContractError(
                f"row {index} has unsupported predicted label {predicted!r}"
            )
        if actual == ANOMALY and predicted == ANOMALY:
            tp += 1
        elif actual == NORMAL and predicted == ANOMALY:
            fp += 1
        elif actual == NORMAL and predicted == NORMAL:
            tn += 1
        else:
            fn += 1
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    f1 = _ratio(2 * tp, 2 * tp + fp + fn)
    fpr = _ratio(fp, fp + tn)
    return BinaryMetrics(
        evaluated_windows=tp + fp + tn + fn,
        tp=tp,
        fp=fp,
        tn=tn,
        fn=fn,
        precision=precision,
        recall=recall,
        f1=f1,
        fpr=fpr,
    )


def aggregate_binary_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    group_keys: Sequence[str],
    ground_truth_key: str = "ground_truth_label",
    predicted_key: str = "predicted_label",
    included_key: str = "included_in_primary_metrics",
) -> list[dict[str, Any]]:
    """Aggregate counts first, then derive ratios for each requested group."""

    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for row in rows:
        key = tuple(row.get(name) for name in group_keys)
        grouped.setdefault(key, []).append(row)
    result: list[dict[str, Any]] = []
    for key in sorted(grouped, key=lambda value: tuple(str(item) for item in value)):
        metrics = compute_binary_metrics(
            grouped[key],
            ground_truth_key=ground_truth_key,
            predicted_key=predicted_key,
            included_key=included_key,
        )
        group = {name: value for name, value in zip(group_keys, key, strict=True)}
        result.append(group | metrics.as_dict())
    return result


def _timestamp(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise MlContractError(f"{name} is not a valid timestamp") from exc
    else:
        raise MlContractError(f"{name} is not a valid timestamp")
    if parsed.tzinfo is None:
        raise MlContractError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


@dataclass(frozen=True, slots=True)
class EventDetectionResult:
    run_id: str
    device_id: str
    anomaly_start_utc: str
    anomaly_end_utc: str
    deadline_utc: str
    status: str
    first_detection_utc: str | None
    latency_seconds: float | None
    reason: str | None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_event_detection(
    event: Mapping[str, Any],
    predictions: Iterable[Mapping[str, Any]],
    *,
    grace_seconds: float = 20.0,
    evaluable: bool = True,
    not_evaluable_reason: str | None = None,
) -> EventDetectionResult:
    """Find the first valid correct-device anomaly detection for one event."""

    run_id = str(event.get("run_id", "")).strip()
    device_id = str(event.get("device_id", "")).strip()
    if not run_id or not device_id:
        raise MlContractError("event requires run_id and device_id")
    start = _timestamp(event.get("anomaly_start_utc"), "anomaly_start_utc")
    end = _timestamp(event.get("anomaly_end_utc"), "anomaly_end_utc")
    if end <= start:
        raise MlContractError("anomaly_end_utc must be after anomaly_start_utc")
    if grace_seconds < 0:
        raise MlContractError("grace_seconds must be non-negative")
    deadline = end + timedelta(seconds=grace_seconds)
    if not evaluable:
        return EventDetectionResult(
            run_id=run_id,
            device_id=device_id,
            anomaly_start_utc=_format_timestamp(start),
            anomaly_end_utc=_format_timestamp(end),
            deadline_utc=_format_timestamp(deadline),
            status="not_evaluable",
            first_detection_utc=None,
            latency_seconds=None,
            reason=not_evaluable_reason or "event failed its pre-scoring quality gate",
        )

    candidates: list[datetime] = []
    for prediction in predictions:
        if str(prediction.get("run_id")) != run_id:
            continue
        if str(prediction.get("device_id")) != device_id:
            continue
        if prediction.get("predicted_label") != ANOMALY:
            continue
        if prediction.get("inference_status", "scored") != "scored":
            continue
        window_start = _timestamp(
            prediction.get("window_start_utc"), "window_start_utc"
        )
        window_end = _timestamp(prediction.get("window_end_utc"), "window_end_utc")
        recorded = _timestamp(
            prediction.get("prediction_recorded_at_utc"),
            "prediction_recorded_at_utc",
        )
        if max(window_start, start) >= min(window_end, end):
            continue
        if recorded < start or recorded > deadline:
            continue
        candidates.append(recorded)

    if not candidates:
        return EventDetectionResult(
            run_id=run_id,
            device_id=device_id,
            anomaly_start_utc=_format_timestamp(start),
            anomaly_end_utc=_format_timestamp(end),
            deadline_utc=_format_timestamp(deadline),
            status="not_detected",
            first_detection_utc=None,
            latency_seconds=None,
            reason=None,
        )
    first = min(candidates)
    return EventDetectionResult(
        run_id=run_id,
        device_id=device_id,
        anomaly_start_utc=_format_timestamp(start),
        anomaly_end_utc=_format_timestamp(end),
        deadline_utc=_format_timestamp(deadline),
        status="detected",
        first_detection_utc=_format_timestamp(first),
        latency_seconds=(first - start).total_seconds(),
        reason=None,
    )
