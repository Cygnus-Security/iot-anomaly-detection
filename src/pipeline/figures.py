"""Figures generated directly from campaign evaluation records."""

from __future__ import annotations

import csv
import math
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.features import FEATURE_NAMES
from src.pipeline.common import parse_utc


def _pyplot() -> Any:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def _save(figure: Any, path: Path) -> str:
    figure.tight_layout()
    figure.savefig(path, dpi=180, bbox_inches="tight")
    figure.clear()
    return str(path.name)


def _confusion_matrix(
    directory: Path, rows: Sequence[Mapping[str, Any]]
) -> str | None:
    eligible = [row for row in rows if bool(row["included_in_primary_metrics"])]
    if not eligible:
        return None
    tn = sum(row["ground_truth_label"] == "normal" and row["predicted_label"] == "normal" for row in eligible)
    fp = sum(row["ground_truth_label"] == "normal" and row["predicted_label"] == "anomaly" for row in eligible)
    fn = sum(row["ground_truth_label"] == "anomaly" and row["predicted_label"] == "normal" for row in eligible)
    tp = sum(row["ground_truth_label"] == "anomaly" and row["predicted_label"] == "anomaly" for row in eligible)
    matrix = [[tn, fp], [fn, tp]]
    plt = _pyplot()
    figure, axis = plt.subplots(figsize=(5.5, 4.5))
    image = axis.imshow(matrix, cmap="Blues")
    figure.colorbar(image, ax=axis, label="Window count")
    axis.set_xticks((0, 1), ("Normal", "Anomaly"))
    axis.set_yticks((0, 1), ("Normal", "Anomaly"))
    axis.set_xlabel("Predicted label")
    axis.set_ylabel("Ground-truth label")
    axis.set_title("Test-window confusion matrix")
    threshold = max(max(row) for row in matrix) / 2 if matrix else 0
    for y, row in enumerate(matrix):
        for x, value in enumerate(row):
            axis.text(
                x,
                y,
                str(value),
                ha="center",
                va="center",
                color="white" if value > threshold else "black",
            )
    name = _save(figure, directory / "confusion_matrix_all_test.png")
    plt.close(figure)
    return name


def _scenario_metrics(
    directory: Path, metrics: Sequence[Mapping[str, Any]]
) -> str | None:
    selected = [row for row in metrics if row["scenario_code"] in {"K2", "K3", "K4"}]
    if not selected:
        return None
    plt = _pyplot()
    figure, axis = plt.subplots(figsize=(8.5, 4.8))
    names = ("precision", "recall", "f1", "fpr")
    labels = ("Precision", "Recall", "F1", "FPR")
    width = 0.18
    centers = list(range(len(selected)))
    for metric_index, (name, label) in enumerate(zip(names, labels, strict=True)):
        values = [
            math.nan if row[name] is None else float(row[name]) for row in selected
        ]
        offsets = [x + (metric_index - 1.5) * width for x in centers]
        axis.bar(offsets, values, width=width, label=label)
    axis.set_xticks(centers, [str(row["scenario_code"]) for row in selected])
    axis.set_ylim(0, 1.05)
    axis.set_ylabel("Metric value")
    axis.set_title("Window-level metrics by anomaly scenario")
    axis.grid(axis="y", alpha=0.25)
    axis.legend(ncol=4)
    name = _save(figure, directory / "metrics_by_scenario.png")
    plt.close(figure)
    return name


def _events(results_root: Path) -> dict[str, tuple[Any, Any]]:
    path = results_root / "event_detection.csv"
    if not path.is_file():
        return {}
    output: dict[str, tuple[Any, Any]] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("anomaly_start_utc") and row.get("anomaly_end_utc"):
                output[str(row["run_id"])] = (
                    parse_utc(str(row["anomaly_start_utc"])),
                    parse_utc(str(row["anomaly_end_utc"])),
                )
    return output


def _score_timeline(
    results_root: Path,
    directory: Path,
    rows: Sequence[Mapping[str, Any]],
    scenario_code: str,
) -> str | None:
    scenario_rows = [
        row
        for row in rows
        if row["scenario_code"] == scenario_code
        and bool(row["is_target_device"])
        and row["inference_status"] == "scored"
    ]
    run_ids = sorted({str(row["run_id"]) for row in scenario_rows})
    if not run_ids:
        return None
    events = _events(results_root)
    plt = _pyplot()
    figure, axes = plt.subplots(
        len(run_ids), 1, figsize=(9.0, 2.7 * len(run_ids)), squeeze=False
    )
    for axis, run_id in zip(axes[:, 0], run_ids, strict=True):
        run_rows = sorted(
            (row for row in scenario_rows if row["run_id"] == run_id),
            key=lambda row: str(row["window_end_utc"]),
        )
        event = events.get(run_id)
        reference = event[0] if event else parse_utc(str(run_rows[0]["window_end_utc"]))
        x = [
            (parse_utc(str(row["window_end_utc"])) - reference).total_seconds()
            for row in run_rows
        ]
        scores = [float(row["score"]) for row in run_rows]
        thresholds = [float(row["threshold"]) for row in run_rows]
        axis.plot(x, scores, marker="o", markersize=2.5, linewidth=1, label="Score")
        axis.plot(x, thresholds, linestyle="--", linewidth=1.2, label="Frozen threshold")
        if event:
            duration = (event[1] - event[0]).total_seconds()
            axis.axvspan(0, duration, alpha=0.15, color="tab:red", label="Ground-truth anomaly phase")
        axis.set_title(run_id)
        axis.set_ylabel("decision_function")
        axis.grid(alpha=0.2)
        axis.legend(loc="best", fontsize=8)
    axes[-1, 0].set_xlabel("Seconds relative to actual anomaly start")
    figure.suptitle(f"{scenario_code}: target-device score and frozen threshold")
    name = _save(figure, directory / f"score_threshold_{scenario_code.lower()}.png")
    plt.close(figure)
    return name


def _feature_distributions(
    directory: Path, rows: Sequence[Mapping[str, Any]]
) -> str | None:
    valid = [row for row in rows if row["feature_status"] == "ok"]
    if not valid:
        return None
    codes = ("K1", "K2", "K3", "K4")
    plt = _pyplot()
    figure, axes = plt.subplots(2, 4, figsize=(15, 7.2))
    for axis, feature in zip(axes.flat, FEATURE_NAMES, strict=False):
        samples = [
            [float(row[feature]) for row in valid if row["scenario_code"] == code]
            for code in codes
        ]
        axis.boxplot(samples, tick_labels=codes, showfliers=False)
        axis.set_title(feature.replace("_", " "))
        axis.grid(axis="y", alpha=0.2)
    for axis in list(axes.flat)[len(FEATURE_NAMES) :]:
        axis.axis("off")
    figure.suptitle(
        "Observed feature distributions by scenario (ground-truth scenario groups)"
    )
    name = _save(figure, directory / "feature_distributions_k1_k4.png")
    plt.close(figure)
    return name


def generate_figures(
    results_root: str | Path,
    test_rows: Sequence[Mapping[str, Any]],
    scenario_metrics: Sequence[Mapping[str, Any]],
) -> list[str]:
    """Generate only figures supported by the measured result tables."""

    root = Path(results_root)
    directory = root / "figures"
    directory.mkdir(parents=True, exist_ok=True)
    cache_directory = directory / ".matplotlib-cache"
    cache_directory.mkdir(exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache_directory))
    generated = [
        _confusion_matrix(directory, test_rows),
        _scenario_metrics(directory, scenario_metrics),
        _feature_distributions(directory, test_rows),
        *(
            _score_timeline(root, directory, test_rows, code)
            for code in ("K2", "K3", "K4")
        ),
    ]
    return [name for name in generated if name is not None]
