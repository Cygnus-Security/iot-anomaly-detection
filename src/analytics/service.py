"""Scheduled feature extraction and optional Isolation Forest inference service."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import signal
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from src.analytics.storage import AnalyticsStore
from src.collector.runtime_state import (
    CollectorStateSnapshot,
    load_runtime_state,
    runtime_state_path,
)
from src.features import (
    ObservationGap,
    ScenarioPhase,
    WindowConfig,
    build_scheduled_windows,
)
from src.ml import TrainedArtifact, load_artifact, score_feature_rows

LOGGER = logging.getLogger(__name__)
NORMAL_SCENARIO = "normal"
PROTOCOL_SCENARIOS = {
    "k1": "normal",
    "k2": "message_burst",
    "k3": "abnormal_interval",
    "k4": "abnormal_payload",
    "normal": "normal",
    "message_burst": "message_burst",
    "abnormal_interval": "abnormal_interval",
    "abnormal_payload": "abnormal_payload",
}


@dataclass(frozen=True, slots=True)
class AnalyticsConfig:
    database_path: Path
    run_id: str
    collection_session_id: str
    device_ids: tuple[str, ...]
    run_start_unix: float
    run_end_unix: float
    scenario: str = NORMAL_SCENARIO
    model_directory: Path | None = None
    poll_seconds: float = 0.5
    settle_seconds: float = 1.0
    ready_file: Path | None = None
    collector_state_path: Path | None = None

    def __post_init__(self) -> None:
        if not self.run_id or not self.collection_session_id:
            raise ValueError("run and collection session IDs are required")
        if not self.device_ids or len(set(self.device_ids)) != len(self.device_ids):
            raise ValueError("device IDs must be non-empty and unique")
        if not (
            math.isfinite(self.run_start_unix)
            and math.isfinite(self.run_end_unix)
            and self.run_start_unix < self.run_end_unix
        ):
            raise ValueError("run bounds must be finite with start < end")
        if self.poll_seconds <= 0 or self.settle_seconds < 0:
            raise ValueError("poll must be positive and settle must be non-negative")


@dataclass(frozen=True, slots=True)
class ProcessSummary:
    cutoff_unix: float
    scheduled_windows: int
    scoreable_windows: int
    new_predictions: int
    feature_windows: int
    window_predictions: int
    inference_calls: int
    anomaly_alerts: int
    phase_log_complete: bool
    collector_state_available: bool
    collector_queue_depth: int | None
    collector_dropped_messages: int | None


def _unix_from_utc(value: str) -> float:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


class AnalyticsService:
    """Build due windows, persist them, and score unseen usable rows."""

    def __init__(
        self,
        config: AnalyticsConfig,
        *,
        store: AnalyticsStore | None = None,
        artifact: TrainedArtifact | None = None,
    ) -> None:
        self.config = config
        self.store = store or AnalyticsStore(config.database_path)
        self._owns_store = store is None
        self.collector_state_path = config.collector_state_path or runtime_state_path(
            config.database_path
        )
        # Unit/offline callers remain supported without a running Collector.
        # Once a sidecar has been observed, a transient read failure must block
        # closure rather than silently falling back to an unsafe cutoff.
        self._collector_state_expected = self.collector_state_path.is_file()
        self.artifact = artifact
        if self.artifact is None and config.model_directory is not None:
            self.artifact = load_artifact(config.model_directory, trusted=True)

    def close(self) -> None:
        if self._owns_store:
            self.store.close()

    def _collector_snapshot(self) -> CollectorStateSnapshot | None:
        if self.collector_state_path.is_file():
            self._collector_state_expected = True
        snapshot = load_runtime_state(self.collector_state_path)
        if snapshot is None and self._collector_state_expected:
            LOGGER.error(
                "collector state is expected but unavailable path=%s",
                self.collector_state_path,
            )
        return snapshot

    def _safe_cutoff(
        self, requested_cutoff: float, snapshot: CollectorStateSnapshot | None
    ) -> float:
        cutoff = min(
            max(requested_cutoff, self.config.run_start_unix),
            self.config.run_end_unix,
        )
        if snapshot is None:
            return self.config.run_start_unix if self._collector_state_expected else cutoff
        oldest = snapshot.oldest_pending_received_at_unix
        if snapshot.queue_depth and oldest is not None:
            # A message received exactly at a window's end is outside that
            # half-open window, so closing through ``oldest`` is safe.
            cutoff = min(cutoff, oldest)
        return max(self.config.run_start_unix, cutoff)

    def _observation_gaps(
        self, snapshot: CollectorStateSnapshot | None
    ) -> Mapping[str, Sequence[ObservationGap]]:
        if snapshot is None or not snapshot.loss_events:
            return {}
        output: dict[str, list[ObservationGap]] = {}
        configured = set(self.config.device_ids)
        for event in snapshot.loss_events:
            if event.get("message_type") != "telemetry":
                continue
            device_id = str(event.get("device_id") or "")
            if device_id not in configured:
                continue
            try:
                lost_at = float(event["received_at_unix"])
            except (KeyError, TypeError, ValueError):
                continue
            if not self.config.run_start_unix <= lost_at < self.config.run_end_unix:
                continue
            reason = str(event.get("reason") or "collector_message_loss")
            output.setdefault(device_id, []).append(
                ObservationGap(
                    start_unix=lost_at,
                    end_unix=lost_at + 1e-6,
                    reason=f"collector_{reason}",
                )
            )
        return output

    def _scenario_phase(self) -> tuple[ScenarioPhase | None, bool]:
        expected_scenario = PROTOCOL_SCENARIOS.get(self.config.scenario.lower())
        if expected_scenario is None:
            LOGGER.error("unknown configured scenario %s", self.config.scenario)
            return None, False
        if expected_scenario == NORMAL_SCENARIO:
            return None, True
        # experiment_run_id is a device-generated protocol UUID, distinct from
        # the human-readable campaign run ID. The database is isolated per
        # campaign run, so pair the single expected protocol event sequence.
        events = [
            row
            for row in self.store.scenario_events()
            if str(row["scenario"]) == expected_scenario
        ]
        protocol_run_ids = {str(row["scenario_run_id"]) for row in events}
        if len(protocol_run_ids) != 1:
            return None, False
        protocol_run_id = next(iter(protocol_run_ids))
        started = next(
            (
                row
                for row in events
                if row["event_type"] == "scenario_started"
                and row["scenario_run_id"] == protocol_run_id
            ),
            None,
        )
        ended = next(
            (
                row
                for row in reversed(events)
                if row["event_type"] == "scenario_ended"
                and row["scenario_run_id"] == protocol_run_id
            ),
            None,
        )
        if started is None or ended is None:
            return None, False
        start_unix = _unix_from_utc(str(started["event_at"]))
        end_unix = _unix_from_utc(str(ended["event_at"]))
        if not self.config.run_start_unix <= start_unix < end_unix <= self.config.run_end_unix:
            LOGGER.error(
                "scenario phase lies outside run bounds run_id=%s start=%s end=%s",
                self.config.run_id,
                start_unix,
                end_unix,
            )
            return None, False
        return (
            ScenarioPhase(
                phase_id=protocol_run_id,
                target_device_id=str(started["device_id"]),
                start_unix=start_unix,
                end_unix=end_unix,
            ),
            True,
        )

    def process_until(self, cutoff_unix: float) -> ProcessSummary:
        """Process all complete run-anchored windows through ``cutoff_unix``."""

        snapshot = self._collector_snapshot()
        cutoff = self._safe_cutoff(cutoff_unix, snapshot)
        observations = self.store.load_observations(
            run_start_unix=self.config.run_start_unix,
            run_end_unix=cutoff,
        )
        phase, phase_complete = self._scenario_phase()
        windows = build_scheduled_windows(
            run_id=self.config.run_id,
            collection_session_id=self.config.collection_session_id,
            device_ids=self.config.device_ids,
            run_start_unix=self.config.run_start_unix,
            run_end_unix=cutoff,
            observations=observations,
            scenario_phase=phase,
            phase_log_complete=phase_complete,
            observation_gaps=self._observation_gaps(snapshot),
            config=WindowConfig(),
        )
        self.store.persist_feature_windows(windows)

        scoreable = [window for window in windows if window.can_score]
        new_predictions = 0
        if self.artifact is not None and scoreable:
            existing = self.store.existing_prediction_ids(
                [window.window_id for window in scoreable]
            )
            unseen = [
                window for window in scoreable if window.window_id not in existing
            ]
            if unseen:
                rows = [window.features.as_dict() for window in unseen if window.features]
                started = time.perf_counter()
                scores = score_feature_rows(self.artifact, rows)
                inference_ms = (time.perf_counter() - started) * 1000.0
                new_predictions = self.store.persist_predictions(
                    unseen,
                    scores,
                    inference_call_id=str(uuid.uuid4()),
                    predicted_at_unix=time.time(),
                    inference_latency_ms=inference_ms,
                )
        counts = self.store.counts(self.config.run_id)
        return ProcessSummary(
            cutoff_unix=cutoff,
            scheduled_windows=len(windows),
            scoreable_windows=len(scoreable),
            new_predictions=new_predictions,
            feature_windows=counts["feature_windows"],
            window_predictions=counts["window_predictions"],
            inference_calls=counts["inference_calls"],
            anomaly_alerts=counts["anomaly_alerts"],
            phase_log_complete=phase_complete,
            collector_state_available=snapshot is not None,
            collector_queue_depth=(None if snapshot is None else snapshot.queue_depth),
            collector_dropped_messages=(
                None if snapshot is None else snapshot.dropped_message_count
            ),
        )

    def write_ready_file(self) -> None:
        if self.config.ready_file is None:
            return
        self.config.ready_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.config.ready_file.with_suffix(
            self.config.ready_file.suffix + ".tmp"
        )
        temporary.write_text(
            json.dumps(
                {
                    "ready": True,
                    "run_id": self.config.run_id,
                    "collection_session_id": self.config.collection_session_id,
                    "raw_schema_version": self.store.raw_schema_version(),
                    "model_loaded": self.artifact is not None,
                    "collector_state_path": str(self.collector_state_path),
                    "collector_state_available": (
                        load_runtime_state(self.collector_state_path) is not None
                    ),
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.config.ready_file)

    def run(self) -> ProcessSummary:
        """Poll until the run end, then make one deterministic final pass."""

        stopping = False

        def stop(*_: object) -> None:
            nonlocal stopping
            stopping = True

        previous_handlers = {
            sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)
        }
        try:
            self.write_ready_file()
            summary = self.process_until(self.config.run_start_unix)
            while not stopping:
                now = time.time()
                if now >= self.config.run_end_unix + self.config.settle_seconds:
                    final = self.process_until(self.config.run_end_unix)
                    if final.cutoff_unix >= self.config.run_end_unix:
                        return final
                    LOGGER.warning(
                        "waiting for Collector watermark before final window closure "
                        "run_id=%s cutoff=%s queue_depth=%s",
                        self.config.run_id,
                        final.cutoff_unix,
                        final.collector_queue_depth,
                    )
                due = min(
                    now - self.config.settle_seconds,
                    self.config.run_end_unix,
                )
                summary = self.process_until(due)
                time.sleep(self.config.poll_seconds)
            due = min(
                time.time() - self.config.settle_seconds,
                self.config.run_end_unix,
            )
            return self.process_until(due)
        finally:
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)


def _environment(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    return None if value is None or not value.strip() else value.strip()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-path", default=_environment("SQLITE_PATH"))
    parser.add_argument("--run-id", default=_environment("EXPERIMENT_RUN_ID"))
    parser.add_argument(
        "--collection-session-id", default=_environment("COLLECTION_SESSION_ID")
    )
    parser.add_argument(
        "--run-start-unix", type=float, default=_environment("RUN_START_UNIX")
    )
    parser.add_argument(
        "--run-end-unix", type=float, default=_environment("RUN_END_UNIX")
    )
    parser.add_argument("--scenario", default=_environment("RUN_SCENARIO", "normal"))
    parser.add_argument("--device-count", type=int, default=int(_environment("DEVICE_COUNT", "12") or 12))
    parser.add_argument("--device-ids", default=_environment("DEVICE_IDS"))
    parser.add_argument("--model-directory", default=_environment("MODEL_DIRECTORY"))
    parser.add_argument("--poll-seconds", type=float, default=float(_environment("ANALYTICS_POLL_SECONDS", "0.5") or 0.5))
    parser.add_argument("--settle-seconds", type=float, default=float(_environment("ANALYTICS_SETTLE_SECONDS", "1.0") or 1.0))
    parser.add_argument("--ready-file", default=_environment("ANALYTICS_READY_FILE"))
    parser.add_argument(
        "--collector-state-file", default=_environment("COLLECTOR_STATE_FILE")
    )
    parser.add_argument("--once", action="store_true", help="process through run end and exit")
    return parser


def _config(arguments: argparse.Namespace, parser: argparse.ArgumentParser) -> AnalyticsConfig:
    required = {
        "database path": arguments.database_path,
        "run ID": arguments.run_id,
        "collection session ID": arguments.collection_session_id,
        "run start": arguments.run_start_unix,
        "run end": arguments.run_end_unix,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        parser.error("missing required values: " + ", ".join(missing))
    if arguments.device_ids:
        device_ids = tuple(
            item.strip() for item in arguments.device_ids.split(",") if item.strip()
        )
    else:
        device_ids = tuple(
            f"iot-{index:03d}" for index in range(1, arguments.device_count + 1)
        )
    return AnalyticsConfig(
        database_path=Path(arguments.database_path),
        run_id=str(arguments.run_id),
        collection_session_id=str(arguments.collection_session_id),
        device_ids=device_ids,
        run_start_unix=float(arguments.run_start_unix),
        run_end_unix=float(arguments.run_end_unix),
        scenario=str(arguments.scenario),
        model_directory=(
            None if arguments.model_directory is None else Path(arguments.model_directory)
        ),
        poll_seconds=arguments.poll_seconds,
        settle_seconds=arguments.settle_seconds,
        ready_file=(None if arguments.ready_file is None else Path(arguments.ready_file)),
        collector_state_path=(
            None
            if arguments.collector_state_file is None
            else Path(arguments.collector_state_file)
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = _parser()
    arguments = parser.parse_args(argv)
    config = _config(arguments, parser)
    service: AnalyticsService | None = None
    try:
        service = AnalyticsService(config)
        service.write_ready_file()
        summary = (
            service.process_until(config.run_end_unix)
            if arguments.once
            else service.run()
        )
    except (OSError, sqlite3.Error, ValueError, RuntimeError) as exc:
        LOGGER.error("analytics service failed: %s", exc)
        return 1
    finally:
        if service is not None:
            service.close()
    print(json.dumps(asdict(summary), sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
