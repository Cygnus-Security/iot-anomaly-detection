from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.analytics.storage import AnalyticsStore
from src.collector.storage import ObservedMessage, ObservationStore
from src.dashboard.data import DashboardRepository
from src.features import FeatureVector, FeatureWindow, GroundTruth
from src.ml import ScoreResult

DEVICE_ID = "iot-001"
SESSION_ID = "29ab4b25-c9ad-4f11-86b3-dabfb06a02cc"
RUN_ID = "71fdc845-3be7-4711-a673-46894844274a"


def observation(topic: str, payload: dict[str, object], at: float) -> ObservedMessage:
    return ObservedMessage(
        topic=topic,
        payload=json.dumps(payload).encode(),
        qos=1,
        retain=False,
        duplicate=False,
        received_at_unix=at,
    )


def common_payload(timestamp: str) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "device_id": DEVICE_ID,
        "session_id": SESSION_ID,
        "sent_at_utc": timestamp,
    }


def test_dashboard_queries_live_observations_without_inventing_detections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "dashboard.db"
    now = time.time()
    timestamp = "2026-11-08T09:00:00Z"
    store = ObservationStore(str(database))

    online = common_payload(timestamp)
    online.update({"state": "online"})
    store.process(
        observation(f"iot/v1/devices/{DEVICE_ID}/status", online, now - 2.0)
    )

    for sequence, received_at in ((1, now - 1.0), (2, now)):
        telemetry = common_payload(timestamp)
        telemetry.update(
            {
                "device_type": "environment_sensor",
                "sequence_no": sequence,
                "measurements": {
                    "temperature_c": 23.5 + sequence,
                    "relative_humidity_pct": 50.0,
                },
            }
        )
        store.process(
            observation(
                f"iot/v1/devices/{DEVICE_ID}/telemetry", telemetry, received_at
            )
        )

    # A QoS 1 application duplicate remains auditable but is excluded from
    # dashboard counts, traffic rates, and device history.
    store.process(
        observation(
            f"iot/v1/devices/{DEVICE_ID}/telemetry", telemetry, now + 0.01
        )
    )

    for event, received_at in (
        ("scenario_started", now - 0.8),
        ("scenario_ended", now - 0.2),
    ):
        scenario = {
            "schema_version": "1.0",
            "event_id": (
                "cb8df2e0-3488-4c44-91f8-239b8016675b"
                if event == "scenario_started"
                else "9f9f448a-aa2d-4272-ad3a-b47d43f67fd5"
            ),
            "device_id": DEVICE_ID,
            "session_id": SESSION_ID,
            "event_at_utc": timestamp,
            "event": event,
            "scenario": "message_burst",
            "experiment_run_id": RUN_ID,
            "request_id": "c410f117-93c7-40ca-9147-e57fa4088e83",
            "seed": 42,
            "duration_seconds": 1,
            "details": {},
        }
        store.process(
            observation(
                f"experiments/v1/devices/{DEVICE_ID}/events",
                scenario,
                received_at,
            )
        )
    store.close()

    repository = DashboardRepository(str(database))
    repository.check_ready()
    overview = repository.overview(now)
    assert overview.device_count == 1
    assert overview.telemetry_count == 2
    assert overview.invalid_payload_count == 0
    assert overview.abnormal_scenario_runs == 1

    devices = repository.devices(window_seconds=300, now_unix=now)
    assert devices.loc[0, "observed_state"] == "online"
    assert devices.loc[0, "telemetry_in_window"] == 2

    traffic = repository.traffic_series(window_seconds=300, now_unix=now)
    assert traffic["messages"].sum() == 2
    history = repository.device_telemetry(DEVICE_ID)
    assert list(history["sequence"]) == [1, 2]
    assert list(history["temperature_c"]) == [24.5, 25.5]

    runs = repository.scenario_runs()
    assert runs.loc[0, "run_state"] == "completed"
    assert runs.loc[0, "scenario"] == "message_burst"
    assert not repository.has_detection_table()

    monkeypatch.setenv("SQLITE_PATH", str(database))
    app_path = Path(__file__).resolve().parents[2] / "src/dashboard/app.py"
    app = AppTest.from_file(app_path).run(timeout=20)
    assert not app.exception
    assert [item.value for item in app.title] == ["IoT Anomaly Detection System"]
    assert len(app.metric) == 6
    assert [item.label for item in app.tabs] == [
        "Overview",
        "Devices",
        "Experiment Scenarios",
        "AI & Alerts",
    ]
    app.selectbox[1].select(DEVICE_ID).run(timeout=20)
    assert not app.exception
    assert len(app.metric) == 10


def test_dashboard_exposes_all_predictions_and_actual_alert_counts(
    tmp_path: Path,
) -> None:
    database = tmp_path / "phase3-dashboard.db"
    store = ObservationStore(str(database))
    store.close()
    window = FeatureWindow(
        run_id="test-k2-01",
        collection_session_id="collection-01",
        device_id=DEVICE_ID,
        window_index=0,
        window_start_unix=1_800_000_000.0,
        window_end_unix=1_800_000_020.0,
        observed_message_count=4,
        feature_status="ok",
        exclusion_reason=None,
        features=FeatureVector(
            message_count=4,
            message_rate_hz=0.2,
            payload_mean_bytes=370.0,
            payload_std_bytes=1.0,
            interarrival_mean_ms=5_000.0,
            interarrival_std_ms=25.0,
            peak_messages_per_second=1,
        ),
        ground_truth=GroundTruth("normal", None, False),
    )
    with AnalyticsStore(database) as analytics:
        analytics.persist_feature_windows([window])
        analytics.persist_predictions(
            [window],
            [
                ScoreResult(
                    score=-0.2,
                    predicted_label="anomaly",
                    threshold=-0.1,
                    model_version="iforest-test-v1",
                )
            ],
            inference_call_id="call-001",
            predicted_at_unix=1_800_000_021.0,
            inference_latency_ms=0.75,
        )

    repository = DashboardRepository(str(database))
    assert repository.has_prediction_table()
    assert repository.prediction_count() == 1
    assert repository.alert_count() == 1
    predictions = repository.predictions(device_id=DEVICE_ID)
    assert predictions.loc[0, "predicted_label"] == "anomaly"
    assert predictions.loc[0, "ground_truth_label"] == "normal"
    assert predictions.loc[0, "anomaly_score"] == pytest.approx(-0.2)
    assert repository.detections().loc[0, "window_id"] == window.window_id


def test_dashboard_marks_stale_then_recovers_after_new_telemetry(tmp_path: Path) -> None:
    database = tmp_path / "stale-recovery.db"
    now = time.time()
    timestamp = "2026-11-08T09:00:00Z"
    store = ObservationStore(str(database))
    online = common_payload(timestamp)
    online.update({"state": "online"})
    store.process(
        observation(f"iot/v1/devices/{DEVICE_ID}/status", online, now - 60.0)
    )

    repository = DashboardRepository(str(database))
    stale = repository.devices(
        window_seconds=300, now_unix=now, stale_after_seconds=15.0
    )
    assert stale.loc[0, "observed_state"] == "stale"

    telemetry = common_payload(timestamp)
    telemetry.update(
        {
            "device_type": "environment_sensor",
            "sequence_no": 1,
            "measurements": {
                "temperature_c": 24.0,
                "relative_humidity_pct": 50.0,
            },
        }
    )
    store.process(
        observation(f"iot/v1/devices/{DEVICE_ID}/telemetry", telemetry, now)
    )
    store.close()
    recovered = repository.devices(
        window_seconds=300, now_unix=now + 1.0, stale_after_seconds=15.0
    )
    assert recovered.loc[0, "observed_state"] == "online"
