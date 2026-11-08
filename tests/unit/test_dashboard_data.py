from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.collector.storage import ObservedMessage, ObservationStore
from src.dashboard.data import DashboardRepository


def observation(topic: str, payload: dict[str, object], at: float) -> ObservedMessage:
    return ObservedMessage(
        topic=topic,
        payload=json.dumps(payload).encode(),
        qos=1,
        retain=False,
        duplicate=False,
        received_at_unix=at,
    )


def common_payload(message_id: str, timestamp: str) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "message_id": message_id,
        "device_id": "device-001",
        "timestamp": timestamp,
    }


def test_dashboard_queries_live_observations_without_inventing_detections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "dashboard.db"
    now = time.time()
    timestamp = "2026-11-08T09:00:00Z"
    store = ObservationStore(str(database))

    online = common_payload("online-1", timestamp)
    online.update({"event": "online", "details": {}})
    store.process(observation("iot/device-001/status", online, now - 2.0))

    for sequence, received_at in ((1, now - 1.0), (2, now)):
        telemetry = common_payload(f"telemetry-{sequence}", timestamp)
        telemetry.update(
            {
                "sequence": sequence,
                "sensor": {
                    "temperature_c": 23.5 + sequence,
                    "humidity_percent": 50.0,
                },
                "device": {"firmware": "test", "uptime_seconds": sequence},
            }
        )
        store.process(
            observation("iot/device-001/telemetry", telemetry, received_at)
        )

    for event, received_at in (
        ("scenario_started", now - 0.8),
        ("scenario_ended", now - 0.2),
    ):
        scenario = common_payload(f"event-{event}", timestamp)
        scenario.update(
            {
                "event": event,
                "scenario": "message_burst",
                "scenario_run_id": "run-1",
                "request_id": "request-1",
                "seed": 42,
                "duration_seconds": 1,
                "details": {},
            }
        )
        store.process(observation("iot/device-001/status", scenario, received_at))
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
    history = repository.device_telemetry("device-001")
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
        "Tổng quan",
        "Thiết bị",
        "Kịch bản thử nghiệm",
        "AI & Cảnh báo",
    ]
    app.selectbox[1].select("device-001").run(timeout=20)
    assert not app.exception
    assert len(app.metric) == 10
