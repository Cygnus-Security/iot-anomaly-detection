from __future__ import annotations

import json
import threading
from typing import Any

import paho.mqtt.client as mqtt
import pytest

from src.common.config import SimulatorSettings
from src.simulator.device import VirtualDevice


@pytest.fixture
def device() -> VirtualDevice:
    settings = SimulatorSettings(
        mqtt_host="localhost",
        mqtt_port=1883,
        mqtt_keepalive=30,
        device_password="test-password",
        device_count=1,
        base_interval_seconds=5.0,
        interval_jitter_seconds=0.5,
        initial_spread_seconds=5.0,
        heartbeat_seconds=30.0,
        simulation_seed=7,
    )
    return VirtualDevice("iot-001", settings, seed=7, stop_event=threading.Event())


def test_duplicate_scenario_command_republishes_same_acknowledgement(
    device: VirtualDevice, monkeypatch: pytest.MonkeyPatch
) -> None:
    publications: list[tuple[str, dict[str, Any]]] = []

    def capture(
        topic: str,
        payload: dict[str, Any],
        *,
        wait_for_publish: bool = False,
    ) -> None:
        del wait_for_publish
        publications.append((topic, payload))

    monkeypatch.setattr(device, "_publish", capture)
    command = {
        "schema_version": "1.0",
        "command": "set_scenario",
        "request_id": "c410f117-93c7-40ca-9147-e57fa4088e83",
        "scenario": "message_burst",
        "duration_seconds": 30,
        "seed": 42,
    }
    message = mqtt.MQTTMessage()
    message.payload = json.dumps(command).encode()

    device._on_message(device._client, None, message)
    first_run_id = device._active_scenario.run_id if device._active_scenario else None
    device._on_message(device._client, None, message)

    assert first_run_id is not None
    assert device._active_scenario is not None
    assert device._active_scenario.run_id == first_run_id
    assert len(publications) == 2
    assert publications[0][0] == "experiments/v1/devices/iot-001/events"
    assert publications[0][1] == publications[1][1]
    assert publications[0][1]["event"] == "scenario_started"


def test_device_becomes_ready_only_after_command_subscription_acknowledgement(
    device: VirtualDevice, monkeypatch: pytest.MonkeyPatch
) -> None:
    states: list[str] = []
    monkeypatch.setattr(device, "_publish_status", states.append)
    monkeypatch.setattr(
        device._client,
        "subscribe",
        lambda topic, qos: (mqtt.MQTT_ERR_SUCCESS, 1),
    )

    device._on_connect(device._client, None, {}, 0, None)
    assert not device._connected.is_set()
    assert states == []

    device._on_subscribe(device._client, None, 1, [], None)
    assert device._connected.is_set()
    assert states == ["online"]
