from __future__ import annotations

import json
import os
import threading
import time
import uuid
from typing import Any

import paho.mqtt.client as mqtt
import pytest


pytestmark = pytest.mark.integration


def require_integration() -> None:
    if os.getenv("RUN_MQTT_INTEGRATION") != "1":
        pytest.skip("set RUN_MQTT_INTEGRATION=1 and provide a broker")


def settings() -> tuple[str, int, str, str]:
    return (
        os.getenv("MQTT_HOST", "localhost"),
        int(os.getenv("MQTT_PORT", "1883")),
        os.environ["DEVICE_PASSWORD"],
        os.environ["COLLECTOR_PASSWORD"],
    )


def test_authenticated_publish_subscribe_flow() -> None:
    require_integration()
    host, port, device_password, collector_password = settings()
    received = threading.Event()
    connected = threading.Event()
    # The dedicated test namespace proves broker delivery without contaminating
    # the production collector's `iot/+/telemetry` dataset subscription.
    topic = "test/integration-test/telemetry"
    payload = json.dumps({"probe": str(uuid.uuid4())})

    subscriber = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"integration-subscriber-{uuid.uuid4()}",
        protocol=mqtt.MQTTv5,
    )
    subscriber.username_pw_set("collector", collector_password)

    def on_connect(
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        assert reason_code == 0
        client.subscribe(topic, qos=1)
        connected.set()

    def on_message(
        client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        del client, userdata
        if message.payload.decode() == payload:
            received.set()

    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    subscriber.connect(host, port, 30)
    subscriber.loop_start()
    assert connected.wait(5)
    time.sleep(0.1)

    publisher = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="integration-test",
        protocol=mqtt.MQTTv5,
    )
    publisher.username_pw_set("integration-test", device_password)
    publisher.connect(host, port, 30)
    publisher.loop_start()
    info = publisher.publish(topic, payload, qos=1)
    info.wait_for_publish(timeout=5)
    assert received.wait(5)

    publisher.disconnect()
    publisher.loop_stop()
    subscriber.disconnect()
    subscriber.loop_stop()


def test_invalid_password_is_rejected() -> None:
    require_integration()
    host, port, _, _ = settings()
    rejected = threading.Event()
    reason: list[str] = []
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"invalid-auth-{uuid.uuid4()}",
        protocol=mqtt.MQTTv5,
    )
    client.username_pw_set("collector", "definitely-wrong-password")

    def on_connect(
        connected_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del connected_client, userdata, flags, properties
        if reason_code != 0:
            reason.append(str(reason_code))
            rejected.set()

    client.on_connect = on_connect
    client.connect(host, port, 30)
    client.loop_start()
    assert rejected.wait(5), "broker unexpectedly accepted an invalid password"
    assert reason
    client.disconnect()
    client.loop_stop()


def test_device_acl_rejects_another_devices_topic() -> None:
    require_integration()
    host, port, device_password, _ = settings()
    publish_ack = threading.Event()
    reasons: list[str] = []
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="integration-test",
        protocol=mqtt.MQTTv5,
    )
    client.username_pw_set("integration-test", device_password)

    def on_publish(
        publishing_client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del publishing_client, userdata, mid, properties
        reasons.append(str(reason_code))
        publish_ack.set()

    client.on_publish = on_publish
    client.connect(host, port, 30)
    client.loop_start()
    client.publish("iot/device-001/telemetry", "{}", qos=1)
    assert publish_ack.wait(5)
    assert any("not authorized" in item.lower() for item in reasons), reasons
    client.disconnect()
    client.loop_stop()
