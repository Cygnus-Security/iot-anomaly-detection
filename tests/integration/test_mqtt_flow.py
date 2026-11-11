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
    subscriber_ready = threading.Event()
    # The dedicated test namespace proves broker delivery without contaminating
    # the production collector's versioned telemetry subscription.
    topic = "test/integration-test/telemetry"
    payload = json.dumps({"probe": str(uuid.uuid4())})

    subscriber = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"integration-subscriber-{uuid.uuid4()}",
        protocol=mqtt.MQTTv311,
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

    def on_subscribe(
        client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[mqtt.ReasonCode],
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, mid, properties
        assert not reason_code_list or all(code < 128 for code in reason_code_list)
        subscriber_ready.set()

    def on_message(
        client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        del client, userdata
        if message.payload.decode() == payload:
            received.set()

    subscriber.on_connect = on_connect
    subscriber.on_subscribe = on_subscribe
    subscriber.on_message = on_message
    subscriber.connect(host, port, 30)
    subscriber.loop_start()
    assert subscriber_ready.wait(5)

    publisher_ready = threading.Event()
    publisher = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="integration-test",
        protocol=mqtt.MQTTv311,
    )
    publisher.username_pw_set("integration-test", device_password)

    def on_publisher_connect(
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, flags, properties
        assert reason_code == 0
        publisher_ready.set()

    publisher.on_connect = on_publisher_connect
    publisher.connect(host, port, 30)
    publisher.loop_start()
    assert publisher_ready.wait(5)
    info = publisher.publish(topic, payload, qos=1)
    info.wait_for_publish(timeout=5)
    assert info.is_published()
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
        protocol=mqtt.MQTTv311,
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


def test_per_device_and_collector_acls_reject_unauthorized_writes() -> None:
    require_integration()
    host, port, device_password, collector_password = settings()
    subscriber_ready = threading.Event()
    forbidden_received = threading.Event()
    device_forbidden_topic = "iot/v1/devices/iot-002/telemetry"
    collector_forbidden_topic = "iot/v1/devices/collector/telemetry"
    forbidden_probe = json.dumps({"acl_probe": str(uuid.uuid4())})

    subscriber = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"acl-observer-{uuid.uuid4()}",
        protocol=mqtt.MQTTv311,
    )
    subscriber.username_pw_set("collector", collector_password)

    def on_connect(
        observing_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        assert reason_code == 0
        observing_client.subscribe(
            [(device_forbidden_topic, 1), (collector_forbidden_topic, 1)]
        )

    def on_subscribe(
        observing_client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[mqtt.ReasonCode],
        properties: mqtt.Properties | None,
    ) -> None:
        del observing_client, userdata, mid, properties
        assert not reason_code_list or all(code < 128 for code in reason_code_list)
        subscriber_ready.set()

    def on_message(
        observing_client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        del observing_client, userdata
        if message.payload.decode() == forbidden_probe:
            forbidden_received.set()

    subscriber.on_connect = on_connect
    subscriber.on_subscribe = on_subscribe
    subscriber.on_message = on_message
    subscriber.connect(host, port, 30)
    subscriber.loop_start()
    assert subscriber_ready.wait(5)

    device_publisher_ready = threading.Event()
    publisher = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"acl-iot-001-{uuid.uuid4()}",
        protocol=mqtt.MQTTv311,
    )
    publisher.username_pw_set("iot-001", device_password)

    def on_device_publisher_connect(
        connected_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del connected_client, userdata, flags, properties
        assert reason_code == 0
        device_publisher_ready.set()

    publisher.on_connect = on_device_publisher_connect
    publisher.connect(host, port, 30)
    publisher.loop_start()
    assert device_publisher_ready.wait(5)
    denied = publisher.publish(device_forbidden_topic, forbidden_probe, qos=1)
    denied.wait_for_publish(timeout=5)
    assert denied.is_published()
    assert not forbidden_received.wait(1.0), "broker forwarded an ACL-denied publish"

    publisher.disconnect()
    publisher.loop_stop()

    collector_publisher_ready = threading.Event()
    collector_publisher = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"collector-publish-test-{uuid.uuid4()}",
        protocol=mqtt.MQTTv311,
    )
    collector_publisher.username_pw_set("collector", collector_password)

    def on_collector_publisher_connect(
        connected_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del connected_client, userdata, flags, properties
        assert reason_code == 0
        collector_publisher_ready.set()

    collector_publisher.on_connect = on_collector_publisher_connect
    collector_publisher.connect(host, port, 30)
    collector_publisher.loop_start()
    assert collector_publisher_ready.wait(5)
    denied = collector_publisher.publish(
        collector_forbidden_topic, forbidden_probe, qos=1
    )
    denied.wait_for_publish(timeout=5)
    assert denied.is_published()
    assert not forbidden_received.wait(1.0), (
        "collector inherited device write permissions"
    )
    collector_publisher.disconnect()
    collector_publisher.loop_stop()
    subscriber.disconnect()
    subscriber.loop_stop()


def test_ungraceful_disconnect_publishes_retained_last_will() -> None:
    require_integration()
    host, port, device_password, collector_password = settings()
    topic = "test/integration-test/status"
    payload = json.dumps({"probe": str(uuid.uuid4()), "state": "offline"})
    received = threading.Event()
    subscriber_ready = threading.Event()

    subscriber = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"lwt-observer-{uuid.uuid4()}",
        protocol=mqtt.MQTTv311,
    )
    subscriber.username_pw_set("collector", collector_password)

    def on_subscriber_connect(
        observing_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        assert reason_code == 0
        observing_client.subscribe(topic, qos=1)

    def on_subscriber_subscribe(
        observing_client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[mqtt.ReasonCode],
        properties: mqtt.Properties | None,
    ) -> None:
        del observing_client, userdata, mid, properties
        assert not reason_code_list or all(code < 128 for code in reason_code_list)
        subscriber_ready.set()

    def on_message(
        observing_client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        del observing_client, userdata
        if message.payload.decode() == payload:
            assert not message.retain
            received.set()

    subscriber.on_connect = on_subscriber_connect
    subscriber.on_subscribe = on_subscriber_subscribe
    subscriber.on_message = on_message
    subscriber.connect(host, port, 30)
    subscriber.loop_start()
    assert subscriber_ready.wait(5)

    publisher_ready = threading.Event()
    publisher = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="integration-test",
        protocol=mqtt.MQTTv311,
    )
    publisher.username_pw_set("integration-test", device_password)
    publisher.will_set(topic, payload, qos=1, retain=True)

    def on_publisher_connect(
        connected_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del connected_client, userdata, flags, properties
        assert reason_code == 0
        publisher_ready.set()

    publisher.on_connect = on_publisher_connect
    publisher.connect(host, port, 30)
    publisher.loop_start()
    assert publisher_ready.wait(5)
    publisher.loop_stop()
    publisher._sock_close()
    assert received.wait(5), "broker did not publish the configured Last Will"

    cleanup = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="integration-test",
        protocol=mqtt.MQTTv311,
    )
    cleanup.username_pw_set("integration-test", device_password)
    cleanup.connect(host, port, 30)
    cleanup.loop_start()
    cleanup.publish(topic, b"", qos=1, retain=True).wait_for_publish(timeout=5)
    cleanup.disconnect()
    cleanup.loop_stop()
    subscriber.disconnect()
    subscriber.loop_stop()


def test_retained_status_is_delivered_to_a_late_subscriber() -> None:
    require_integration()
    host, port, device_password, collector_password = settings()
    topic = "test/integration-test/status"
    payload = json.dumps({"probe": str(uuid.uuid4()), "state": "online"})

    publisher = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="integration-test",
        protocol=mqtt.MQTTv311,
    )
    publisher.username_pw_set("integration-test", device_password)
    publisher.connect(host, port, 30)
    publisher.loop_start()
    info = publisher.publish(topic, payload, qos=1, retain=True)
    info.wait_for_publish(timeout=5)

    received = threading.Event()
    retained_flags: list[bool] = []
    subscriber = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"retained-observer-{uuid.uuid4()}",
        protocol=mqtt.MQTTv311,
    )
    subscriber.username_pw_set("collector", collector_password)

    def on_connect(
        observing_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        assert reason_code == 0
        observing_client.subscribe(topic, qos=1)

    def on_message(
        observing_client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        del observing_client, userdata
        if message.payload.decode() == payload:
            retained_flags.append(bool(message.retain))
            received.set()

    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    subscriber.connect(host, port, 30)
    subscriber.loop_start()
    assert received.wait(5)
    assert retained_flags == [True]

    subscriber.disconnect()
    subscriber.loop_stop()
    publisher.publish(topic, b"", qos=1, retain=True).wait_for_publish(timeout=5)
    publisher.disconnect()
    publisher.loop_stop()


def test_broker_does_not_forward_payload_over_limit() -> None:
    require_integration()
    host, port, device_password, collector_password = settings()
    topic = "test/integration-test/telemetry"
    received = threading.Event()
    subscriber_ready = threading.Event()

    subscriber = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"size-observer-{uuid.uuid4()}",
        protocol=mqtt.MQTTv311,
    )
    subscriber.username_pw_set("collector", collector_password)

    def on_connect(
        observing_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        assert reason_code == 0
        observing_client.subscribe(topic, qos=1)

    def on_subscribe(
        observing_client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[mqtt.ReasonCode],
        properties: mqtt.Properties | None,
    ) -> None:
        del observing_client, userdata, mid, properties
        assert not reason_code_list or all(code < 128 for code in reason_code_list)
        subscriber_ready.set()

    def on_message(
        observing_client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        del observing_client, userdata, message
        received.set()

    subscriber.on_connect = on_connect
    subscriber.on_subscribe = on_subscribe
    subscriber.on_message = on_message
    subscriber.connect(host, port, 30)
    subscriber.loop_start()
    assert subscriber_ready.wait(5)

    publisher_ready = threading.Event()
    publisher_disconnected = threading.Event()
    publisher = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="integration-test",
        protocol=mqtt.MQTTv311,
    )
    publisher.username_pw_set("integration-test", device_password)

    def on_publisher_connect(
        connected_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del connected_client, userdata, flags, properties
        assert reason_code == 0
        publisher_ready.set()

    def on_publisher_disconnect(
        disconnected_client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del disconnected_client, userdata, disconnect_flags, reason_code, properties
        publisher_disconnected.set()

    publisher.on_connect = on_publisher_connect
    publisher.on_disconnect = on_publisher_disconnect
    publisher.connect(host, port, 30)
    publisher.loop_start()
    assert publisher_ready.wait(5)
    oversized = publisher.publish(topic, b"x" * 4097, qos=1)
    deadline = time.monotonic() + 5
    while (
        not oversized.is_published()
        and not publisher_disconnected.is_set()
        and time.monotonic() < deadline
    ):
        publisher_disconnected.wait(0.05)
    assert oversized.is_published() or publisher_disconnected.is_set(), (
        "broker did not acknowledge or reject the oversized publication"
    )
    assert not received.wait(1.0), "broker forwarded a payload over 4096 bytes"

    publisher.disconnect()
    publisher.loop_stop()
    subscriber.disconnect()
    subscriber.loop_stop()


def test_disconnect_and_recovery_publish_observable_status_sequence() -> None:
    """A real disconnect is followed by an explicit recovered online status."""

    require_integration()
    host, port, device_password, collector_password = settings()
    topic = "test/integration-test/status"
    probe = str(uuid.uuid4())
    online = json.dumps({"probe": probe, "state": "online"})
    offline = json.dumps({"probe": probe, "state": "offline"})
    subscriber_ready = threading.Event()
    online_seen = threading.Event()
    offline_seen = threading.Event()
    recovered_seen = threading.Event()
    observed: list[str] = []

    subscriber = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"recovery-observer-{uuid.uuid4()}",
        protocol=mqtt.MQTTv311,
    )
    subscriber.username_pw_set("collector", collector_password)

    def on_subscriber_connect(
        observing_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        assert reason_code == 0
        observing_client.subscribe(topic, qos=1)

    def on_subscriber_subscribe(
        observing_client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[mqtt.ReasonCode],
        properties: mqtt.Properties | None,
    ) -> None:
        del observing_client, userdata, mid, properties
        assert not reason_code_list or all(code < 128 for code in reason_code_list)
        subscriber_ready.set()

    def on_status(
        observing_client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        del observing_client, userdata
        decoded = message.payload.decode()
        if decoded not in {online, offline}:
            return
        observed.append(decoded)
        if decoded == offline:
            offline_seen.set()
        elif offline_seen.is_set():
            recovered_seen.set()
        else:
            online_seen.set()

    subscriber.on_connect = on_subscriber_connect
    subscriber.on_subscribe = on_subscriber_subscribe
    subscriber.on_message = on_status
    subscriber.connect(host, port, 30)
    subscriber.loop_start()
    assert subscriber_ready.wait(5)

    device = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="integration-test",
        protocol=mqtt.MQTTv311,
    )
    device.username_pw_set("integration-test", device_password)
    device.will_set(topic, offline, qos=1, retain=True)
    device.connect(host, port, 30)
    device.loop_start()
    published = device.publish(topic, online, qos=1, retain=True)
    published.wait_for_publish(timeout=5)
    assert published.is_published()
    assert online_seen.wait(5)

    device.loop_stop()
    device._sock_close()
    assert offline_seen.wait(5), "Last Will was not observed after socket loss"

    recovered = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="integration-test",
        protocol=mqtt.MQTTv311,
    )
    recovered.username_pw_set("integration-test", device_password)
    recovered.will_set(topic, offline, qos=1, retain=True)
    recovered.connect(host, port, 30)
    recovered.loop_start()
    published = recovered.publish(topic, online, qos=1, retain=True)
    published.wait_for_publish(timeout=5)
    assert published.is_published()
    assert recovered_seen.wait(5), "recovered online status was not observed"
    assert observed.index(offline) < len(observed) - 1

    cleared = recovered.publish(topic, b"", qos=1, retain=True)
    cleared.wait_for_publish(timeout=5)
    recovered.disconnect()
    recovered.loop_stop()
    subscriber.disconnect()
    subscriber.loop_stop()
