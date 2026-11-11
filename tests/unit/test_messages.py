from __future__ import annotations

import json

import pytest

from src.common.messages import (
    MessageValidationError,
    decode_object,
    topic_identity,
    validate_command,
    validate_telemetry,
)


def telemetry() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "message_id": "7b01b841-8d5f-4976-8868-e682f28a15f0",
        "device_id": "iot-001",
        "device_type": "environment_sensor",
        "session_id": "29ab4b25-c9ad-4f11-86b3-dabfb06a02cc",
        "sequence_no": 1,
        "sent_at_utc": "2026-11-08T09:00:00Z",
        "measurements": {"temperature_c": 23.5, "relative_humidity_pct": 55.0},
    }


def test_decode_and_validate_telemetry() -> None:
    encoded = json.dumps(telemetry()).encode()
    decoded = decode_object(encoded)
    validate_telemetry(decoded, "iot-001")


def test_rejects_payload_topic_identity_mismatch() -> None:
    with pytest.raises(MessageValidationError, match="does not match"):
        validate_telemetry(telemetry(), "iot-002")


@pytest.mark.parametrize(
    "topic",
    ["iot/v1/devices/iot-001/command", "bad/topic", "iot//status"],
)
def test_rejects_unsupported_observation_topic(topic: str) -> None:
    with pytest.raises(MessageValidationError):
        topic_identity(topic)


def test_command_duration_and_scenario_are_validated() -> None:
    command = {
        "schema_version": "1.0",
        "command": "set_scenario",
        "request_id": "c410f117-93c7-40ca-9147-e57fa4088e83",
        "scenario": "message_burst",
        "duration_seconds": 30,
        "seed": 42,
    }
    validate_command(command)
    command["duration_seconds"] = 0
    with pytest.raises(MessageValidationError, match="duration_seconds"):
        validate_command(command)


def test_telemetry_requires_explicit_utc_and_uuid_session() -> None:
    payload = telemetry()
    payload["sent_at_utc"] = "2026-11-08T09:00:00+07:00"
    with pytest.raises(MessageValidationError, match="ending in Z"):
        validate_telemetry(payload, "iot-001")

    payload["sent_at_utc"] = "2026-11-08T09:00:00Z"
    payload["session_id"] = "not-a-uuid"
    with pytest.raises(MessageValidationError, match="session_id"):
        validate_telemetry(payload, "iot-001")


def test_versioned_topics_extract_device_identity() -> None:
    assert topic_identity("iot/v1/devices/iot-001/telemetry") == (
        "iot-001",
        "telemetry",
    )
    assert topic_identity("experiments/v1/devices/iot-001/events") == (
        "iot-001",
        "experiment",
    )


def test_telemetry_rejects_non_finite_measurements_and_invalid_optional_fields() -> None:
    payload = telemetry()
    payload["measurements"] = {"temperature_c": float("inf")}
    with pytest.raises(MessageValidationError, match="non-finite"):
        validate_telemetry(payload, "iot-001")

    payload = telemetry()
    payload["uptime_seconds"] = -1
    with pytest.raises(MessageValidationError, match="uptime_seconds"):
        validate_telemetry(payload, "iot-001")


@pytest.mark.parametrize(
    "forbidden_key",
    [
        "scenario",
        "experiment_run_id",
        "scenario_label",
        "ground_truth",
        "ground_truth_label",
        "is_anomaly",
        "attack_type",
        "label",
    ],
)
def test_telemetry_rejects_ground_truth_at_any_object_level(
    forbidden_key: str,
) -> None:
    payload = telemetry()
    payload["measurements"] = {
        "temperature_c": 23.5,
        "diagnostics": {forbidden_key: "must-not-leak"},
    }
    with pytest.raises(MessageValidationError, match="ground truth"):
        validate_telemetry(payload, "iot-001")


def test_telemetry_rejects_numbers_outside_supported_range() -> None:
    payload = telemetry()
    payload["measurements"] = {"unsafe_integer": 10**1000}
    with pytest.raises(MessageValidationError, match="supported range"):
        validate_telemetry(payload, "iot-001")
