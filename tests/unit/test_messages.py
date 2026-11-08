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
        "message_id": "message-1",
        "device_id": "device-001",
        "timestamp": "2026-11-08T09:00:00Z",
        "sequence": 1,
        "sensor": {"temperature_c": 23.5, "humidity_percent": 55.0},
        "device": {"firmware": "test", "uptime_seconds": 1.0},
    }


def test_decode_and_validate_telemetry() -> None:
    encoded = json.dumps(telemetry()).encode()
    decoded = decode_object(encoded)
    validate_telemetry(decoded, "device-001")


def test_rejects_payload_topic_identity_mismatch() -> None:
    with pytest.raises(MessageValidationError, match="does not match"):
        validate_telemetry(telemetry(), "device-002")


@pytest.mark.parametrize("topic", ["iot/device-001/command", "bad/topic", "iot//status"])
def test_rejects_unsupported_observation_topic(topic: str) -> None:
    with pytest.raises(MessageValidationError):
        topic_identity(topic)


def test_command_duration_and_scenario_are_validated() -> None:
    command = {
        "schema_version": "1.0",
        "command": "set_scenario",
        "request_id": "request-1",
        "scenario": "message_burst",
        "duration_seconds": 30,
        "seed": 42,
    }
    validate_command(command)
    command["duration_seconds"] = 0
    with pytest.raises(MessageValidationError, match="duration_seconds"):
        validate_command(command)
