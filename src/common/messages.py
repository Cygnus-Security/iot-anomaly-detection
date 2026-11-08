"""Versioned MQTT message helpers and strict lightweight validation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = "1.0"
STATUS_EVENTS = {
    "online",
    "offline",
    "heartbeat",
    "scenario_started",
    "scenario_ended",
}
SCENARIOS = {
    "normal",
    "message_burst",
    "abnormal_interval",
    "abnormal_payload",
}


class MessageValidationError(ValueError):
    """Raised when a message fails the versioned application contract."""


def utc_now() -> str:
    """Return an ISO 8601 UTC timestamp with a `Z` suffix."""

    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def topic_identity(topic: str) -> tuple[str, str]:
    """Return `(device_id, message_type)` for a supported IoT topic."""

    parts = topic.split("/")
    if len(parts) != 3 or parts[0] != "iot" or parts[2] not in {
        "telemetry",
        "status",
    }:
        raise MessageValidationError(f"unsupported topic: {topic}")
    if not parts[1]:
        raise MessageValidationError("topic device_id is empty")
    return parts[1], parts[2]


def decode_object(payload: bytes) -> dict[str, Any]:
    """Decode a UTF-8 JSON object without accepting arrays or scalars."""

    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MessageValidationError("payload is not valid UTF-8") from exc
    try:
        value = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise MessageValidationError(f"payload is not valid JSON: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise MessageValidationError("payload must be a JSON object")
    return value


def _required_text(message: dict[str, Any], key: str) -> str:
    value = message.get(key)
    if not isinstance(value, str) or not value:
        raise MessageValidationError(f"{key} must be a non-empty string")
    return value


def _validate_common(message: dict[str, Any], expected_device_id: str) -> None:
    if message.get("schema_version") != SCHEMA_VERSION:
        raise MessageValidationError(
            f"schema_version must be {SCHEMA_VERSION!r}"
        )
    if _required_text(message, "device_id") != expected_device_id:
        raise MessageValidationError("payload device_id does not match topic")
    _required_text(message, "message_id")
    timestamp = _required_text(message, "timestamp")
    try:
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MessageValidationError("timestamp must be ISO 8601") from exc


def validate_telemetry(
    message: dict[str, Any], expected_device_id: str
) -> None:
    """Validate fields needed by collection and later feature extraction."""

    _validate_common(message, expected_device_id)
    sequence = message.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise MessageValidationError("sequence must be a non-negative integer")
    sensor = message.get("sensor")
    if not isinstance(sensor, dict):
        raise MessageValidationError("sensor must be an object")
    for key in ("temperature_c", "humidity_percent"):
        value = sensor.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise MessageValidationError(f"sensor.{key} must be numeric")
    device = message.get("device")
    if not isinstance(device, dict):
        raise MessageValidationError("device must be an object")


def validate_status(message: dict[str, Any], expected_device_id: str) -> None:
    """Validate a device status or scenario event."""

    _validate_common(message, expected_device_id)
    event = _required_text(message, "event")
    if event not in STATUS_EVENTS:
        raise MessageValidationError(f"unsupported status event: {event}")
    if event.startswith("scenario_"):
        scenario = _required_text(message, "scenario")
        if scenario not in SCENARIOS:
            raise MessageValidationError(f"unsupported scenario: {scenario}")
        _required_text(message, "scenario_run_id")
        _required_text(message, "request_id")


def validate_command(message: dict[str, Any]) -> None:
    """Validate a scenario command before a device changes behavior."""

    if message.get("schema_version") != SCHEMA_VERSION:
        raise MessageValidationError(
            f"schema_version must be {SCHEMA_VERSION!r}"
        )
    if message.get("command") != "set_scenario":
        raise MessageValidationError("command must be 'set_scenario'")
    _required_text(message, "request_id")
    scenario = _required_text(message, "scenario")
    if scenario not in SCENARIOS:
        raise MessageValidationError(f"unsupported scenario: {scenario}")
    duration = message.get("duration_seconds")
    if (
        not isinstance(duration, (int, float))
        or isinstance(duration, bool)
        or not 1 <= float(duration) <= 3600
    ):
        raise MessageValidationError("duration_seconds must be between 1 and 3600")
    seed = message.get("seed")
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise MessageValidationError("seed must be an integer")
