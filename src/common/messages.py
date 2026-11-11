"""Versioned MQTT topic helpers and strict application-message validation."""

from __future__ import annotations

import json
import math
import re
import uuid
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = "1.0"
DEVICE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{2,31}$")
STATUS_STATES = {"online", "offline"}
EXPERIMENT_EVENTS = {"scenario_started", "scenario_ended"}
SCENARIOS = {
    "normal",
    "message_burst",
    "abnormal_interval",
    "abnormal_payload",
}
FORBIDDEN_TELEMETRY_KEYS = {
    "attack_type",
    "experiment_run_id",
    "ground_truth",
    "ground_truth_label",
    "is_anomaly",
    "label",
    "scenario",
    "scenario_label",
}
MAX_SQLITE_INTEGER = 2**63 - 1
MIN_SQLITE_INTEGER = -(2**63)
MAX_JSON_DEPTH = 64


class MessageValidationError(ValueError):
    """Raised when a message fails the versioned application contract."""


def _reject_non_finite_constant(constant: str) -> None:
    raise ValueError(f"non-finite number {constant}")


def utc_now() -> str:
    """Return an RFC 3339-compatible UTC timestamp with a `Z` suffix."""

    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def topic_identity(topic: str) -> tuple[str, str]:
    """Return `(device_id, message_type)` for a supported versioned topic."""

    parts = topic.split("/")
    if (
        len(parts) == 5
        and parts[:3] == ["iot", "v1", "devices"]
        and parts[4] in {"telemetry", "status"}
    ):
        device_id = parts[3]
        _validate_device_id(device_id)
        return device_id, parts[4]
    if (
        len(parts) == 5
        and parts[:3] == ["experiments", "v1", "devices"]
        and parts[4] == "events"
    ):
        device_id = parts[3]
        _validate_device_id(device_id)
        return device_id, "experiment"
    raise MessageValidationError(f"unsupported topic: {topic}")


def decode_object(payload: bytes) -> dict[str, Any]:
    """Decode a UTF-8 JSON object without accepting arrays or scalars."""

    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MessageValidationError("payload is not valid UTF-8") from exc
    try:
        value = json.loads(decoded, parse_constant=_reject_non_finite_constant)
    except RecursionError as exc:
        raise MessageValidationError(
            "payload JSON nesting exceeds the supported depth"
        ) from exc
    except (json.JSONDecodeError, ValueError) as exc:
        detail = exc.msg if isinstance(exc, json.JSONDecodeError) else str(exc)
        raise MessageValidationError(f"payload is not valid JSON: {detail}") from exc
    if not isinstance(value, dict):
        raise MessageValidationError("payload must be a JSON object")
    return value


def _required_text(message: dict[str, Any], key: str) -> str:
    value = message.get(key)
    if not isinstance(value, str) or not value:
        raise MessageValidationError(f"{key} must be a non-empty string")
    return value


def _validate_device_id(device_id: str) -> None:
    if not DEVICE_ID_PATTERN.fullmatch(device_id):
        raise MessageValidationError("device_id does not match the MQTT v1 contract")


def _validate_timestamp(message: dict[str, Any], key: str) -> str:
    timestamp = _required_text(message, key)
    if not timestamp.endswith("Z"):
        raise MessageValidationError(f"{key} must be an explicit UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MessageValidationError(f"{key} must be RFC 3339/ISO 8601") from exc
    if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise MessageValidationError(f"{key} must use UTC")
    return timestamp


def _validate_session_id(message: dict[str, Any]) -> str:
    session_id = _required_text(message, "session_id")
    try:
        uuid.UUID(session_id)
    except ValueError as exc:
        raise MessageValidationError("session_id must be a UUID") from exc
    return session_id


def _validate_uuid(message: dict[str, Any], key: str) -> str:
    value = _required_text(message, key)
    try:
        uuid.UUID(value)
    except ValueError as exc:
        raise MessageValidationError(f"{key} must be a UUID") from exc
    return value


def _validate_json_value(value: Any, path: str) -> None:
    stack: list[tuple[Any, str, int]] = [(value, path, 0)]
    while stack:
        item, item_path, depth = stack.pop()
        if depth > MAX_JSON_DEPTH:
            raise MessageValidationError(
                f"{item_path} exceeds the supported JSON depth"
            )
        if item is None or isinstance(item, (str, bool)):
            continue
        if isinstance(item, (int, float)):
            try:
                finite = math.isfinite(float(item))
            except (OverflowError, ValueError) as exc:
                raise MessageValidationError(
                    f"{item_path} contains a number outside the supported range"
                ) from exc
            if not finite:
                raise MessageValidationError(
                    f"{item_path} contains a non-finite number"
                )
            continue
        if isinstance(item, list):
            stack.extend(
                (child, f"{item_path}[{index}]", depth + 1)
                for index, child in enumerate(item)
            )
            continue
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise MessageValidationError(
                        f"{item_path} keys must be strings"
                    )
                stack.append((child, f"{item_path}.{key}", depth + 1))
            continue
        raise MessageValidationError(f"{item_path} contains a non-JSON value")


def _reject_telemetry_ground_truth(value: Any, path: str = "telemetry") -> None:
    """Keep experiment-only labels out of every telemetry object level."""

    stack: list[tuple[Any, str, int]] = [(value, path, 0)]
    while stack:
        item, item_path, depth = stack.pop()
        if depth > MAX_JSON_DEPTH:
            raise MessageValidationError(
                f"{item_path} exceeds the supported JSON depth"
            )
        if isinstance(item, dict):
            for key, child in item.items():
                if key in FORBIDDEN_TELEMETRY_KEYS:
                    raise MessageValidationError(
                        f"{item_path}.{key} is experiment ground truth and is forbidden"
                    )
                stack.append((child, f"{item_path}.{key}", depth + 1))
        elif isinstance(item, list):
            stack.extend(
                (child, f"{item_path}[{index}]", depth + 1)
                for index, child in enumerate(item)
            )


def _validate_common(message: dict[str, Any], expected_device_id: str) -> None:
    if message.get("schema_version") != SCHEMA_VERSION:
        raise MessageValidationError(f"schema_version must be {SCHEMA_VERSION!r}")
    device_id = _required_text(message, "device_id")
    _validate_device_id(device_id)
    if device_id != expected_device_id:
        raise MessageValidationError("payload device_id does not match topic")


def validate_telemetry(message: dict[str, Any], expected_device_id: str) -> None:
    """Validate telemetry metadata required for deduplication and windowing."""

    _reject_telemetry_ground_truth(message)
    _validate_common(message, expected_device_id)
    _validate_session_id(message)
    _validate_timestamp(message, "sent_at_utc")
    _required_text(message, "device_type")
    sequence = message.get("sequence_no")
    if (
        not isinstance(sequence, int)
        or isinstance(sequence, bool)
        or not 0 <= sequence <= MAX_SQLITE_INTEGER
    ):
        raise MessageValidationError(
            "sequence_no must be a non-negative signed 64-bit integer"
        )
    measurements = message.get("measurements")
    if not isinstance(measurements, dict) or not measurements:
        raise MessageValidationError("measurements must be a non-empty object")
    _validate_json_value(measurements, "measurements")
    if "message_id" in message:
        _validate_uuid(message, "message_id")
    if "firmware_version" in message:
        _required_text(message, "firmware_version")
    if "uptime_seconds" in message:
        uptime = message["uptime_seconds"]
        try:
            numeric_uptime = float(uptime)
        except (OverflowError, TypeError, ValueError):
            numeric_uptime = math.nan
        if not isinstance(uptime, (int, float)) or isinstance(uptime, bool) or not (
            math.isfinite(numeric_uptime) and numeric_uptime >= 0
        ):
            raise MessageValidationError("uptime_seconds must be finite and non-negative")


def validate_status(message: dict[str, Any], expected_device_id: str) -> None:
    """Validate retained online/offline status and Last Will payloads."""

    _validate_common(message, expected_device_id)
    _validate_session_id(message)
    _validate_timestamp(message, "sent_at_utc")
    state = _required_text(message, "state")
    if state not in STATUS_STATES:
        raise MessageValidationError(f"unsupported status state: {state}")


def validate_experiment_event(
    message: dict[str, Any], expected_device_id: str
) -> None:
    """Validate ground-truth events kept outside telemetry and model inputs."""

    _validate_common(message, expected_device_id)
    _validate_session_id(message)
    _validate_uuid(message, "event_id")
    _validate_timestamp(message, "event_at_utc")
    event = _required_text(message, "event")
    if event not in EXPERIMENT_EVENTS:
        raise MessageValidationError(f"unsupported experiment event: {event}")
    scenario = _required_text(message, "scenario")
    if scenario not in SCENARIOS:
        raise MessageValidationError(f"unsupported scenario: {scenario}")
    _validate_uuid(message, "experiment_run_id")
    _validate_uuid(message, "request_id")
    seed = message.get("seed")
    if (
        not isinstance(seed, int)
        or isinstance(seed, bool)
        or not MIN_SQLITE_INTEGER <= seed <= MAX_SQLITE_INTEGER
    ):
        raise MessageValidationError("seed must be a signed 64-bit integer")
    duration = message.get("duration_seconds")
    try:
        numeric_duration = float(duration)
    except (OverflowError, TypeError, ValueError):
        numeric_duration = math.nan
    if not isinstance(duration, (int, float)) or isinstance(duration, bool) or not (
        math.isfinite(numeric_duration) and 1 <= numeric_duration <= 3600
    ):
        raise MessageValidationError("duration_seconds must be between 1 and 3600")
    if not isinstance(message.get("details"), dict):
        raise MessageValidationError("details must be an object")
    _validate_json_value(message["details"], "details")


def validate_command(message: dict[str, Any]) -> None:
    """Validate a finite, reproducible lab scenario command."""

    if message.get("schema_version") != SCHEMA_VERSION:
        raise MessageValidationError(f"schema_version must be {SCHEMA_VERSION!r}")
    if message.get("command") != "set_scenario":
        raise MessageValidationError("command must be 'set_scenario'")
    _validate_uuid(message, "request_id")
    scenario = _required_text(message, "scenario")
    if scenario not in SCENARIOS:
        raise MessageValidationError(f"unsupported scenario: {scenario}")
    duration = message.get("duration_seconds")
    try:
        numeric_duration = float(duration)
    except (OverflowError, TypeError, ValueError):
        numeric_duration = math.nan
    if not isinstance(duration, (int, float)) or isinstance(duration, bool) or not (
        math.isfinite(numeric_duration) and 1 <= numeric_duration <= 3600
    ):
        raise MessageValidationError("duration_seconds must be between 1 and 3600")
    seed = message.get("seed")
    if (
        not isinstance(seed, int)
        or isinstance(seed, bool)
        or not MIN_SQLITE_INTEGER <= seed <= MAX_SQLITE_INTEGER
    ):
        raise MessageValidationError("seed must be a signed 64-bit integer")
