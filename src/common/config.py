"""Environment-backed runtime settings with fail-fast validation."""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigurationError(ValueError):
    """Raised when required runtime configuration is missing or invalid."""


def _text(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or not value.strip():
        raise ConfigurationError(f"{name} is required")
    return value.strip()


def _secret(name: str) -> str:
    value = _text(name)
    if value.startswith("CHANGE_ME"):
        raise ConfigurationError(
            f"{name} still contains an example value; generate a local .env first"
        )
    return value


def _integer(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


def _number(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be numeric") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True, slots=True)
class MqttSettings:
    """Connection details for an authenticated MQTT client."""

    host: str
    port: int
    keepalive: int
    username: str
    password: str

    @classmethod
    def from_env(
        cls,
        *,
        username: str | None = None,
        password_variable: str = "MQTT_PASSWORD",
    ) -> "MqttSettings":
        return cls(
            host=_text("MQTT_HOST", "mosquitto"),
            port=_integer("MQTT_PORT", 1883, 1, 65535),
            keepalive=_integer("MQTT_KEEPALIVE", 60, 5, 3600),
            username=username or _text("MQTT_USERNAME"),
            password=_secret(password_variable),
        )


@dataclass(frozen=True, slots=True)
class SimulatorSettings:
    """Settings shared by all independently stateful virtual devices."""

    mqtt_host: str
    mqtt_port: int
    mqtt_keepalive: int
    device_password: str
    device_count: int
    base_interval_seconds: float
    interval_jitter_seconds: float
    heartbeat_seconds: float
    simulation_seed: int

    @classmethod
    def from_env(cls) -> "SimulatorSettings":
        return cls(
            mqtt_host=_text("MQTT_HOST", "mosquitto"),
            mqtt_port=_integer("MQTT_PORT", 1883, 1, 65535),
            mqtt_keepalive=_integer("MQTT_KEEPALIVE", 60, 5, 3600),
            device_password=_secret("DEVICE_PASSWORD"),
            device_count=_integer("DEVICE_COUNT", 12, 1, 999),
            base_interval_seconds=_number(
                "DEVICE_BASE_INTERVAL_SECONDS", 2.0, 0.05, 3600.0
            ),
            interval_jitter_seconds=_number(
                "DEVICE_INTERVAL_JITTER_SECONDS", 0.2, 0.0, 600.0
            ),
            heartbeat_seconds=_number(
                "DEVICE_HEARTBEAT_SECONDS", 30.0, 1.0, 3600.0
            ),
            simulation_seed=_integer(
                "SIMULATION_SEED", 20261108, -2_147_483_648, 2_147_483_647
            ),
        )


@dataclass(frozen=True, slots=True)
class CollectorSettings:
    """Collector MQTT and SQLite settings."""

    mqtt: MqttSettings
    database_path: str
    queue_size: int

    @classmethod
    def from_env(cls) -> "CollectorSettings":
        return cls(
            mqtt=MqttSettings.from_env(),
            database_path=_text(
                "SQLITE_PATH", "/app/data/runtime/iot_anomaly.db"
            ),
            queue_size=_integer("COLLECTOR_QUEUE_SIZE", 10_000, 100, 1_000_000),
        )
