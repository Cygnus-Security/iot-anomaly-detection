"""One independently stateful virtual IoT device and MQTT session."""

from __future__ import annotations

import json
import logging
import random
import threading
import time
import uuid
from typing import Any

import paho.mqtt.client as mqtt

from src.common.config import SimulatorSettings
from src.common.messages import (
    MessageValidationError,
    SCHEMA_VERSION,
    decode_object,
    utc_now,
    validate_command,
)
from src.simulator.scenarios import ScenarioRun, normal_interval

LOGGER = logging.getLogger(__name__)


class VirtualDevice:
    """A virtual sensor with its own identity, state, RNG, and MQTT client."""

    def __init__(
        self,
        device_id: str,
        settings: SimulatorSettings,
        seed: int,
        stop_event: threading.Event,
    ) -> None:
        self.device_id = device_id
        self.settings = settings
        self._rng = random.Random(seed)
        self._stop_event = stop_event
        self._connected = threading.Event()
        self._scenario_lock = threading.RLock()
        self._active_scenario: ScenarioRun | None = None
        self._sequence = 0
        self._started_monotonic = time.monotonic()
        self._next_heartbeat = self._started_monotonic + settings.heartbeat_seconds

        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=device_id,
            protocol=mqtt.MQTTv5,
        )
        self._client.username_pw_set(device_id, settings.device_password)
        self._client.reconnect_delay_set(min_delay=1, max_delay=15)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.will_set(
            self.status_topic,
            payload=self._encode_status("offline", details={"reason": "last_will"}),
            qos=1,
            retain=False,
        )

    @property
    def telemetry_topic(self) -> str:
        return f"iot/{self.device_id}/telemetry"

    @property
    def status_topic(self) -> str:
        return f"iot/{self.device_id}/status"

    @property
    def command_topic(self) -> str:
        return f"iot/{self.device_id}/command"

    def run(self) -> None:
        """Connect, publish telemetry until stopped, and disconnect cleanly."""

        if not self._connect_with_retry():
            return
        try:
            while not self._stop_event.is_set():
                delay = self._publish_cycle()
                self._stop_event.wait(delay)
        finally:
            if self._connected.is_set():
                self._publish_status("offline", details={"reason": "clean_shutdown"})
                time.sleep(0.05)
            self._client.disconnect()
            self._client.loop_stop()
            LOGGER.info("device stopped device_id=%s", self.device_id)

    def _connect_with_retry(self) -> bool:
        while not self._stop_event.is_set():
            try:
                self._client.connect(
                    self.settings.mqtt_host,
                    self.settings.mqtt_port,
                    self.settings.mqtt_keepalive,
                )
                self._client.loop_start()
                if self._connected.wait(timeout=10):
                    return True
                LOGGER.warning("CONNACK timeout device_id=%s", self.device_id)
                self._client.loop_stop()
            except OSError as exc:
                LOGGER.warning(
                    "MQTT connection failed device_id=%s error=%s",
                    self.device_id,
                    exc,
                )
            self._stop_event.wait(2.0)
        return False

    def _publish_cycle(self) -> float:
        now = time.monotonic()
        with self._scenario_lock:
            scenario = self._active_scenario
            if scenario is not None and scenario.expired(now):
                self._finish_scenario(scenario, reason="duration_elapsed")
                scenario = None

            self._publish_telemetry(scenario)
            if now >= self._next_heartbeat:
                self._publish_status("heartbeat")
                self._next_heartbeat = now + self.settings.heartbeat_seconds

            if scenario is None:
                return normal_interval(
                    self._rng,
                    self.settings.base_interval_seconds,
                    self.settings.interval_jitter_seconds,
                )
            return min(
                scenario.next_interval(
                    self.settings.base_interval_seconds,
                    self.settings.interval_jitter_seconds,
                ),
                scenario.remaining_seconds(),
            )

    def _publish_telemetry(self, scenario: ScenarioRun | None) -> None:
        self._sequence += 1
        ordinal = int(self.device_id.rsplit("-", maxsplit=1)[-1])
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "message_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "timestamp": utc_now(),
            "sequence": self._sequence,
            "sensor": {
                "temperature_c": round(22.0 + ordinal * 0.1 + self._rng.gauss(0, 0.3), 3),
                "humidity_percent": round(50.0 + self._rng.gauss(0, 1.0), 3),
            },
            "device": {
                "firmware": "sim-1.0",
                "uptime_seconds": round(time.monotonic() - self._started_monotonic, 3),
            },
        }
        if scenario is not None:
            payload.update(scenario.payload_extension())
        self._publish(self.telemetry_topic, payload)

    def _encode_status(
        self,
        event: str,
        *,
        scenario: ScenarioRun | None = None,
        details: dict[str, Any] | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "message_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "timestamp": utc_now(),
            "event": event,
            "details": details or {},
        }
        if scenario is not None:
            payload.update(
                {
                    "scenario": scenario.name,
                    "scenario_run_id": scenario.run_id,
                    "request_id": scenario.request_id,
                    "seed": scenario.seed,
                    "duration_seconds": scenario.duration_seconds,
                }
            )
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)

    def _publish_status(
        self,
        event: str,
        *,
        scenario: ScenarioRun | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        payload = self._encode_status(event, scenario=scenario, details=details)
        result = self._client.publish(self.status_topic, payload, qos=1, retain=False)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error(
                "status publish failed device_id=%s event=%s rc=%s",
                self.device_id,
                event,
                result.rc,
            )

    def _publish(self, topic: str, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        result = self._client.publish(topic, encoded, qos=1, retain=False)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error(
                "publish failed device_id=%s topic=%s rc=%s",
                self.device_id,
                topic,
                result.rc,
            )

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        if reason_code != 0:
            LOGGER.error(
                "broker rejected device_id=%s reason=%s", self.device_id, reason_code
            )
            return
        result, _ = client.subscribe(self.command_topic, qos=1)
        if result != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error(
                "command subscribe failed device_id=%s rc=%s", self.device_id, result
            )
            return
        self._connected.set()
        self._publish_status("online")
        LOGGER.info("device connected device_id=%s", self.device_id)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, disconnect_flags, properties
        self._connected.clear()
        if reason_code != 0 and not self._stop_event.is_set():
            LOGGER.warning(
                "unexpected disconnect device_id=%s reason=%s",
                self.device_id,
                reason_code,
            )

    def _on_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        del client, userdata
        try:
            command = decode_object(message.payload)
            validate_command(command)
        except MessageValidationError as exc:
            LOGGER.warning(
                "invalid command device_id=%s error=%s", self.device_id, exc
            )
            return

        scenario = ScenarioRun(
            name=str(command["scenario"]),
            duration_seconds=float(command["duration_seconds"]),
            seed=int(command["seed"]),
            request_id=str(command["request_id"]),
        )
        with self._scenario_lock:
            current = self._active_scenario
            if current is not None:
                self._finish_scenario(current, reason="replaced_by_command")
            self._active_scenario = scenario
            self._publish_status(
                "scenario_started", scenario=scenario, details=scenario.details()
            )
        LOGGER.info(
            "scenario started device_id=%s scenario=%s run_id=%s seed=%s",
            self.device_id,
            scenario.name,
            scenario.run_id,
            scenario.seed,
        )

    def _finish_scenario(self, scenario: ScenarioRun, *, reason: str) -> None:
        self._publish_status(
            "scenario_ended", scenario=scenario, details={"reason": reason}
        )
        if self._active_scenario is scenario:
            self._active_scenario = None
        LOGGER.info(
            "scenario ended device_id=%s scenario=%s run_id=%s reason=%s",
            self.device_id,
            scenario.name,
            scenario.run_id,
            reason,
        )
