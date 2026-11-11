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
        self._publish_wakeup = threading.Event()
        self._scenario_lock = threading.RLock()
        self._active_scenario: ScenarioRun | None = None
        self._scenario_acknowledgements: dict[str, dict[str, Any]] = {}
        self._session_id = str(uuid.uuid4())
        self._sequence = 0
        self._started_monotonic = time.monotonic()
        self._next_heartbeat = self._started_monotonic + settings.heartbeat_seconds

        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=device_id,
            protocol=mqtt.MQTTv311,
        )
        self._client.username_pw_set(device_id, settings.device_password)
        self._client.reconnect_delay_set(min_delay=1, max_delay=15)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.will_set(
            self.status_topic,
            payload=self._encode_status("offline"),
            qos=1,
            retain=True,
        )

    @property
    def telemetry_topic(self) -> str:
        return f"iot/v1/devices/{self.device_id}/telemetry"

    @property
    def status_topic(self) -> str:
        return f"iot/v1/devices/{self.device_id}/status"

    @property
    def command_topic(self) -> str:
        return f"iot/v1/devices/{self.device_id}/command"

    @property
    def experiment_topic(self) -> str:
        return f"experiments/v1/devices/{self.device_id}/events"

    def run(self) -> None:
        """Connect, publish telemetry until stopped, and disconnect cleanly."""

        if not self._connect_with_retry():
            return
        try:
            initial_delay = self._rng.uniform(
                0.0, self.settings.initial_spread_seconds
            )
            if self._stop_event.wait(initial_delay):
                return
            while not self._stop_event.is_set():
                delay = self._publish_cycle()
                deadline = time.monotonic() + delay
                while not self._stop_event.is_set():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    if self._publish_wakeup.wait(min(remaining, 0.5)):
                        self._publish_wakeup.clear()
                        break
        finally:
            with self._scenario_lock:
                if self._active_scenario is not None:
                    self._finish_scenario(
                        self._active_scenario,
                        reason="simulator_shutdown",
                        wait_for_publish=True,
                    )
            if self._connected.is_set():
                self._publish_status("offline", wait_for_publish=True)
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
                LOGGER.warning(
                    "MQTT connection/subscription readiness timeout device_id=%s",
                    self.device_id,
                )
                self._client.disconnect()
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
                # Refresh retained availability while keeping the status schema
                # limited to the documented online/offline states.
                self._publish_status("online")
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
        ordinal = int(self.device_id.rsplit("-", maxsplit=1)[-1])
        measurements: dict[str, Any] = {
            "temperature_c": round(22.0 + ordinal * 0.1 + self._rng.gauss(0, 0.3), 3),
            "relative_humidity_pct": round(50.0 + self._rng.gauss(0, 1.0), 3),
        }
        if scenario is not None:
            measurements.update(scenario.payload_extension())
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "message_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "device_type": "environment_sensor",
            "session_id": self._session_id,
            "sequence_no": self._sequence,
            "sent_at_utc": utc_now(),
            "measurements": measurements,
            "firmware_version": "sim-1.0",
            "uptime_seconds": round(time.monotonic() - self._started_monotonic, 3),
        }
        self._publish(self.telemetry_topic, payload)
        self._sequence += 1

    def _encode_status(
        self,
        state: str,
    ) -> str:
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "device_id": self.device_id,
            "session_id": self._session_id,
            "state": state,
            "sent_at_utc": utc_now(),
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)

    def _publish_status(
        self,
        state: str,
        *,
        wait_for_publish: bool = False,
    ) -> None:
        payload = self._encode_status(state)
        result = self._client.publish(self.status_topic, payload, qos=1, retain=True)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error(
                "status publish failed device_id=%s state=%s rc=%s",
                self.device_id,
                state,
                result.rc,
            )
        elif wait_for_publish:
            try:
                result.wait_for_publish(timeout=2.0)
            except RuntimeError as exc:
                LOGGER.warning(
                    "status acknowledgement failed device_id=%s state=%s error=%s",
                    self.device_id,
                    state,
                    exc,
                )
            if not result.is_published():
                LOGGER.warning(
                    "status acknowledgement timed out device_id=%s state=%s",
                    self.device_id,
                    state,
                )

    def _publish_experiment_event(
        self,
        event: str,
        scenario: ScenarioRun,
        details: dict[str, Any],
        *,
        wait_for_publish: bool = False,
    ) -> dict[str, Any]:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "event_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "session_id": self._session_id,
            "event_at_utc": utc_now(),
            "event": event,
            "scenario": scenario.name,
            "experiment_run_id": scenario.run_id,
            "request_id": scenario.request_id,
            "seed": scenario.seed,
            "duration_seconds": scenario.duration_seconds,
            "details": details,
        }
        self._publish(
            self.experiment_topic, payload, wait_for_publish=wait_for_publish
        )
        return payload

    def _publish(
        self,
        topic: str,
        payload: dict[str, Any],
        *,
        wait_for_publish: bool = False,
    ) -> None:
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        result = self._client.publish(topic, encoded, qos=1, retain=False)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error(
                "publish failed device_id=%s topic=%s rc=%s",
                self.device_id,
                topic,
                result.rc,
            )
        elif wait_for_publish:
            try:
                result.wait_for_publish(timeout=2.0)
            except RuntimeError as exc:
                LOGGER.warning(
                    "publish acknowledgement failed device_id=%s topic=%s error=%s",
                    self.device_id,
                    topic,
                    exc,
                )
            if not result.is_published():
                LOGGER.warning(
                    "publish acknowledgement timed out device_id=%s topic=%s",
                    self.device_id,
                    topic,
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
        self._connected.clear()
        result, _ = client.subscribe(self.command_topic, qos=1)
        if result != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error(
                "command subscribe failed device_id=%s rc=%s", self.device_id, result
            )
            return

    def _on_subscribe(
        self,
        client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[mqtt.ReasonCode],
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, mid, properties
        if reason_code_list and any(code >= 128 for code in reason_code_list):
            LOGGER.error(
                "command subscription rejected device_id=%s reasons=%s",
                self.device_id,
                reason_code_list,
            )
            return
        self._connected.set()
        self._publish_status("online")
        LOGGER.info("device connected and subscribed device_id=%s", self.device_id)

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

        request_id = str(command["request_id"])
        with self._scenario_lock:
            previous_acknowledgement = self._scenario_acknowledgements.get(request_id)
            if previous_acknowledgement is not None:
                self._publish(self.experiment_topic, previous_acknowledgement)
                LOGGER.info(
                    "duplicate scenario command acknowledged device_id=%s request_id=%s",
                    self.device_id,
                    request_id,
                )
                return

        scenario = ScenarioRun(
            name=str(command["scenario"]),
            duration_seconds=float(command["duration_seconds"]),
            seed=int(command["seed"]),
            request_id=request_id,
        )
        with self._scenario_lock:
            current = self._active_scenario
            if current is not None:
                self._finish_scenario(current, reason="replaced_by_command")
            self._active_scenario = scenario
            acknowledgement = self._publish_experiment_event(
                "scenario_started", scenario, scenario.details()
            )
            self._scenario_acknowledgements[request_id] = acknowledgement
            self._publish_wakeup.set()
        LOGGER.info(
            "scenario started device_id=%s scenario=%s run_id=%s seed=%s",
            self.device_id,
            scenario.name,
            scenario.run_id,
            scenario.seed,
        )

    def _finish_scenario(
        self,
        scenario: ScenarioRun,
        *,
        reason: str,
        wait_for_publish: bool = False,
    ) -> None:
        self._publish_experiment_event(
            "scenario_ended",
            scenario,
            {"reason": reason},
            wait_for_publish=wait_for_publish,
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
