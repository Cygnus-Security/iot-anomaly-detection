"""MQTT subscriber service that feeds the single-writer SQLite store."""

from __future__ import annotations

import logging
import queue
import signal
import threading
import time
from typing import Any

import paho.mqtt.client as mqtt

from src.collector.storage import ObservedMessage, ObservationStore
from src.common.config import CollectorSettings, ConfigurationError
from src.common.logging import configure_logging

LOGGER = logging.getLogger(__name__)


class CollectorService:
    """Collect accepted MQTT messages without blocking the Paho network loop."""

    def __init__(self, settings: CollectorSettings) -> None:
        self.settings = settings
        self.stop_event = threading.Event()
        self.connected = threading.Event()
        self.messages: queue.Queue[ObservedMessage | None] = queue.Queue(
            maxsize=settings.queue_size
        )
        self.store = ObservationStore(settings.database_path)
        self.worker = threading.Thread(
            target=self._storage_worker, name="sqlite-writer", daemon=False
        )
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="collector-service",
            protocol=mqtt.MQTTv5,
        )
        self.client.username_pw_set(
            settings.mqtt.username, settings.mqtt.password
        )
        self.client.reconnect_delay_set(min_delay=1, max_delay=15)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def run(self) -> None:
        self.worker.start()
        self._connect_with_retry()
        if self.stop_event.is_set():
            self._shutdown_storage()
            return
        self.client.loop_start()
        LOGGER.info("collector started database=%s", self.settings.database_path)
        try:
            self.stop_event.wait()
        finally:
            self.client.disconnect()
            self.client.loop_stop()
            self._shutdown_storage()
            LOGGER.info("collector stopped")

    def stop(self) -> None:
        self.stop_event.set()

    def _connect_with_retry(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.client.connect(
                    self.settings.mqtt.host,
                    self.settings.mqtt.port,
                    self.settings.mqtt.keepalive,
                )
                return
            except OSError as exc:
                LOGGER.warning("MQTT connection failed: %s", exc)
                self.stop_event.wait(2.0)

    def _shutdown_storage(self) -> None:
        self.messages.join()
        self.messages.put(None)
        self.worker.join(timeout=10)
        self.store.close()

    def _storage_worker(self) -> None:
        while True:
            observed = self.messages.get()
            try:
                if observed is None:
                    return
                self.store.process(observed)
            except Exception:
                LOGGER.exception("failed to persist MQTT observation")
            finally:
                self.messages.task_done()

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
            LOGGER.error("broker rejected collector: %s", reason_code)
            return
        subscriptions = [
            ("iot/+/telemetry", 1),
            ("iot/+/status", 1),
        ]
        result, _ = client.subscribe(subscriptions)
        if result != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error("collector subscription failed rc=%s", result)
            return
        self.connected.set()
        LOGGER.info("collector connected and subscribed")

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, disconnect_flags, properties
        self.connected.clear()
        if reason_code != 0 and not self.stop_event.is_set():
            LOGGER.warning("unexpected collector disconnect: %s", reason_code)

    def _on_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        del client, userdata
        observed = ObservedMessage(
            topic=message.topic,
            payload=bytes(message.payload),
            qos=message.qos,
            retain=message.retain,
            duplicate=message.dup,
            received_at_unix=time.time(),
        )
        try:
            self.messages.put_nowait(observed)
        except queue.Full:
            LOGGER.error(
                "collector queue full; dropping topic=%s payload_bytes=%s",
                message.topic,
                len(message.payload),
            )


def main() -> int:
    configure_logging()
    try:
        settings = CollectorSettings.from_env()
        service = CollectorService(settings)
    except (ConfigurationError, OSError, RuntimeError) as exc:
        LOGGER.error("collector initialization failed: %s", exc)
        return 64

    def request_stop(signum: int, frame: object) -> None:
        del frame
        LOGGER.info("shutdown requested signal=%s", signum)
        service.stop()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    service.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
