"""Authenticated CLI for manually starting repeatable K1-K4 scenarios."""

from __future__ import annotations

import argparse
import json
import logging
import threading
import uuid
from typing import Any, Sequence

import paho.mqtt.client as mqtt

from src.common.config import ConfigurationError, MqttSettings
from src.common.logging import configure_logging
from src.common.messages import SCENARIOS, SCHEMA_VERSION, decode_object

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Start a finite communication scenario on one virtual device."
    )
    parser.add_argument("--device-id", required=True, help="e.g. iot-001")
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    parser.add_argument("--duration-seconds", type=float, default=30.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--ack-timeout-seconds", type=float, default=10.0)
    return parser


def run_command(args: argparse.Namespace, settings: MqttSettings) -> dict[str, Any]:
    if not 1 <= args.duration_seconds <= 3600:
        raise ValueError("--duration-seconds must be between 1 and 3600")
    if args.ack_timeout_seconds <= 0:
        raise ValueError("--ack-timeout-seconds must be positive")

    request_id = str(uuid.uuid4())
    connected = threading.Event()
    subscribed = threading.Event()
    acknowledged = threading.Event()
    acknowledgement: dict[str, Any] = {}
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"operator-{uuid.uuid4()}",
        protocol=mqtt.MQTTv311,
    )
    client.username_pw_set(settings.username, settings.password)

    def on_connect(
        connected_client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        if reason_code != 0:
            LOGGER.error("operator connection rejected: %s", reason_code)
            return
        connected.set()
        connected_client.subscribe(
            f"experiments/v1/devices/{args.device_id}/events", qos=1
        )

    def on_subscribe(
        subscribed_client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[mqtt.ReasonCode],
        properties: mqtt.Properties | None,
    ) -> None:
        del subscribed_client, userdata, mid, properties
        if reason_code_list and all(code < 128 for code in reason_code_list):
            subscribed.set()

    def on_message(
        message_client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        del message_client, userdata
        try:
            payload = decode_object(message.payload)
        except ValueError:
            return
        if (
            payload.get("event") == "scenario_started"
            and payload.get("request_id") == request_id
        ):
            acknowledgement.update(payload)
            acknowledged.set()

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.connect(settings.host, settings.port, settings.keepalive)
    client.loop_start()
    try:
        if not connected.wait(timeout=args.ack_timeout_seconds):
            raise TimeoutError("broker connection acknowledgement timed out")
        if not subscribed.wait(timeout=args.ack_timeout_seconds):
            raise TimeoutError("experiment-event subscription timed out")
        command = {
            "schema_version": SCHEMA_VERSION,
            "command": "set_scenario",
            "request_id": request_id,
            "scenario": args.scenario,
            "duration_seconds": args.duration_seconds,
            "seed": args.seed,
        }
        info = client.publish(
            f"iot/v1/devices/{args.device_id}/command",
            json.dumps(command, separators=(",", ":"), sort_keys=True),
            qos=1,
            retain=False,
        )
        info.wait_for_publish(timeout=args.ack_timeout_seconds)
        if not info.is_published():
            raise TimeoutError("command publish acknowledgement timed out")
        if not acknowledged.wait(timeout=args.ack_timeout_seconds):
            raise TimeoutError(
                "device did not acknowledge scenario; verify device ID and simulator"
            )
        return acknowledgement
    finally:
        client.disconnect()
        client.loop_stop()


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    try:
        settings = MqttSettings.from_env()
        acknowledgement = run_command(args, settings)
    except (ConfigurationError, OSError, TimeoutError, ValueError) as exc:
        LOGGER.error("scenario command failed: %s", exc)
        return 2
    print(json.dumps(acknowledgement, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
