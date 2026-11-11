"""Process entry point for a fleet of independent virtual MQTT devices."""

from __future__ import annotations

import logging
import signal
import threading

from src.common.config import ConfigurationError, SimulatorSettings
from src.common.logging import configure_logging
from src.simulator.device import VirtualDevice

LOGGER = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    try:
        settings = SimulatorSettings.from_env()
    except ConfigurationError as exc:
        LOGGER.error("invalid configuration: %s", exc)
        return 64

    stop_event = threading.Event()

    def request_stop(signum: int, frame: object) -> None:
        del frame
        LOGGER.info("shutdown requested signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    threads: list[threading.Thread] = []
    for index in range(1, settings.device_count + 1):
        device_id = f"iot-{index:03d}"
        device = VirtualDevice(
            device_id,
            settings,
            seed=settings.simulation_seed + index,
            stop_event=stop_event,
        )
        thread = threading.Thread(
            target=device.run,
            name=f"virtual-{device_id}",
            daemon=False,
        )
        thread.start()
        threads.append(thread)

    LOGGER.info("simulator started device_count=%s", settings.device_count)
    for thread in threads:
        thread.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
