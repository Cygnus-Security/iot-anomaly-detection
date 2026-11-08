"""Consistent service logging setup."""

from __future__ import annotations

import logging
import os


def configure_logging() -> None:
    """Configure a compact UTC logger once per process."""

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.Formatter.converter = __import__("time").gmtime
    logging.basicConfig(
        level=level,
        format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
