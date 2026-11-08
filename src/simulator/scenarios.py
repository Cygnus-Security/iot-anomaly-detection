"""Communication behavior scenarios K1-K4."""

from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from src.common.messages import SCENARIOS


@dataclass(slots=True)
class ScenarioRun:
    """A finite, reproducible scenario active on one device."""

    name: str
    duration_seconds: float
    seed: int
    request_id: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_monotonic: float = field(default_factory=time.monotonic)
    _rng: random.Random = field(init=False, repr=False)
    _interval_index: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.name not in SCENARIOS:
            raise ValueError(f"unknown scenario: {self.name}")
        if not 1 <= self.duration_seconds <= 3600:
            raise ValueError("duration_seconds must be between 1 and 3600")
        self._rng = random.Random(self.seed)

    def expired(self, now: float | None = None) -> bool:
        """Return true after the configured monotonic duration."""

        current = time.monotonic() if now is None else now
        return current >= self.started_monotonic + self.duration_seconds

    def remaining_seconds(self, now: float | None = None) -> float:
        """Return time left so a long publish delay cannot overrun the run."""

        current = time.monotonic() if now is None else now
        return max(
            0.0,
            self.started_monotonic + self.duration_seconds - current,
        )

    def next_interval(
        self, base_seconds: float, jitter_seconds: float
    ) -> float:
        """Return the next publish delay for this communication behavior."""

        if self.name == "message_burst":
            center = max(0.02, base_seconds / 12.0)
            return max(0.02, center + self._rng.uniform(-0.01, 0.01))
        if self.name == "abnormal_interval":
            self._interval_index += 1
            if self._interval_index % 2:
                return max(0.03, base_seconds * 0.05)
            return max(0.1, base_seconds * 4.0)
        return max(
            0.05,
            base_seconds + self._rng.uniform(-jitter_seconds, jitter_seconds),
        )

    def payload_extension(self) -> dict[str, Any]:
        """Return scenario-specific payload fields without changing semantics."""

        if self.name == "abnormal_payload":
            alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            padding = "".join(self._rng.choice(alphabet) for _ in range(2048))
            return {"padding": padding}
        return {}

    def details(self) -> dict[str, Any]:
        """Describe the applied behavior for ground-truth records."""

        if self.name == "message_burst":
            return {"burst_multiplier": 12.0}
        if self.name == "abnormal_interval":
            return {"short_interval_factor": 0.05, "long_interval_factor": 4.0}
        if self.name == "abnormal_payload":
            return {"padding_bytes": 2048}
        return {"behavior": "configured normal communication"}


def normal_interval(
    rng: random.Random, base_seconds: float, jitter_seconds: float
) -> float:
    """Return a bounded delay for ordinary operation outside an experiment."""

    return max(0.05, base_seconds + rng.uniform(-jitter_seconds, jitter_seconds))
