from __future__ import annotations

import random

import pytest

from src.simulator.scenarios import ScenarioRun, normal_interval


def make_run(name: str, seed: int = 42) -> ScenarioRun:
    return ScenarioRun(
        name=name,
        duration_seconds=30,
        seed=seed,
        request_id="request-1",
    )


def test_burst_is_faster_than_normal_baseline() -> None:
    run = make_run("message_burst")
    assert 0.02 <= run.next_interval(2.0, 0.2) < 0.25


def test_abnormal_interval_alternates_short_and_long() -> None:
    run = make_run("abnormal_interval")
    assert run.next_interval(2.0, 0.2) == 0.1
    assert run.next_interval(2.0, 0.2) == 8.0


def test_abnormal_payload_is_large_and_reproducible() -> None:
    first = make_run("abnormal_payload", seed=7).payload_extension()["padding"]
    second = make_run("abnormal_payload", seed=7).payload_extension()["padding"]
    assert len(first) == 2048
    assert first == second


def test_normal_jitter_is_seed_reproducible_and_bounded() -> None:
    first = normal_interval(random.Random(5), 2.0, 0.2)
    second = normal_interval(random.Random(5), 2.0, 0.2)
    assert first == second
    assert 1.8 <= first <= 2.2


def test_scenario_expiry_uses_monotonic_deadline() -> None:
    run = make_run("normal")
    assert not run.expired(run.started_monotonic + 29.9)
    assert run.remaining_seconds(run.started_monotonic + 29.9) == pytest.approx(0.1)
    assert run.expired(run.started_monotonic + 30.0)
    assert run.remaining_seconds(run.started_monotonic + 30.0) == 0.0
