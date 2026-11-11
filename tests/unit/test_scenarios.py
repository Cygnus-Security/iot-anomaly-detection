from __future__ import annotations

import random

import pytest

from src.simulator.scenarios import ScenarioRun, normal_interval


def make_run(name: str, seed: int = 42) -> ScenarioRun:
    return ScenarioRun(
        name=name,
        duration_seconds=30,
        seed=seed,
        request_id="c410f117-93c7-40ca-9147-e57fa4088e83",
    )


def test_burst_is_faster_than_normal_baseline() -> None:
    run = make_run("message_burst")
    assert run.next_interval(5.0, 0.5) == 0.2


def test_abnormal_interval_alternates_short_and_long() -> None:
    run = make_run("abnormal_interval")
    assert run.next_interval(5.0, 0.5) == 1.0
    assert run.next_interval(5.0, 0.5) == 9.0


def test_abnormal_payload_is_large_and_reproducible() -> None:
    first = make_run("abnormal_payload", seed=7).payload_extension()["diagnostic_blob"]
    second = make_run("abnormal_payload", seed=7).payload_extension()["diagnostic_blob"]
    assert len(first) == 1024
    assert first == second


def test_normal_jitter_is_seed_reproducible_and_bounded() -> None:
    first = normal_interval(random.Random(5), 5.0, 0.5)
    second = normal_interval(random.Random(5), 5.0, 0.5)
    assert first == second
    assert 4.5 <= first <= 5.5


def test_scenario_expiry_uses_monotonic_deadline() -> None:
    run = make_run("normal")
    assert not run.expired(run.started_monotonic + 29.9)
    assert run.remaining_seconds(run.started_monotonic + 29.9) == pytest.approx(0.1)
    assert run.expired(run.started_monotonic + 30.0)
    assert run.remaining_seconds(run.started_monotonic + 30.0) == 0.0
