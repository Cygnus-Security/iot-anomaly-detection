# Chapter IV Experiment Runbook

**Status:** executable workflow; measured results are generated only by live runs

**Updated:** 2026-11-11

## 1. Evidence rule

This runbook distinguishes implemented source, experimental ground truth, model
output, and measured evaluation results:

- implemented code is not evidence that a live test passed;
- K2-K4 phase events describe the behavior that the controller applied, not an
  ML detection;
- every model decision is stored, including normal decisions;
- alerts are the subset whose `decision_function` score is strictly below the
  validation-calibrated threshold;
- metrics and figures are generated from completed held-out databases, never
  entered manually or inferred from run names.

All campaign evidence belongs under one ignored directory, `results/`. The live
campaign database remains in the Docker-managed `experiment_data` volume at
`/app/experiment-data/run.db`; the host must never open or copy it while a
writer is active. A per-run `results/` database is created only after writer
shutdown, checkpoint, and a successful integrity gate.

## 2. Frozen campaign

The canonical inputs are:

- `experiments/run_plan.csv` - 13 ordered runs, unique seeds, durations, target
  devices, and planned phases;
- `experiments/split_manifest.json` - two train runs, one validation run, and ten
  held-out test runs.

The plan contains 2,820 seconds of measured run time:

| Role | Runs | Duration per run | Purpose |
|---|---:|---:|---|
| Train normal | 2 | 300 s | Fit the Isolation Forest on normal communication only |
| Validation normal | 1 | 300 s | Freeze the first-percentile score threshold |
| Test normal (K1) | 1 | 300 s | Measure normal-test false positives and coverage |
| Test K2 | 3 | 180 s | 60 s baseline, 60 s at 5 messages/s, 60 s recovery |
| Test K3 | 3 | 180 s | 60 s baseline, 60 s alternating 1/9-second gaps, 60 s recovery |
| Test K4 | 3 | 180 s | 60 s baseline, 60 s with 1,024 added ASCII bytes, 60 s recovery |

No test run may begin until the model and threshold are frozen from train and
validation data. Test observations must never trigger refitting.

## 3. Preflight

```bash
./scripts/bootstrap_env.sh
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
docker compose --profile experiment --profile tools config --quiet
.venv/bin/python scripts/run_campaign.py --dry-run
.venv/bin/python -m src.experiments.prepare --results-dir results
```

Run the prepare command once for a new campaign; it intentionally refuses a
non-empty result directory. Confirm that `.env` contains non-empty lab
credentials. Do not print or copy those values into evidence. The initializer
records a redacted effective configuration, host/runtime versions, source
commit, and dirty-worktree flag.

Functional checks T01-T16 are separate from the ML split. Their data must not be
used for training, validation, or test metrics. The functional evidence CLI
marks a check `not_run` when its required live artifact is absent.

### 3.1. Functional evidence

Start the real broker and export the local `.env` values only into the current
shell before executing broker nodes:

```bash
docker compose up --build -d mosquitto
set -a
source .env
set +a
export MQTT_HOST=127.0.0.1 MQTT_PORT="${MQTT_HOST_PORT:-1883}"
.venv/bin/python scripts/run_functional_tests.py \
  --results-dir results \
  --include-integration \
  --live-evidence results/evidence/live-functional.json
```

The optional live-evidence file is keyed by T01-T16. Each supplied entry names
a real file below `results/`:

```json
{
  "T01": {
    "status": "pass",
    "observed_result": "Required services were healthy and logs contained no secret.",
    "evidence_path": "evidence/t01-compose-health.txt"
  }
}
```

The CLI rejects external paths, redacts credential values from captured test
logs, and never lets a claimed live pass override a failed automated check.

## 4. Run the campaign

```bash
.venv/bin/python scripts/run_campaign.py \
  --build \
  --trainer-command ".venv/bin/python scripts/train_model.py --results-dir results"
```

For each entry, the runner:

1. uses a new `collection_session_id` and clean per-run SQLite database in the
   Docker-managed `experiment_data` volume;
2. starts the authenticated broker and collector;
3. anchors the real UTC run interval before the simulator's deterministic
   0-5-second initial spread;
4. starts analytics with the same run anchor and waits for readiness;
5. verifies valid telemetry from all 12 devices from inside the Linux container
   environment and monitors Analytics liveness throughout the run;
6. applies K2-K4 at the planned monotonic deadline and later reads the actual
   start/end events from SQLite;
7. samples Docker resource use at one-second intervals;
8. waits for Analytics to complete, stops all SQLite writers, performs a
   container-side WAL checkpoint and `integrity_check`, and rejects any result
   other than `ok`;
9. copies the verified database and collector-state sidecar into the run's
   `results/` directory, checks scheduled-window completeness, and writes the
   manifests and complete profile-aware logs.

Completed run evidence is immutable. If an attempt is running, failed, or
interrupted, the runner does not silently reuse its database. Retry explicitly:

```bash
.venv/bin/python scripts/run_campaign.py \
  --build \
  --retry-failed \
  --trainer-command ".venv/bin/python scripts/train_model.py --results-dir results"
```

The previous attempt is moved under the run's `attempts/` directory before the
new attempt starts. Its manifest, source version, collection session, runtime
logs, database/WAL files, and integrity report remain linked from attempt
history. Every retry receives a new collection session. Corrupt or incomplete
attempts are archive-only evidence and are never eligible for training,
inference, or evaluation.

## 5. Model freeze and trust boundary

The trainer reads usable normal `feature_windows` directly from the two train
databases and the validation database. It fits exactly seven ordered features
with an Isolation Forest configured as follows:

- `n_estimators=100`
- `max_samples="auto"`
- `max_features=1.0`
- `contamination="auto"`
- `bootstrap=false`
- `random_state=42`

It freezes the anomaly rule as `decision_function < validation 1% quantile` and
writes `results/artifacts/model.joblib`, `model_metadata.json`, feature exports,
the split manifest, and an artifact manifest with checksums and fit/save timing.

Joblib can execute code during loading. The analytics service loads only the
explicit campaign artifact with `trusted=True`, after verifying its SHA-256,
feature names/order, feature schema, window length, stride, minimum-message
rule, threshold direction, and metadata.

## 6. Export and evaluation

After every planned run is completed:

```bash
.venv/bin/python scripts/export_run.py --results-dir results
.venv/bin/python scripts/evaluate_results.py --results-dir results
```

For T10, replay a completed test run from its raw rows and actual phase events,
reload the frozen artifact, and compare every offline feature/score with the
persisted online records:

```bash
.venv/bin/python scripts/verify_run_replay.py \
  --results-dir results \
  --run-id test-k2-01
```

The comparison is written to
`results/evidence/t10-replay-test-k2-01.json`; a mismatch is failed evidence,
not a value to hide or round away.

The evaluator constructs the full scheduled grid from the run start, duration,
and all 12 configured devices. It uses a left join so missing feature or
prediction records remain visible. Primary classification metrics include only
rows with valid features, a valid prediction, and a `normal` or `anomaly`
ground-truth label. `transition` and `unknown` rows remain in the export but are
excluded from the primary confusion matrix.

Counts are aggregated before ratios are computed. Undefined denominators remain
empty/NA with a reason; they are not reported as 100%. Event detection requires
an anomaly decision for the correct target device, from a window intersecting
the real phase, recorded between the real phase start and `phase_end + 20 s`.

## 7. Result layout

```text
results/
  campaign_manifest.json
  environment.json
  functional_tests.csv
  run_plan.csv
  split_manifest.json
  artifacts/
    model.joblib
    model_metadata.json
    artifact_manifest.json
    training_features.csv
    validation_features.csv
  runs/{run_id}/
    run.db
    run.db.collector-state.json
    run_manifest.json
    messages.csv
    scenario_phases.csv
    feature_windows.csv
    predictions.csv
    alerts.csv
    resource_samples.csv
    timing_samples.csv
    orchestration_events.csv
    runtime.log
    attempts/          # immutable evidence from failed or interrupted attempts
  evaluation_windows.csv
  metrics_by_run.csv
  metrics_by_scenario.csv
  event_detection.csv
  evaluation_summary.json
  evidence/
  figures/
  screenshots/
```

`timing_samples.csv` contains one row per unique inference call, including the
ordered window list, batch size, monotonic call duration, and measurement time.
Per-window processing delay remains in `predictions.csv` and
`evaluation_windows.csv`. `orchestration_events.csv` is separate so controller
and service-start drift cannot be mistaken for inference timing.

## 8. Validity gates

Before reporting a number, verify all of the following:

- the manifest says `completed`, has actual UTC start/end times, and identifies
  the exact source version and collection session;
- all 12 expected Device IDs produced canonical telemetry;
- the scheduled-window count matches the duration formula;
- K2-K4 are visible in observed rate, gap, or payload features, not merely in
  the run filename or controller configuration;
- scenario start/end records form one complete pair for the intended target;
- train, validation, and test run IDs are disjoint;
- test predictions use the frozen artifact and include all valid normal and
  anomalous decisions;
- coverage and every exclusion reason are reported;
- figures can be regenerated from exported CSV files;
- logs and exported configuration contain no secret.

Low F1, high FPR, or a missed scenario remains a valid result and must be
reported. Do not tune against held-out test runs, remove unfavorable completed
runs, or convert a missed detection into a zero-second latency.
