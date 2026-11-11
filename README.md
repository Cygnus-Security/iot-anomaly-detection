# IoT Anomaly Detection System

Research prototype for detecting abnormal IoT communication behavior with real,
authenticated MQTT traffic and an Isolation Forest model. Twelve independently
identified virtual devices publish through Mosquitto; a collector stores raw
observations in SQLite; a shared window implementation builds communication
features; and the analytics service persists every model decision and alert.

K2-K4 are controlled communication anomalies and experimental ground truth.
They are not described as attacks, botnets, or proof of compromise.

## Implementation status

| Phase | Source status | Evidence boundary |
|---|---|---|
| 1 - Design | Implemented | Requirements, architecture, MQTT, data, security, and experiment contracts |
| 2 - MQTT simulation and collection | Implemented and previously smoke-tested | Mosquitto, 12 clients, K1-K4 control, collector, SQLite, raw export, tests |
| 3 - Features and machine learning | Implemented and unit-tested | Scheduled 20-second windows, seven features, Isolation Forest training/calibration, trusted artifact loading, online scoring |
| 4 - Dashboard | Implemented and unit-tested | Read-only telemetry, scenario, all-prediction, score/threshold, and alert views |
| 5 - Chapter IV campaign | Runner and evaluation pipeline implemented | The frozen 13-run campaign must complete before any measured metric is claimed |

Source availability is not an experimental result. Generated model files,
predictions, metrics, figures, and screenshots are valid evidence only after the
corresponding live run completes under `results/`.

## Runtime data flow

```text
Virtual devices --> Mosquitto --> Collector --> live per-run SQLite named volume
       ^                 |                         |
       |                 +--> broker logs         +--> scheduled feature windows
       +-- Operator CLI                               --> Isolation Forest decisions
                                                        --> alerts
                                                             |
                                                             v
                                                    verified results snapshot
                                                             |
                                                             v
                                                    read-only dashboard

train runs --> offline trainer --> checksum-protected artifact --> test-run analytics
                                                           |
                                                           v
                                                exporter and evaluator
```

See [System Architecture](docs/ARCHITECTURE.md),
[MQTT Protocol](docs/MQTT_PROTOCOL.md), and
[Data Design](docs/DATA_DESIGN.md) for the normative contracts.

## Prerequisites

- Docker Engine with Docker Compose v2
- Python 3.11 or later for host-side orchestration and evaluation
- OpenSSL for the credential bootstrap script

Create the local environment once:

```bash
./scripts/bootstrap_env.sh
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

The bootstrap script refuses to overwrite an existing `.env`. Generated
credentials, databases, models, and result data are Git-ignored.

## Interactive lab

Start the MQTT collection and dashboard path:

```bash
docker compose up --build -d mosquitto collector simulator dashboard
docker compose ps
```

The broker and dashboard are bound to localhost. Open
<http://127.0.0.1:8501>. The default fleet is `iot-001` through `iot-012`.

Trigger a finite scenario with an authenticated command:

```bash
docker compose --profile tools run --rm scenario \
  --device-id iot-002 \
  --scenario message_burst \
  --duration-seconds 30 \
  --seed 42
```

Allowed scenarios are `normal` (K1), `message_burst` (K2),
`abnormal_interval` (K3), and `abnormal_payload` (K4). The controller exits only
after the target device acknowledges the real `scenario_started` event.

Export an interactive raw database:

```bash
docker compose exec collector python -m src.collector.export_csv \
  --database /app/data/runtime/iot_anomaly.db \
  --output-dir /app/data/exports
```

## Chapter IV campaign

The frozen plan is `experiments/run_plan.csv`; the run-level split is
`experiments/split_manifest.json`. It contains two normal training runs, one
normal validation run, one normal test run, and three independent runs for each
of K2-K4. The required order is:

```text
train --> validation --> freeze model and threshold --> held-out test
```

Inspect the exact plan without changing state:

```bash
.venv/bin/python scripts/run_campaign.py --dry-run
```

Before collection, initialize the non-overwriting result tree and run the
functional evidence matrix outside the ML split:

```bash
.venv/bin/python -m src.experiments.prepare --results-dir results
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

Missing live evidence is recorded as `not_run`; it is never converted to a pass
because matching source code exists.

Run the campaign. The trainer hook runs before the first test if the trusted
artifact does not yet exist:

```bash
.venv/bin/python scripts/run_campaign.py \
  --build \
  --trainer-command ".venv/bin/python scripts/train_model.py --results-dir results"
```

The runner creates each live database in a Docker named volume shared only by
the Linux Collector and Analytics containers. The host does not open that WAL
database while either writer is active. After Analytics completes, the runner
stops the writers, checkpoints and integrity-checks the database, and only then
copies the verified database and collector-state sidecar into the run's
`results/` directory. A failed integrity gate cannot enter training or
evaluation. The runner also records actual UTC boundaries, samples Docker
resource use, and refuses to overwrite completed evidence. To archive and retry
a failed or interrupted attempt, repeat the command with `--retry-failed`.

After all runs complete, export every run and generate evaluation tables and
figures:

```bash
.venv/bin/python scripts/export_run.py --results-dir results
.venv/bin/python scripts/evaluate_results.py --results-dir results
```

See [Chapter IV Runbook](docs/CHAPTER4_RUNBOOK.md) for live-evidence format,
result files, recovery rules, and validity gates.

## Model and inference contract

- Each sample is one device window anchored to the real run start.
- Windows are 20 seconds long, advance every 5 seconds, and use `[start,end)`.
- Fewer than three canonical telemetry messages produce
  `insufficient_data`, not a synthetic normal prediction.
- The exact feature order is message count, rate, payload mean/std,
  inter-arrival mean/std, and one-second peak count.
- Training uses only the two normal training runs. The frozen threshold is the
  first percentile of normal validation `decision_function` scores.
- A window is anomalous only when `score < threshold`; equality is normal.
- The model checksum, feature schema, window settings, run IDs, seed, threshold,
  and dependency versions are verified when the artifact is loaded.
- Joblib files are loaded only through the explicit trusted-artifact path.
- All valid normal and anomalous decisions are stored in
  `window_predictions`; `anomaly_alerts` is a traceable subset.
- Ground-truth phase labels are evaluation metadata and never model inputs.

## Dashboard

The interface provides fleet health, traffic, device details, K1-K4 history,
and an **AI & Alerts** tab. When a test database contains predictions, that tab
shows every score, the frozen threshold, predicted label, model version, and
the alert subset. A database with analytics tables but no model decisions is
reported as such; the interface never substitutes scenario labels for ML
output.

## Tests

```bash
# Host suite
.venv/bin/pytest

# Deterministic tests in the application image
docker compose --profile test run --rm --build tests pytest tests/unit

# Authenticated broker integration tests
docker compose --profile test run --rm --build tests pytest tests/integration
```

## Repository map

- `docker/mosquitto/` - authenticated broker image and generated ACLs
- `src/simulator/` - virtual devices and K1-K4 behavior
- `src/collector/` - MQTT collection, validation, raw SQLite, and raw export
- `src/features/` - shared scheduled windows and seven-feature schema
- `src/ml/` - Isolation Forest contracts, artifacts, scoring, and metrics
- `src/analytics/` - online feature/prediction/alert persistence
- `src/experiments/` - frozen plan, evidence handling, and campaign runner
- `src/pipeline/` - training, run export, evaluation, and figures
- `src/dashboard/` - read-only Streamlit interface
- `experiments/` - canonical run plan and split manifest
- `results/` - generated campaign evidence; not committed by default

## Limits

- The collector sees broker-forwarded publications, not rejected connections or
  publishes; authentication and ACL denials remain broker-log evidence.
- This is application-level device simulation over real MQTT, not ESP32 or
  wireless-channel emulation.
- TLS and unique production device credentials are deployment work outside the
  isolated lab.
- SQLite is suitable for this one-computer research campaign, not horizontal
  production scale.
- Overlapping windows are not independent experimental repetitions. Three runs
  per anomaly scenario support a preliminary lab assessment, not broad
  generalization.
- No favorable accuracy, F1, latency, or real-time claim is valid until it is
  computed from completed held-out runs under `results/`.
