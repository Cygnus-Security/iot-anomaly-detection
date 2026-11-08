# IoT Anomaly Detection System

Research prototype for **detecting abnormal communication behavior in an IoT
network using Machine Learning**. The current system creates real, authenticated
MQTT traffic from independently identified virtual devices, stores observations
and experimental ground truth in SQLite, and visualizes them in a live dashboard.

## Current implementation status

| Phase | Status | Delivered |
|---|---|---|
| 1 - System design | Complete | Requirements, architecture, protocol, data, security, phase gates |
| 2 - IoT simulation | Complete | Mosquitto, 12 default clients, collector, SQLite/CSV, K1-K4 controls, tests |
| 3 - Machine learning | Not implemented | Planned 30-second features and Isolation Forest |
| 4 - Dashboard | Implemented for collected data | Live Streamlit monitoring; AI alerts activate only after Phase 3 produces detections |
| 5 - Experiments | Not run | No metrics are claimed or pre-generated |

The source does not label K2-K4 as attacks. They are controlled communication
anomalies used to generate ground truth for later ML evaluation.

## Architecture

```text
Virtual devices --telemetry/status--> Mosquitto --accepted iot/#--> Collector
       ^                                  |                              |
       |                                  +--> broker-only logs          v
       +--------- per-device command <--- Operator CLI                SQLite
                                                                         |
                                                                         v
                                                                        CSV
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/MQTT_PROTOCOL.md](docs/MQTT_PROTOCOL.md) for the exact contracts and
subscriber observation boundary.

## Prerequisites

- Docker Engine with Docker Compose v2
- OpenSSL (only for the credential bootstrap script)

No Python installation is required for the Docker workflow.

## Start the Phase 2 lab

```bash
./scripts/bootstrap_env.sh
docker compose up --build -d mosquitto collector simulator dashboard
docker compose ps
```

The default fleet has 12 devices (`device-001` through `device-012`). Mosquitto
is exposed only at `127.0.0.1:1883`; all services also use the private Compose
network. Follow live activity with:

```bash
docker compose logs -f --tail=100 mosquitto collector simulator
```

Open the dashboard at [http://127.0.0.1:8501](http://127.0.0.1:8501). It is
bound to localhost and refreshes live data every five seconds by default.

The interface provides:

- fleet health, traffic rate, payload validation, and experiment counters;
- MQTT telemetry frequency and ground-truth distribution charts;
- per-device payload size, inter-arrival time, sensor history, and raw metadata;
- K1-K4 run history with configured and actual duration;
- an AI/Alerts tab that reads real `anomaly_detections` when Phase 3 creates it.

Until Phase 3 is implemented, the AI tab explicitly reports that inference data
does not exist and shows K2-K4 only as experimental ground truth. It never
manufactures anomaly scores or alerts. See
[docs/DASHBOARD.md](docs/DASHBOARD.md) for metric definitions.

Inspect evidence collected from real MQTT messages:

```bash
docker compose exec collector python -m src.collector.summary \
  --database /app/data/runtime/iot_anomaly.db
```

## Trigger repeatable scenarios

The controller sends an authenticated MQTT command and exits only after the
target device acknowledges `scenario_started` on its status topic.

```bash
# K1 - finite normal communication experiment
docker compose --profile tools run --rm scenario \
  --device-id device-001 --scenario normal --duration-seconds 30 --seed 42

# K2 - message burst
docker compose --profile tools run --rm scenario \
  --device-id device-002 --scenario message_burst --duration-seconds 30 --seed 42

# K3 - alternating abnormal publish interval
docker compose --profile tools run --rm scenario \
  --device-id device-003 --scenario abnormal_interval --duration-seconds 30 --seed 42

# K4 - abnormal payload size
docker compose --profile tools run --rm scenario \
  --device-id device-004 --scenario abnormal_payload --duration-seconds 30 --seed 42
```

Starting another scenario on the same device ends the current run with reason
`replaced_by_command`. Normal behavior resumes automatically after duration.

## Export raw data

```bash
docker compose exec collector python -m src.collector.export_csv \
  --database /app/data/runtime/iot_anomaly.db \
  --output-dir /app/data/exports
```

The command writes `mqtt_observations.csv`, `devices.csv`, and
`scenario_events.csv` under `data/exports/`. SQLite in `data/runtime/` remains
the source of truth. Runtime artifacts are Git-ignored.

## Tests

Run deterministic unit tests in the application image:

```bash
docker compose --profile test run --rm --build tests pytest tests/unit
```

Run broker integration tests for authenticated publish/subscribe, invalid
password rejection, and per-device ACL isolation:

```bash
docker compose --profile test run --rm --build tests pytest tests/integration
```

Run all tests:

```bash
docker compose --profile test run --rm --build tests pytest
```

## Stop the lab

```bash
docker compose down
```

This preserves the named broker volumes and the bind-mounted SQLite dataset.
Use `docker compose down -v` only when you intentionally want to remove broker
runtime volumes; it does not delete `data/runtime/iot_anomaly.db`.

## Configuration

All settings are documented in `.env.example`. The bootstrap script refuses to
overwrite an existing `.env`. Device identities are unique but share one lab
password in Phase 2; production provisioning requires unique secrets and TLS.

## Repository map

- `docs/` - Phase 1 design and phase gates
- `docker/mosquitto/` - authenticated broker image and ACL generation
- `src/simulator/` - virtual devices, K1-K4 behavior, operator CLI
- `src/collector/` - MQTT service, SQLite store, summary and CSV export
- `src/features/`, `src/ml/` - reserved packages for Phase 3
- `src/dashboard/` - live Streamlit dashboard and read-only SQLite query layer
- `tests/` - unit and real-broker integration tests
- `data/`, `models/` - ignored runtime/generated artifacts

## Known Phase 2 limitations

- The collector observes broker-forwarded publications, not failed connections,
  rejected publishes, or the broker session table. Those remain in broker logs.
- TLS and unique per-device credentials are production work, not MVP features.
- SQLite is appropriate for one-laptop research experiments, not horizontal
  scale.
- ML, real-time inference, model metrics, and AI alerts are intentionally not
  claimed until Phase 3 and Phase 5 are implemented and measured. The dashboard
  already exposes real Phase 2 observations and experiment ground truth.
