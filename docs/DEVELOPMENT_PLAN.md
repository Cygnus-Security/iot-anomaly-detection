# Development plan

## Phase 1 - System design

Inputs: research requirements and an empty repository.

Outputs: requirements traceability, architecture, MQTT contracts, data design,
security boundary, phased plan, and repository skeleton.

Gate: every Phase 2 component has an explicit contract and ownership boundary.

## Phase 2 - IoT simulation and collection

Outputs: authenticated Mosquitto, 10-20 virtual Paho clients, K1-K4 controller,
SQLite collector, CSV exporter, Compose runtime, and unit/integration tests.

Gate: the acceptance gate in `REQUIREMENTS.md` passes using real MQTT traffic.

## Phase 3 - Machine learning

Inputs: independent normal training sessions and mixed labelled test sessions.

Outputs: deterministic 30-second feature extraction, leakage-safe session
split, fitted preprocessing, trained Isolation Forest artifact, real-time
inference, and measured precision/recall/F1/FPR/latency.

Gate: training and inference consume the same versioned feature contract; no
ground-truth field enters the model; reported metrics are generated artifacts.

## Phase 4 - Dashboard and integration

Outputs: Streamlit device/activity views, traffic charts, current alerts,
suspect device detail, and anomaly history backed by SQLite.

Gate: every displayed number can be traced to stored data or model output.

Current state: device/activity views, traffic charts, scenario history, and the
read-only dashboard runtime are implemented. Alert and anomaly-history widgets
remain data-gated: they activate only after Phase 3 creates real
`anomaly_detections` records.

## Phase 5 - Experiment and evaluation

Outputs: repeated independent K1-K4 sessions, raw/processed datasets, result
tables and plots, limitations, and reproducible experiment instructions.

Gate: a clean machine can reproduce the experiment and metrics from source.

## Parallel work boundaries

After Phase 1 contracts are frozen, the following tracks can run in parallel
without editing the same files:

- Broker/security: `docker/mosquitto`, Compose broker configuration.
- Device simulation: `src/simulator`, scenario unit tests.
- Collection/storage: `src/collector`, database unit tests.
- Documentation/operations: `README.md`, scripts, runbooks.

Phase 3 feature extraction must wait for the raw schema to stabilize. Dashboard
queries can be developed alongside ML only after Phase 3 adds anomaly tables.
