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

Outputs: deterministic 20-second windows with a 5-second stride, leakage-safe run
split, fitted preprocessing, trained Isolation Forest artifact, real-time
inference, and measured precision/recall/F1/FPR/latency.

Gate: training and inference consume the same versioned feature contract; no
ground-truth field enters the model; reported metrics are generated artifacts.

Current source state: the shared window/feature implementation, Isolation
Forest fit/calibration and trusted artifact contract, analytics persistence,
training/export/evaluation CLIs, and their deterministic tests are implemented.
The completed campaign generated the artifact and held-out metrics rather than
predeclaring them: window-level F1 is 0.2443 and the separate event rule detected
7/9 phases.

## Phase 4 - Dashboard and integration

Outputs: Streamlit device/activity views, traffic charts, current alerts,
suspect device detail, and anomaly history backed by SQLite.

Gate: every displayed number can be traced to stored data or model output.

Current state: device/activity views, traffic charts, scenario history, all
model decisions, score/threshold charts, and alert history are implemented.
The latter remain data-gated: they display only records actually persisted by
analytics and never derive a detection from the scenario label.

## Phase 5 - Experiment and evaluation

Outputs: repeated independent K1-K4 sessions, raw/processed datasets, result
tables and plots, limitations, and reproducible experiment instructions.

Gate: a clean machine can reproduce the experiment and metrics from source.

Current source state: the frozen 13-run plan, resumable Compose runner,
functional evidence matrix, exporter, evaluator, and automatic figures are
implemented. Phase 5 completed 13/13 live runs, generated the evaluation
artifacts, passed T01-T16, and retained unfavorable outcomes. The local
`results/` directory remains Git-ignored. Assembled result reports, copied
figures, and dashboard screenshots are also local outputs and are not committed.

## Parallel work boundaries

After Phase 1 contracts are frozen, the following tracks can run in parallel
without editing the same files:

- Broker/security: `docker/mosquitto`, Compose broker configuration.
- Device simulation: `src/simulator`, scenario unit tests.
- Collection/storage: `src/collector`, database unit tests.
- Documentation/operations: `README.md`, scripts, runbooks.

Physical raw schema v3 remains the immutable observation boundary. Derived
feature, prediction, inference-call, and alert tables extend each run-isolated
database without changing that raw schema version. Dashboard anomaly views stay
data-gated until analytics creates real prediction records.
