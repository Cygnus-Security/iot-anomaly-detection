# System Requirements and MVP Scope

**Document version:** 1.2

**Updated:** 2026-11-11

**Status:** Living target specification with implementation traceability

## 1. Purpose and product goal

The IoT Anomaly Detection System is a research prototype for observing real
MQTT application traffic and detecting deviations from normal device
communication behavior with Isolation Forest. It is not a packet-capture,
intrusion-attribution, or production security platform.

The target MVP combines virtual IoT devices, an authenticated Eclipse
Mosquitto broker, a collector, a versioned raw-data store, per-device feature
windows, offline model training, online inference, and a Streamlit dashboard.
Virtual devices and a future ESP32 publisher use the same MQTT and JSON
contracts so that hardware integration does not require a collector rewrite.

Implementation status is recorded separately from the target requirement:

- **Implemented** means source code and deterministic contract tests exist.
- **Partial** means a useful subset exists, but the acceptance criteria are not
  yet fully met or require live campaign evidence.
- **Planned** means the requirement is part of the target design but must not be
  described as a delivered capability.

The source implements training and inference, but no generated model, anomaly
score, alert, or measured ML performance is claimed until reproducible live
experiments create it under `results/`.

## 2. Scope

### 2.1. Target MVP scope

- MQTT 3.1.1 over the isolated Docker network, using Eclipse Mosquitto and
  Eclipse Paho MQTT.
- Ten to twenty independently identified devices, with 12 devices by default.
- A normal communication baseline of approximately one telemetry message every
  5 seconds per device, with bounded jitter.
- Versioned telemetry, retained status, scenario-command, and experiment-event
  topics defined by `MQTT_PROTOCOL.md`.
- Collection of broker-accepted application-level MQTT publications. The
  collector does not claim to observe every TCP packet, CONNECT attempt,
  authentication failure, or broker-rejected publish.
- SQLite as the source of truth, plus reproducible CSV exports.
- Four bounded scenarios: K1 normal, K2 message burst, K3 irregular interval,
  and K4 payload-size shift.
- Per-device communication windows, Isolation Forest training and inference,
  anomaly alerts, and a read-only monitoring dashboard.
- Docker Compose deployment on one ordinary computer without a GPU.

### 2.2. Out of scope

- Attributing a communication anomaly to a specific botnet, DDoS campaign, or
  successful compromise without independent evidence.
- Snort/Suricata, deep packet inspection, full packet capture, a complete SIEM
  or SOAR platform, federated learning, or public-Internet deployment.
- Using unusual temperature or humidity values as the primary ML signal. The
  model targets communication timing, frequency, and payload-size behavior.
- Production-scale availability, horizontal scaling, or performance claims for
  untested hardware and traffic profiles.
- Running unbounded traffic or denial-of-service experiments outside the
  isolated lab.

### 2.3. Hardware boundary

The target design includes a reference IoT device with an MCU, sensors, an MQTT
client, and IP connectivity. The current MVP uses software simulation and must
not be described as a completed physical-device implementation. A future ESP32
must publish the same topics, QoS, retained-status policy, and JSON schema.

## 3. Current implementation baseline

| Capability | Status | Source evidence or remaining gap |
|---|---|---|
| Independent virtual devices | Implemented | `src/simulator/main.py`, `src/simulator/device.py` |
| Authenticated broker and role ACLs | Implemented | `docker/mosquitto/` and six broker integration tests verify authentication, per-device ACL isolation, retained delivery, Last Will, and payload limits |
| MQTT 3.1.1 and versioned topics | Implemented | Simulator, controller, collector, ACLs, documentation, and tests use the same MQTT 3.1.1 contract |
| Raw MQTT collection and SQLite persistence | Implemented | `src/collector/` validates and persists schema-v3 telemetry, retained status, separate MQTT/application duplicate flags, and separated experiment events |
| K1-K4 scenario generation | Implemented | `src/simulator/` provides deterministic bounded scenarios; live K1-K4 runs produced ordered start/end pairs and scenario-associated telemetry |
| Raw CSV export | Implemented | `src/collector/export_csv.py` exports the current raw tables |
| Monitoring and AI dashboard | Implemented | `src/dashboard/` reads raw and derived records in English and displays all decisions, score/threshold, and alerts only when persisted |
| Window features | Implemented | `src/features/windows.py` produces the exact scheduled half-open windows, quality states, and seven features |
| Isolation Forest training and inference | Implemented | `src/ml/`, `src/analytics/`, and `src/pipeline/training.py` implement fit, calibration, artifact validation, scoring, timing, and persistence |
| ML evaluation and measured metrics | Implemented and live-measured | The completed 13-run campaign generated complete-grid metrics, event results, timing data, and figures under local `results/` |

### 3.1. Verified Phase 2 evidence

The current Phase 2 baseline has the following recorded verification evidence:

- 40 host unit tests passed.
- 46 tests passed in Docker, including six real-broker integration tests.
- Docker Compose configuration validation passed.
- Schema-v1-to-v3 and schema-v2-to-v3 migration tests passed, and the live
  database was migrated from version 2 to version 3.
- Mosquitto, collector, and dashboard health checks passed while the simulator
  ran all 12 connected clients.
- All 12 default `iot-NNN` devices connected and produced stored observations.
- Retained status was observed with `retain = 1`, and an ungraceful disconnect
  activated the configured retained Last Will in the broker integration suite.
- Short live K1-K4 runs produced ordered `scenario_started` and
  `scenario_ended` events. K2 associated 20 telemetry rows and an approximately
  0.12-second minimum gap; K4 produced a 1416-byte observation.
- A quiescent live export matched SQLite exactly: 19,662 observations, 24 device
  rows including legacy identities, and 18 scenario events.

## 4. Actors and operational flows

| Actor or component | Responsibility |
|---|---|
| Operator | Starts the lab, invokes the authenticated scenario CLI, observes the dashboard, and exports evidence |
| Device or simulator | Maintains an authenticated MQTT session and publishes telemetry, status, and experiment events |
| MQTT broker | Authenticates identities, enforces ACLs, and forwards accepted publications |
| Collector | Records collector receipt time, validates contracts, identifies duplicates, and persists observations |
| Feature processor | Builds leakage-safe per-device windows from valid, non-duplicate telemetry |
| Trainer | Fits and versions preprocessing, Isolation Forest, threshold, and feature metadata offline |
| Detector | Scores eligible windows and creates traceable anomaly alerts |
| Researcher | Defines independent runs, exports datasets, and computes evaluation metrics |

The Streamlit dashboard remains a read-only consumer of SQLite. Scenario
activation is performed by the authenticated operator CLI, not by granting the
dashboard broker write privileges.

## 5. Functional requirements

| ID | Target requirement and acceptance criteria | Status | Evidence or gap |
|---|---|---|---|
| FR-01 | Run at least 10 independently identified MQTT clients; the default deployment runs 12. Each client has separate session state, sequence state, and RNG state. | Implemented | `src/simulator/main.py`, `src/simulator/device.py`, `src/common/config.py` |
| FR-02 | Use MQTT 3.1.1 with authentication and ACLs. Publish QoS 1 telemetry and retained QoS 1 status on `iot/v1/devices/{device_id}/...`; commands use the matching command topic. | Implemented | Six Docker integration tests passed, including a real per-device cross-topic denial, retained replay, and forced Last Will |
| FR-03 | Show last-seen and online/offline/stale state. Distinguish received status/LWT from a state estimated by elapsed collector receipt time. | Implemented | Schema-v3 dashboard queries and UI tests passed; the full stack was healthy with 12 connected devices |
| FR-04 | Store collector receipt time, original topic, payload byte length, Device ID, session ID, sequence number, QoS, retain/duplicate metadata, sender time, and bounded raw JSON. Invalid input must not enter feature generation. | Implemented | Schema-v3 storage plus the canonical-input predicate exclude invalid, duplicate, retained, non-telemetry, and sessionless rows |
| FR-05 | Build one feature vector per device window with a single versioned implementation reused by training and inference. | Implemented | `src/features/windows.py`; boundary, population-standard-deviation, UTF-8 byte, quality, and label tests |
| FR-06 | Export raw observations, experiment ground truth, feature windows, split manifests, scores, and metrics with run IDs and versioned schemas. | Implemented; products generated on demand | `src/pipeline/exporting.py`, `src/pipeline/evaluation.py`, and `results/` contracts |
| FR-07 | Run K1-K4 with explicit duration and seed. Publish ground truth on `experiments/v1/devices/{device_id}/events`; never place scenario labels in telemetry or model inputs. | Implemented and live-verified | K1 plus all nine K2-K4 held-out runs completed with separate protocol events, observed phases, and a seven-feature allow-list |
| FR-08 | Fit Isolation Forest on independent normal runs and save the exact feature order, threshold, window configuration, random seed, run IDs, checksums, and library versions. | Implemented; artifact generated after collection | `src/pipeline/training.py` and `src/ml/model.py` |
| FR-09 | Load a trusted compatible artifact and score every eligible new window. Reject missing/checksum/schema-invalid configured artifacts; continue feature collection when no model is configured. | Implemented | `src/ml.load_artifact` and `src.analytics.service` |
| FR-10 | Persist every valid prediction and a traceable alert subset with Device ID, window ID, detection time, score, threshold, model version, and run/session identity. | Implemented | `window_predictions`, `inference_calls`, and `anomaly_alerts`; campaign evaluation intentionally applies no alert cooldown |
| FR-11 | Provide a Streamlit interface for fleet state, throughput, traffic charts, run history, model readiness, scores, and alert history. Every number must trace to stored data or real model output. | Implemented | Read-only raw and Phase 3 query/UI tests; missing and zero-alert states remain explicit |
| FR-12 | Produce confusion matrix, Precision, Recall, F1, FPR, detection latency, scored-window count, insufficient-data count, and missed-run count from held-out runs. | Implemented and live-measured | 4,248 test windows were scored; 4,176 entered the primary matrix, and event evaluation retained all nine planned events including two missed K4 events |
| FR-13 | Provide deterministic seeds, external configuration, versioned schemas, structured logs, documented commands, test fixtures, and split manifests sufficient for reproduction. | Implemented | Frozen plan/split, manifests, artifact metadata, runner, runbook, and automated evidence mapping |
| FR-14 | Compose starts the configured MQTT/dashboard runtime and exposes analytics as an explicit experiment-profile service; training/evaluation run on demand. | Implemented | `docker-compose.yml`, campaign runner, and host CLIs |

## 6. Non-functional requirements

| ID | Requirement and verification | Status |
|---|---|---|
| NFR-01 | Keep simulator, collection, feature processing, ML, and dashboard responsibilities in separate modules. Verify through repository structure and import boundaries. | Implemented |
| NFR-02 | Log operational failures and handle broker reconnect, invalid JSON, invalid identity, oversized payload, missing model, and SQLite contention without fabricating data. | Implemented for the lab contracts; live stress evidence remains part of T09/T16 |
| NFR-03 | Run the 12-device demo on one ordinary machine without a GPU. Verify through the Compose smoke test. | Implemented; the running stack recorded all 12 default devices |
| NFR-04 | Load configuration and secrets from environment or secret files. Commit `.env.example`, never a live `.env` or generated model/runtime data. | Implemented |
| NFR-05 | Disable anonymous broker access, apply least-privilege ACLs, and bind host ports to localhost by default. | Implemented; configuration and broker integration tests verify the Phase 2 boundary |
| NFR-06 | Preserve sent, received, scenario-boundary, window, inference-call, prediction, and detection timestamps so every latency claim is auditable. | Implemented |
| NFR-07 | Provide unit tests for contracts, feature math, window boundaries, duplicate handling, labels, artifact compatibility, and evaluation, plus real-broker integration tests. | Implemented; the separate T01-T16 report records which live checks actually ran |
| NFR-08 | Permit a conforming ESP32 to replace a virtual device without collector or feature-code changes. Verify with a protocol conformance test before claiming hardware support. | Partial |

No fixed F1 or latency target is declared before experiments. The acceptance
criterion is the ability to measure, persist, reproduce, and explain real
results; model effectiveness must be concluded from evidence.

## 7. Data and evaluation contract

- One ML sample is one activity window for one device, not one MQTT message or
  one sensor reading.
- The default window is 20 seconds, the stride is 5 seconds, and at least three
  valid, non-duplicate telemetry messages are required.
- Windows use collector receipt time with half-open bounds `[start, end)`.
- A window below the minimum is recorded as `insufficient_data` and is not
  passed to the model. Missing values must not be imputed merely to force a
  prediction.
- QoS 1 duplicates are identified by `(device_id, session_id, sequence_no)` and
  excluded from feature counts while remaining auditable as raw observations.
- The canonical feature order and formulas are defined in `DATA_DESIGN.md` and
  stored with each model artifact.
- Training, validation, and test data are split by campaign `run_id` before any
  overlapping windows can cross a split boundary. The device-emitted
  `experiment_run_id` UUID identifies scenario events, not dataset membership.
- K1 supplies normal evaluation windows. For K2-K4, only target-device windows
  wholly inside the abnormal interval are labelled `anomaly`.
- Boundary-crossing windows are labelled `transition` and excluded from primary
  metrics. Other devices in the run remain normal controls.
- Scenario, label, Device ID, experiment ID, and raw timestamps are metadata,
  never model features.
- If no model is configured, feature collection continues and the analytics
  ready record reports `model_loaded=false`; it creates no synthetic scores or
  alerts. If an explicitly configured artifact is missing or incompatible,
  analytics fails closed.

## 8. Target deliverables

| Deliverable | Current status |
|---|---|
| Organized source, README, environment template, and Docker Compose runtime | Implemented |
| Requirements, architecture, MQTT, data, dashboard, security, and campaign runbook | Implemented with source/evidence boundaries |
| Reproducible raw, processed, ground-truth, split, and score datasets | Generated by the completed local live campaign |
| Versioned Isolation Forest artifact and training/evaluation commands | Implemented and generated from 1,368 training plus 684 validation windows |
| Streamlit monitoring, scenario demonstration, and real alert history | Implemented and data-gated |
| Evidence tables and plots for the research report | Generated on demand under ignored local output paths; not committed |

## 9. Acceptance gates

### Phase 2 core runtime — passed with one diagnostic gap

This gate passed: Compose configuration validated; broker, collector, simulator,
and dashboard were healthy; 12 devices appeared in SQLite; MQTT 3.1.1
authentication and versioned-topic integration tests passed; retained status
was stored with `retain = 1`; a forced disconnect activated Last Will; schema
versions 1 and 2 migrate to version 3; and live K1-K4 runs produced ordered
separated events. Raw CSV export matched all three source-table row counts on a
quiescent snapshot. Invalid experiment events are rejected and logged but do not
yet receive a persisted diagnostic row; this keeps the full MQTT protocol
contract Partial without blocking the verified core runtime.

### Feature and ML implementation — passed by source and deterministic tests

Training and inference call the same feature code; window and duplicate tests
exist; artifact checksum/compatibility is enforced; no-model state creates no
fake output; and analytics persists every score plus the alert subset.

### Experiment and reporting — passed with measured limitations

The frozen campaign completed 13/13 runs and 6,300 scheduled windows. All 4,248
held-out windows were scored; 4,176 fully labelled rows produced TP=16, FP=43,
TN=4,061, FN=56, Precision=0.2712, Recall=0.2222, F1=0.2443, and FPR=0.01048.
The distinct event criterion detected 7/9 phases with 3.710-second median
latency. It does not override the zero window-level recall observed for K3 and
K4. Replay produced zero mismatches and functional checks T01-T16 passed. The
generated `results/` tree is local and Git-ignored; generated dashboard
screenshots, copied figures, and assembled result reports are also treated as
local outputs and are not committed.

## 10. Research-report traceability

| Report section | Repository evidence |
|---|---|
| System requirements | This document, acceptance tests, and Compose health checks |
| Analysis and design | `ARCHITECTURE.md`, `MQTT_PROTOCOL.md`, and `DATA_DESIGN.md` |
| IoT implementation | Broker configuration, simulator, collector, SQLite records, and raw exports |
| AI implementation | Feature code, split manifests, trainer, model registry, detector, and artifact metadata |
| Results | Reproducible scenario runs, score exports, metric files, plots, and dashboard records |

## 11. Technical references

- OASIS MQTT 3.1.1: <https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html>
- Eclipse Mosquitto documentation: <https://mosquitto.org/documentation/>
- Eclipse Paho Python client: <https://eclipse.dev/paho/clients/python/>
- Scikit-learn Isolation Forest: <https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html>
