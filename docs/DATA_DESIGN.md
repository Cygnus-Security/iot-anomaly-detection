# Data collection, persistence, and experiment design

**Design version:** 1.1

**Last updated:** 2026-11-11

**Model family:** Isolation Forest over MQTT telemetry communication behavior

## 1. Purpose and implementation boundary

The system keeps two information paths separate:

1. **Observations** are broker-forwarded MQTT publications recorded by the
   collector. Valid, non-duplicate telemetry observations are the only raw input
   to feature extraction and ML inference.
2. **Experiment ground truth** contains scenario, run, seed, timing, and target
   information emitted through the experiment-event channel. It is used for
   evaluation and never becomes a model feature.

The ML sample unit is one time window for one device. It is not an individual
temperature value, an MQTT control message, or an IP/TCP packet.

Implementation status is explicit:

| Scope | Status | Data products |
|---|---|---|
| Phase 2 collection | Implemented with physical database schema v3 | Raw SQLite tables and raw CSV exports |
| Phase 3 features and ML | Implemented; generated per run | Feature windows, inference calls, all predictions, alerts, and a checksum-protected artifact |
| Phase 5 experiments | Completed and evaluated | Frozen plan/splits, 13 completed run manifests, resource/timing samples, metrics, event results, and figures |

The source implements the Phase 3 contracts below. A table or artifact exists as
experimental evidence only after a corresponding live run creates it under
`results/`; source completion does not pre-generate model performance. The
current local campaign generated 6,300 scheduled windows, including 4,248 scored
held-out windows. This `results/` tree is experimental output and remains
Git-ignored.

## 2. Sources and expected scale

| Source | Content | Role |
|---|---|---|
| MQTT simulator | 10-20 independent devices; 12 by default | Publishes real telemetry and retained status through Mosquitto |
| MQTT collector | Callback metadata and validated payload fields | Creates raw observations with authoritative receipt time |
| Scenario controller and device events | Run IDs, seeds, configurations, boundaries, and target devices | Supplies evaluation-only ground truth |
| Window processor | Per-device scheduled-window statistics | Creates the exact shared feature vectors used by train and inference |
| CICIoT2023, optional | Public IoT dataset | Separate offline experiment only when its features and labels are compatible |

The frozen campaign normal profile publishes approximately once every 5 seconds
with uniform jitter in ±0.5 seconds and a deterministic initial spread of 0-5
seconds. A device has a stable
`device_id`, a new `session_id` at process start, and an increasing `sequence_no`
within that session.

## 3. Observation boundary

The collector subscribes to the versioned topics defined in
`MQTT_PROTOCOL.md`. It records application publications that Mosquitto accepts
and forwards. It does not observe rejected authentication, denied publishes,
CONNECT packets, broker TCP-session state, unforwarded MQTT packets, or
network-layer byte counts. Those events remain in separate broker logs.

`payload_size_bytes` is exactly `len(payload_bytes)` at the collector. It is not
the number of Unicode characters and does not include protocol headers.
Collector receipt time, not the device clock, is the time basis for rates,
windows, and inter-arrival statistics.

## 4. Raw observation contract

The schema-v3 collector retains the existing Phase 2 physical tables through
explicit migrations from schema versions 1 and 2. In `mqtt_observations`, the
logical data contract maps to physical columns as follows:

| Logical value | Physical column | Source and semantics |
|---|---|---|
| Observation ID | `id` | SQLite monotonic row key and receipt-order tie-breaker |
| Device ID | `device_id` | Parsed from the topic and checked against valid JSON |
| Session ID | `session_id` | Telemetry/status UUID; introduced in physical schema v2 |
| Sequence number | `sequence` | Stores telemetry `sequence_no`; null for status rows |
| Topic | `topic` | Original MQTT topic |
| Message type | `message_type` | `telemetry` or `status` |
| Receipt time | `received_at`, `received_at_unix` | Collector UTC clock in ISO and Unix forms |
| Publisher time | `publisher_timestamp` | Stores `sent_at_utc`; diagnostic only |
| Message ID | `message_id` | Optional telemetry tracing UUID |
| Payload size | `payload_size_bytes` | Exact received byte length before decoding |
| Raw payload | `payload_text` | Bounded UTF-8 text; SQLite remains the raw source of truth |
| MQTT metadata | `qos`, `retain`, `mqtt_duplicate` | Values available to the subscriber callback; for rows created under schema v3, `mqtt_duplicate` preserves Paho's diagnostic DUP flag |
| Duplicate state | `duplicate` | Application-level `is_duplicate`, derived only from the canonical telemetry key |
| Validation | `payload_valid`, `validation_error` | Contract result and a safe diagnostic reason |
| Inter-arrival | `inter_arrival_seconds` | Receipt-time gap from the previous valid, canonical telemetry row for the device |
| Scenario metadata | `ground_truth_label`, `scenario_run_id` | Compatibility columns populated from active experiment state; never model inputs |

Experiment publications are validated and persisted in `scenario_events`; they
are not duplicated into `mqtt_observations`. Commands are not collector
observations. Invalid experiment events are currently rejected and logged but
do not yet receive a persisted bounded diagnostic row; this is an explicit raw
observability gap.

The maximum accepted application payload is 4096 bytes. An oversized, invalid,
or identity-mismatched publication is excluded from features. Diagnostic records
must remain bounded so malformed input cannot create unbounded database growth.

### 4.1. Deduplication

The canonical telemetry key is:

```text
(device_id, session_id, sequence_no)
```

The first valid row for a previously unseen key is canonical even when MQTT's
DUP flag is set. The physical `mqtt_duplicate` column preserves that broker flag
for diagnostics, while `duplicate` is set only when the application key was
already observed. Application-duplicate rows may be kept for diagnostics, but
they do not update canonical inter-arrival state and must be excluded from
dashboard rates, feature windows, training, and inference.

Sequence numbers can restart only when `session_id` changes. `message_id` is not
a replacement for the canonical key. Legacy schema-v1 rows without a session ID
must not be mixed into deduplicated training data unless an explicit migration
policy handles them.

### 4.2. Physical-schema migration policy

- A new database starts at physical schema version 3.
- Version 1 adds nullable `session_id` and `mqtt_duplicate`. Because those rows
  predate session-aware application deduplication, their old `duplicate` value is
  preserved as `mqtt_duplicate`, while application `duplicate` is reset. Rows
  without a trustworthy session remain ineligible for session-aware training.
- Version 2 adds `mqtt_duplicate`. Version 2 combined broker DUP and repeated-key
  state in one `duplicate` flag, so exact historical broker-DUP provenance cannot
  be reconstructed. Migration leaves `mqtt_duplicate = 0` for session-aware
  version-2 rows and deterministically recomputes application `duplicate` from
  `(device_id, session_id, sequence)` in receipt order, then rebuilds canonical
  inter-arrival values and the device summary table. A zero broker-DUP value in
  migrated version-2 history means false-or-unknown, not proven false; without a
  per-row provenance marker it MUST NOT support historical broker-DUP analysis.
- Health checks and the dashboard reject unsupported physical schema versions.

### 4.3. Ordering and late data

Rows are ordered by `(received_at_unix, id)`. Device `sent_at_utc` is retained for
debugging but never determines rate or window membership. A repeated or lower
sequence number is diagnostic evidence; the collector does not move it in time
to make the sequence appear ordered.

### 4.4. Collector watermark and loss evidence

The collector atomically maintains `<database>.collector-state.json` with its
queue depth, enqueue/persist watermarks, and bounded loss events. Analytics does
not close a due window while the watermark shows an earlier received message
still pending. A `queue_full` or `storage_error` event creates an
`observation_gap` for every intersecting affected-device window; it is not
silently represented as normal or insufficient data. The sidecar is part of the
run/replay evidence and must be archived with a failed attempt.

## 5. Time-window contract

| Parameter | Value | Rule |
|---|---:|---|
| `window_seconds` | 20 seconds | Per-device sliding window |
| `stride_seconds` | 5 seconds | Distance between successive scoring boundaries |
| `min_messages` | 3 | Fewer canonical telemetry rows yields `insufficient_data` |
| Time basis | `received_at_unix` | Device timestamps are not used for rate calculations |
| Bounds | `[start, end)` | Start inclusive and end exclusive |
| Deduplication | Device, session, sequence | Only canonical rows enter a window |

Windows may overlap. A telemetry row exactly at `window_end` belongs to the next
eligible window, not both sides of the boundary. Inter-arrival gaps are computed
between consecutive canonical telemetry rows inside the ordered window.

Insufficient windows are not forced to zero so that the model can guess. They
are stored or reported as `insufficient_data` and excluded from scoring. A silent
device is primarily a monitoring/status condition; the design does not assume an
Isolation Forest trained on message-bearing windows will detect prolonged
silence.

Because overlapping windows are highly correlated, train, validation, and test
data must be separated by experiment run, never by randomly splitting individual
windows.

## 6. Feature schema v1.0

The following order is exact and must be persisted with every model artifact:

| Order | Feature | Unit | Definition |
|---:|---|---|---|
| 1 | `message_count` | Messages | Number of valid, non-duplicate telemetry publications in the window |
| 2 | `message_rate_hz` | Messages/second | `message_count / 20` |
| 3 | `payload_mean_bytes` | Bytes | Arithmetic mean of `payload_size_bytes` |
| 4 | `payload_std_bytes` | Bytes | Population standard deviation (`ddof=0`) of payload sizes |
| 5 | `interarrival_mean_ms` | Milliseconds | Mean of consecutive collector receipt-time gaps |
| 6 | `interarrival_std_ms` | Milliseconds | Population standard deviation (`ddof=0`) of receipt-time gaps |
| 7 | `peak_messages_per_second` | Messages/second | Largest telemetry count in any one-second bucket aligned to the window start |

The feature schema version is `1.0`. Training and inference must call the same
windowing and feature code. A model load fails closed if feature names, order,
window size, stride, or schema version differ from its artifact metadata.

The following values are grouping, display, traceability, or evaluation metadata
and MUST NOT be model features:

- `device_id` and `device_type`;
- session, sequence, message, request, and experiment IDs;
- scenario and ground-truth labels;
- raw timestamps and window boundary timestamps;
- status, command, retained-delivery, or broker-log fields.

The MVP assumes broadly similar normal communication profiles across devices. If
new device classes publish at materially different normal rates, the experiment
must justify profile-specific models or normalization fitted only on training
data.

## 7. Raw SQLite schema v3

Schema v3 keeps the collector-owned raw tables so prior data and queries can be
migrated without conflating raw and derived records. Analytics adds separate
tables without changing the raw schema version.

### 7.1. `schema_metadata`

Contains the integer database schema version, initialization time, and an
idempotent maintenance revision used to repair derived state after a migration.
Writers and readers reject unsupported versions instead of silently interpreting
columns with the wrong contract.

### 7.2. `mqtt_observations`

Contains one row per broker-forwarded telemetry or status publication processed
by the collector, including bounded invalid or duplicate diagnostic rows. Its
field mapping is defined in Section 4.

Recommended indexes cover `(device_id, received_at_unix)`, message type plus
receipt time, active experiment/run metadata, and the telemetry deduplication
key.

### 7.3. `devices`

Contains one observed summary row per device: first/last seen, last telemetry,
last status, last status state, online flag, and last logical sequence number.
This is application-observed state, not a claim about Mosquitto's live TCP
session table. A retained `online` delivery alone is not current liveness proof.

### 7.4. `scenario_events`

Contains one unique `scenario_started` or `scenario_ended` event with event ID,
experiment run ID, request ID, device, scenario, publisher and collector times,
seed, configured duration, and JSON details. Existing physical column names such
as `event_message_id` and `scenario_run_id` are compatibility names for v1
`event_id` and `experiment_run_id` semantics.

The table is the implemented ground-truth source. A start row without a matching
end row is an explicitly open run, not a completed experiment.

### 7.5. Storage rules

- SQLite uses WAL and a busy timeout. The collector owns raw-table writes and
  analytics owns derived-table writes. Interactive readers may use the local
  `data/runtime/` database, but Chapter IV live readers stay inside the Linux
  container environment.
- A live campaign database resides in the Docker-managed `experiment_data`
  named volume. After Analytics completes and all writers stop, the runner
  checkpoints it, requires `PRAGMA integrity_check` to return `ok`, and copies
  the database plus collector-state sidecar into the per-run `results/`
  directory. Host-side training, export, evaluation, and dashboard inspection
  use only this verified snapshot.
- A failed, interrupted, or corrupt attempt is retained as archive-only
  evidence and is excluded from every model and metric input.
- Verified SQLite snapshots and later model artifacts live under the ignored
  `results/` tree.
- MQTT passwords and other secrets are never written to raw payload columns or
  CSV exports.
- A reader-visible or behavior-changing physical schema update requires a
  schema-version increment and an explicit migration or documented dataset
  reset. A backward-compatible metadata addition or deterministic repair-only
  pass may instead advance a documented, idempotent maintenance revision;
  `CREATE TABLE IF NOT EXISTS` alone is not a migration strategy.
- Raw observations remain the source from which derived features can be rebuilt.

## 8. Implemented derived tables and artifact records

`src.analytics` initializes the following tables in each per-run database. They
are separate from the collector's physical schema version and carry both the
campaign run ID and `collection_session_id`.

| Table | Key fields | Purpose |
|---|---|---|
| `feature_windows` | `window_id`, run/session/device IDs, scheduled index/bounds, seven features, feature status, ground-truth metadata | One row for every scheduled device window, including insufficient/invalid rows |
| `inference_calls` | `inference_call_id`, run/session, model version, ordered window-ID JSON, batch size, duration, measured time | One monotonic timing record per real model call |
| `window_predictions` | `window_id`, run/session/device, score, threshold, predicted label, model/schema/call IDs, recorded time, processing delay | Every valid normal or anomalous model decision |
| `anomaly_alerts` | `window_id`, run/session/device, detection time, score, threshold, model version, reason | Traceable subset where `score < threshold` |

Run configuration, actual boundaries, seed, split, source version, model version,
and output inventory are stored in `run_manifest.json`. Model provenance lives
in `results/artifacts/model_metadata.json` and `artifact_manifest.json`, not a
mutable database registry. `anomaly_alerts` is the canonical alert table; code
and documentation must not introduce a competing `anomaly_detections` contract.

## 9. Ground-truth and evaluation labels

The collector maintains active scenario state from ordered experiment events.
After `scenario_started`, canonical telemetry for that device receives the raw
scenario name and experiment run ID until the matching `scenario_ended`. Outside
an active run it receives raw label `normal` and no run ID. These compatibility
columns are evaluation metadata, not features. Collector receipt order and
receipt time govern the active interval and its bounded missing-end timeout;
`event_at_utc` remains device-clock trace metadata.

Analytics derives window-level evaluation labels as follows:

1. A valid K1 window is `normal`.
2. For K2-K4, a target-device window fully contained in the actual anomaly phase
   is `anomaly`.
3. A window for an unaffected control device is `normal`.
4. A window crossing the actual start or end boundary is `transition` and is
   excluded from primary metrics.
5. A window with fewer than three canonical telemetry rows is
   `insufficient_data` and is reported separately.
6. A run missing its end event remains open and is excluded or repaired by an
   explicit, auditable rule; it is never silently treated as complete.

Scenario names and `label` are joined to feature windows only for evaluation.
They must not be present in the array or DataFrame passed to `fit()`,
`decision_function()`, `score_samples()`, or `predict()`.

## 10. Controlled data-generation scenarios

| Code | Scenario | Reference behavior | Expected feature effect |
|---|---|---|---|
| K1 | Normal | One publication per 5 seconds with uniform ±0.5-second jitter | Stable rate and inter-arrival statistics |
| K2 | Message burst | Exactly 5 publications/second for the 60-second campaign phase | Higher count, rate, and one-second peak |
| K3 | Irregular interval | Alternating 1-second and 9-second gaps for 60 seconds | Higher inter-arrival variation |
| K4 | Payload size shift | Add exactly 1,024 ASCII diagnostic bytes for 60 seconds | Higher payload mean and possibly standard deviation |

K2-K4 are controlled communication changes in an isolated lab. They are not
proof of a successful attack, intrusion, malware infection, or botnet activity.
Hard traffic, duration, and payload bounds protect the broker and make runs
repeatable.

Every run records the run and request IDs, scenario, target devices, seed,
configuration, requested duration, actual start/end boundaries, and completion
state. Unaffected devices provide normal controls when they are linked to the
same campaign run manifest and actual phase record.

## 11. CSV and dataset products

### 11.1. Implemented Phase 2 exports

The current export CLI writes exactly these RFC 4180 CSV files:

```text
data/exports/
  mqtt_observations.csv
  devices.csv
  scenario_events.csv
```

It exports physical table columns in stable row order and does not silently
clean, impute, relabel, or synthesize values. `schema_metadata` is not exported.
For the interactive lab, the SQLite database under `data/runtime/` remains the
source of truth. For Chapter IV, each integrity-verified per-run database under
`results/runs/{run_id}/run.db` is the immutable source of truth; the live named
volume is transient and is never read by host-side training or evaluation.

### 11.2. Chapter IV campaign products

The implemented export/evaluation pipeline uses this generated structure:

```text
results/
  artifacts/          # model, metadata, checksums, frozen split, train/validation features
  runs/{run_id}/      # raw messages, phases, windows, all predictions, alerts, resources, timing
  evaluation_windows.csv
  metrics_by_run.csv
  metrics_by_scenario.csv
  event_detection.csv
  evaluation_summary.json
  figures/
  screenshots/
```

Large runtime datasets and generated model artifacts are not committed to Git.
The repository commits the exact plan, split manifest, schemas, code, and
reproduction instructions.

## 12. Training, validation, and test design

### 12.1. Splits

- Training uses independent normal runs and fits every learned preprocessing
  step plus Isolation Forest.
- Validation uses different normal runs to select a decision threshold. If model
  hyperparameters use scenario data, those validation runs are recorded.
- Test uses held-out K1-K4 runs that did not influence preprocessing,
  hyperparameters, or the threshold.
- The split unit is the campaign `run_id`; the protocol-level UUID emitted by a
  device identifies one scenario event sequence but does not replace the
  campaign split identity. Overlapping windows from one run never cross subsets.
- A committed or exported manifest records every run assigned to each subset.

### 12.2. Isolation Forest contract

- Fit on the seven ordered features from usable normal training windows.
- Use 100 trees, `max_samples="auto"`, `max_features=1.0`,
  `contamination="auto"`, `bootstrap=false`, and `random_state=42`.
- Use no learned scaling in the frozen starting configuration.
- Use `decision_function`; lower scores mean more anomalous behavior.
- Set `decision_threshold` to the first percentile of normal validation scores
  and freeze it before final test evaluation. The decision is anomalous only
  when `score < threshold`.
- Store model version, feature names/order, feature schema version, window,
  stride, threshold, training run IDs, random seed, and library versions.

### 12.3. Leakage prevention

- Never use labels, run IDs, device IDs, scenario fields, or simulator-only
  values as features.
- Never split overlapping windows from one run across subsets.
- Never tune the threshold after inspecting final test results.
- Do not train directly on CICIoT2023 and apply that model to live MQTT unless
  the exact feature semantics are demonstrably compatible. Otherwise treat the
  public dataset as a separate offline study.

## 13. Evaluation

| Metric | Definition |
|---|---|
| TP | An anomaly-labelled window predicted anomalous |
| FP | A normal-labelled window predicted anomalous |
| TN | A normal-labelled window predicted normal |
| FN | An anomaly-labelled window predicted normal |
| Precision | `TP / (TP + FP)`, with zero-denominator handling reported |
| Recall | `TP / (TP + FN)`, with zero-denominator handling reported |
| F1 | Harmonic mean of precision and recall |
| FPR | `FP / (FP + TN)` |
| Detection latency | Time from actual phase start to the first valid anomalous decision for the correct target/window by `phase_end + 20 s`; otherwise `not_detected` or `not_evaluable` with reason |
| Resource use | CPU and memory for the services during each scenario |

Accuracy, F1, or other favorable metrics must not be claimed before reproducible
runs are executed. Reports include the number of scored, transition, and
insufficient windows and the number of runs with no detection.

The evaluator constructs the expected schedule from all 12 configured devices
and left-joins features, predictions, and ground truth. It reports both scored
coverage (`scored / scheduled`) and primary-evaluation coverage (`eligible /
scheduled`). Undefined metric denominators remain NA with an explicit reason.

## 14. Minimum collection plan

1. Collect the frozen four independent normal runs: two train, one validation,
   and one normal test, all with different seeds.
2. Collect exactly three independent held-out runs for each of K2, K3, and K4
   under the frozen 60/60/60-second phase schedule.
3. Keep feature schema, window, and stride fixed across compared runs.
4. Export raw observations, feature windows, ground truth, split manifests,
   scores, and measured metrics.
5. Plot `message_rate_hz` and anomaly score over time for one or two clear
   examples and produce a confusion matrix from held-out eligible windows.

These counts are an MVP starting point, not a claim of statistical sufficiency.
Additional repetitions should be added when time and resources permit.

## 15. Required data tests

- Reject or safely record invalid UTF-8, malformed JSON, unsupported topics,
  topic/payload identity mismatches, and payloads over 4096 bytes.
- Require explicit UTC timestamps and normalize no device timestamp into the
  receipt-time feature basis.
- Verify UTF-8 byte length with multibyte payload content.
- Verify QoS 1 and application duplicates do not increase inter-arrival or
  feature counts.
- Verify a new session can reuse sequence zero without becoming a duplicate.
- Verify `[start, end)` boundaries count a publication exactly once.
- Verify fewer than three messages yields `insufficient_data`.
- Verify K2 raises rate/peak behavior, K3 changes gap variation, and K4 changes
  payload-size statistics in generated data.
- Verify experiment labels never enter model input columns.
- Verify group splits contain no run ID in more than one subset.
- Verify open runs and transition windows are excluded from primary metrics.
- Verify CSV export preserves the implemented physical values without hidden
  transformations.

## 16. References

- [scikit-learn IsolationForest](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html)
- [MQTT Version 3.1.1, OASIS Standard](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
