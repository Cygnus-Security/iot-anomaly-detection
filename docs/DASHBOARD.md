# Security dashboard

## Runtime

The Streamlit service is available at `http://127.0.0.1:8501` and opens the
collector SQLite database in read-only mode. Short-lived read connections keep
the collector as the only writer. The main fragment refreshes every five
seconds by default; users can disable refresh or change the interval.

## Views

- **Overview:** active-device count, all-time telemetry, one-minute message
  rate, invalid payloads, experiment count, traffic, ground-truth distribution,
  and the fleet table.
- **Devices:** payload size, inter-arrival time, temperature, humidity,
  sequence number, and recent stored metadata for a selected device.
- **Experiment Scenarios:** K1-K4 runs paired by `scenario_run_id`, including
  configured duration, observed duration, seed, and completion state.
- **AI & Alerts:** reads every persisted decision from `window_predictions`,
  plots the Isolation Forest score against its frozen threshold, and shows the
  traceable `anomaly_alerts` subset. Training/validation databases with no model
  decisions are identified explicitly.

## Metric definitions

- A device is **online** when its last explicit state is online and its latest
  accepted application observation is at most 15 seconds old.
- **Stale** means the device previously reported online but no accepted
  application message has arrived for more than 15 seconds. This is an
  application-level estimate, not the broker TCP-session state.
- **Unknown** means no explicit online/offline state has been stored.
- **60-second rate** is valid, non-duplicate telemetry received during the last
  60 seconds divided by 60.
- **Abnormal scenarios** counts K2-K4 `scenario_started` events. This is
  controlled experiment ground truth, not an ML detection.
- **AI alerts** is the count of real rows in `anomaly_alerts`. Availability is
  determined by the prediction schema; zero means the model produced no alert,
  not that the UI should manufacture one. The UI never substitutes scenario
  labels or rule-based values for model output.

## Phase 3 prediction and alert contracts

`window_predictions` is the source for all valid normal and anomalous model
decisions. It contains the run/session/device/window identity, score, threshold,
predicted label, model and feature-schema versions, inference-call identity,
recording time, and processing delay. `anomaly_alerts` contains only decisions
where `score < threshold`, together with detection time and reason.

The dashboard opens both tables read-only; `src.analytics` owns their writes.
Ground-truth labels are joined only for traceability and evaluation and are not
model inputs. Any schema change must be versioned and updated in the data design,
analytics storage, export pipeline, and dashboard query layer.
