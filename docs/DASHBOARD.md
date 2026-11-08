# Security dashboard

## Runtime

The Streamlit service is available at `http://127.0.0.1:8501` and reads the
collector database in SQLite read-only mode. It uses short-lived connections so
the collector remains the only writer. The main fragment refreshes every five
seconds by default; users can disable or change the interval in the sidebar.

## Views

- **Tổng quan**: active device count, all-time telemetry, one-minute message
  rate, invalid payloads, experimental scenario count, traffic chart, label
  distribution, and fleet table.
- **Thiết bị**: payload size, inter-arrival, temperature, humidity, sequence,
  and the latest stored message metadata for a selected device.
- **Kịch bản thử nghiệm**: K1-K4 runs paired by `scenario_run_id`, including
  configured duration, observed duration, seed, and completion state.
- **AI & Cảnh báo**: reads the Phase 3 `anomaly_detections` contract when it
  exists. Before that, it reports the missing inference layer and shows K2-K4
  ground truth separately.

## Metric definitions

- A device is **Đang hoạt động** when its latest application observation is at
  most 15 seconds old and its latest explicit status is not offline.
- **Mất tín hiệu** means the device previously reported online but the collector
  has not received an application message for more than 15 seconds. This is not
  broker TCP-session state.
- **Tốc độ 60 giây** is the count of valid telemetry received during the last
  60 seconds divided by 60.
- **Kịch bản bất thường** counts K2-K4 `scenario_started` events. This is
  simulation ground truth, not an ML detection.
- **Cảnh báo AI** is unavailable until actual inference rows exist. The UI does
  not substitute scenario labels or rule-based values for ML output.

## Phase 3 detection contract

When Phase 3 is implemented, the dashboard expects an `anomaly_detections`
table with these fields: `detected_at`, `device_id`, `anomaly_score`,
`is_anomaly`, `model_version`, `feature_window_start`, `feature_window_end`, and
`detection_latency_ms`. The dashboard only reads this table; the inference
service owns writes.
