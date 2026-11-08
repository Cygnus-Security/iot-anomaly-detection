# Data collection and persistence design

## SQLite tables

### `mqtt_observations`

One row per accepted telemetry or status publication forwarded to the
collector. Important fields are collector receipt time (ISO and Unix), device,
topic, type, payload bytes/text, publisher timestamp, sequence, QoS, retain,
validation state/error, telemetry inter-arrival seconds, ground-truth label,
and scenario run ID.

Inter-arrival is calculated only between valid telemetry rows for the same
device. It is null for the first observed sample after an empty database.

### `devices`

Latest observed status per device: online flag, first/last seen, last telemetry,
last status event, and last sequence. This is observed application state, not a
claim about the broker's TCP session table.

### `scenario_events`

One row per `scenario_started` or `scenario_ended` status event. Run ID, request
ID, device, scenario, event time, seed, duration, and JSON details preserve the
experiment ground truth.

## Labelling rules

The collector maintains an active scenario map from ordered device status
events. Telemetry between the start and matching end event receives that
scenario and run ID. Otherwise it receives label `normal` and a null run ID.
Status rows keep their associated event label but are not ML feature inputs.

Because start/end status and telemetry share one MQTT client and use QoS 1, the
broker preserves publisher order. Collector receipt timestamps remain the
authoritative timing source. Abrupt device loss can leave a run open; this is
visible as a missing end event and must be excluded or repaired explicitly in
later dataset preparation.

## CSV export

The export CLI writes one RFC 4180 CSV per table. It does not silently transform
or synthesize values. Raw SQLite remains the source of truth.
