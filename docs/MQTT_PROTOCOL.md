# MQTT protocol and message contracts

**Contract version:** 1.0

**Last updated:** 2026-11-11

**Applies to:** the Python IoT simulator, Mosquitto, the MQTT collector, the
scenario controller, and a future ESP32 implementation

## 1. Purpose and scope

This document is the normative communication contract between IoT devices and
the analysis system. A physical device can replace a simulated device without
requiring changes to the collector or feature pipeline when it implements this
contract exactly.

The lab uses MQTT 3.1.1 over TCP, UTF-8 JSON objects, and UTC timestamps. The
simulator publishes live messages through Mosquitto; a CSV replay is not a
substitute for the broker path. Telemetry, availability state, control commands,
and experiment ground truth use separate topics so devices never publish labels
inside telemetry. Collector-side compatibility labels remain evaluation metadata
and the implemented feature pipeline explicitly excludes them from the
seven-value model vector.

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** express requirement
strength in this document.

## 2. Connection and security contract

| Setting | Contract | Notes |
|---|---|---|
| MQTT protocol | 3.1.1 | Every application client uses the same protocol version |
| Broker address | `mosquitto:1883` inside Compose | Host access is bound to `127.0.0.1:1883` by default |
| Transport | TCP on the private Compose network | The lab listener is not exposed to the public Internet |
| Keepalive | 30 seconds | Configurable only if all runtime documentation is updated consistently |
| Authentication | Username and password | Anonymous access is disabled |
| Authorization | Mosquitto ACL | Permissions are least-privilege and topic-specific |
| Device client ID | Equal to `device_id` | It must be unique among connected clients |
| Application QoS | 1 | Delivery is at least once; application deduplication is mandatory |
| Character encoding | UTF-8 | Payloads are JSON objects, not arrays or scalar values |

Each device has its own broker username and uses the same value as its client ID.
The isolated MVP may provision distinct usernames with a shared lab-only device
password. A deployment outside the lab MUST use unique device secrets, secure
provisioning, firewall rules, and TLS.

The MQTT client ID and the `device_id` field do not authenticate a message by
themselves. Identity is established by broker authentication and ACLs, then
checked again by matching the authenticated topic identity to the JSON field.

## 3. Identifiers and time

- `device_id` is stable and matches
  `^[a-z0-9][a-z0-9_-]{2,31}$`; the default fleet is `iot-001` through
  `iot-012`.
- `device_type` is application metadata such as `environment_sensor` or
  `vibration_sensor`.
- `session_id` is a UUID generated once whenever a device process starts.
- `sequence_no` starts at `0`, is bounded by signed 64-bit storage, and
  increments once for every telemetry publication attempted within a session.
- `message_id`, when present, is a UUID for diagnostic tracing. It is not the
  canonical deduplication key.
- `request_id`, `event_id`, and `experiment_run_id` are UUID strings.
- `schema_version` is the string `"1.0"` for every v1 application payload.
- All application timestamps are RFC 3339/ISO 8601 UTC strings ending in `Z`,
  for example `2026-11-08T02:00:00.000000Z`.

Collector receipt time is authoritative for rate and inter-arrival features.
Device timestamps are retained for traceability and clock-drift diagnostics.

## 4. Topic and ACL contract

| Topic | Publisher | Subscriber | QoS | Retained | Purpose |
|---|---|---|---:|---:|---|
| `iot/v1/devices/{device_id}/telemetry` | matching device | collector | 1 | No | Sensor values and communication observations |
| `iot/v1/devices/{device_id}/status` | matching device or its Last Will | collector, operator | 1 | Yes | Latest online/offline availability state |
| `iot/v1/devices/{device_id}/command` | operator | matching device | 1 | No | Finite K1-K4 scenario control |
| `experiments/v1/devices/{device_id}/events` | matching device | collector, operator | 1 | No | Scenario start/end acknowledgement and ground truth |
| `security/v1/alerts` | optional future broker publisher | authorized consumers | 1 | No | Not used by the current analytics path; the dashboard reads SQLite |

The collector subscribes to:

- `iot/v1/devices/+/telemetry`
- `iot/v1/devices/+/status`
- `experiments/v1/devices/+/events`

The generated Mosquitto ACL contains an explicit block for every provisioned
device username and its matching `{device_id}` topic segment. A device can
publish only its own telemetry, status, and experiment events, and can read only
its own command topic. The operator can publish commands and read experiment
acknowledgements. The collector can read observation topics but cannot issue
device commands.

Broker health-check namespaces, integration-test topics, and `$SYS/#` are
operational exceptions. They are not part of the application contract and MUST
NOT enter the research dataset.

Only valid, non-duplicate telemetry publications contribute to ML features.
Status, retained deliveries, commands, and experiment events are excluded from
telemetry counts and feature windows.

## 5. QoS, retention, ordering, and size

QoS 1 permits redelivery. The broker DUP flag is diagnostic metadata only; the
collector MUST detect application duplicates using:

```text
(device_id, session_id, sequence_no)
```

The first valid telemetry publication for a key is canonical. Later rows with
the same key MAY be retained for diagnostics but MUST be marked duplicate and
excluded from inter-arrival calculations, rates, feature windows, training, and
inference.

Telemetry, commands, experiment events, and any optional future MQTT alerts are
non-retained.
Status is retained so a new subscriber can obtain the latest reported state.
A retained `online` message is historical state, not proof that the device is
connected when it is viewed. Receiving a retained replay MUST NOT reset
`last_seen` to the replay instant; freshness uses live observations and the
status publisher timestamp.

Publications from one device use one MQTT session. Scenario start/end events and
telemetry therefore preserve publisher order at the broker. The collector still
orders persisted data by collector receipt time and a stable row ID, not by the
device clock.

The maximum accepted application payload is **4096 bytes**, measured as the
length of the MQTT payload byte array received by the collector. It does not
include MQTT, TCP, IP, or link-layer headers. The collector MUST apply the limit
before JSON parsing; the broker SHOULD use a compatible packet-size limit. JSON
nesting deeper than 64 container levels is invalid and is recorded as bounded
diagnostic metadata for telemetry/status.

## 6. Telemetry payload schema v1

Example:

```json
{
  "schema_version": "1.0",
  "message_id": "7b01b841-8d5f-4976-8868-e682f28a15f0",
  "device_id": "iot-001",
  "device_type": "environment_sensor",
  "session_id": "29ab4b25-c9ad-4f11-86b3-dabfb06a02cc",
  "sequence_no": 42,
  "sent_at_utc": "2026-11-08T02:00:00.000000Z",
  "measurements": {
    "temperature_c": 28.4,
    "relative_humidity_pct": 65.2
  },
  "firmware_version": "sim-1.0",
  "uptime_seconds": 120.5
}
```

| Field | Required | Type | Constraint |
|---|---:|---|---|
| `schema_version` | Yes | String | Exactly `"1.0"` |
| `device_id` | Yes | String | Valid ID and equal to the topic identity |
| `device_type` | Yes | String | Non-empty configured device profile |
| `session_id` | Yes | String | UUID for the current process session |
| `sequence_no` | Yes | Integer | `0` through `9223372036854775807`, monotonically increasing per session |
| `sent_at_utc` | Yes | String | Explicit UTC RFC 3339 timestamp ending in `Z` |
| `measurements` | Yes | Object | Non-empty; numeric sensor values must be finite |
| `message_id` | No | String | UUID used only for tracing |
| `firmware_version` | No | String | Simulator or firmware build identifier |
| `uptime_seconds` | No | Number | Finite and at least `0` |

Additional measurement fields are allowed for other sensor profiles. They do
not change the required communication metadata.

K4 adds a bounded `diagnostic_blob` inside `measurements`. The frozen campaign
implementation uses exactly 1,024 ASCII bytes. The entire serialized payload
must remain within 4,096 bytes.

Telemetry MUST NOT contain `scenario`, `experiment_run_id`, `scenario_label`,
`ground_truth`, `ground_truth_label`, `label`, `is_anomaly`, `attack_type`, or
any other evaluation-only value. The validator applies this rule recursively.

## 7. Availability status and Last Will schema v1

Example:

```json
{
  "schema_version": "1.0",
  "device_id": "iot-001",
  "session_id": "29ab4b25-c9ad-4f11-86b3-dabfb06a02cc",
  "state": "online",
  "sent_at_utc": "2026-11-08T01:59:55.000000Z"
}
```

Required fields are `schema_version`, `device_id`, `session_id`, `state`, and
`sent_at_utc`. Valid states are `online` and `offline` only.

- Before connecting, a device configures a retained QoS 1 Last Will containing
  `offline` for the current session.
- After a successful connection and command subscription, it publishes retained
  `online`.
- While connected, it may refresh retained `online` at the configured heartbeat
  interval using a new `sent_at_utc` value; no third status state is introduced.
- During controlled shutdown, it publishes retained `offline`, waits for the
  QoS 1 publication to complete, and then disconnects.
- During unexpected loss, Mosquitto publishes the retained Last Will.

A Last Will payload is prepared before the failure occurs, so its device-provided
timestamp cannot identify the exact disconnect instant. Collector
`received_at_utc` is the observation time for an unexpected offline event.
Retained status is monitoring data and never contributes to telemetry features.
For a historical retained replay, liveness uses the earlier of its publisher and
collector timestamps so a future-skewed device clock cannot poison later state.

## 8. Scenario command schema v1

Example:

```json
{
  "schema_version": "1.0",
  "command": "set_scenario",
  "request_id": "c410f117-93c7-40ca-9147-e57fa4088e83",
  "scenario": "abnormal_payload",
  "duration_seconds": 30.0,
  "seed": 42
}
```

| Field | Required | Constraint |
|---|---:|---|
| `schema_version` | Yes | Exactly `"1.0"` |
| `command` | Yes | Exactly `set_scenario` |
| `request_id` | Yes | UUID and idempotency key |
| `scenario` | Yes | One of the four values below |
| `duration_seconds` | Yes | Number from 1 through 3600 |
| `seed` | Yes | Signed 64-bit integer used for repeatable randomness |

Allowed scenarios are `normal` (K1), `message_burst` (K2),
`abnormal_interval` (K3), and `abnormal_payload` (K4). A duplicate command with
an already processed `request_id` MUST return or republish the existing
acknowledgement and MUST NOT create another run.

## 9. Experiment event schema v1

Example start event:

```json
{
  "schema_version": "1.0",
  "event_id": "dd377e16-826a-43aa-84f8-8d7684680053",
  "device_id": "iot-001",
  "session_id": "29ab4b25-c9ad-4f11-86b3-dabfb06a02cc",
  "event_at_utc": "2026-11-08T02:05:00.000000Z",
  "event": "scenario_started",
  "scenario": "message_burst",
  "experiment_run_id": "2a38c44f-3d39-467b-a3bf-a2850f3717b8",
  "request_id": "c410f117-93c7-40ca-9147-e57fa4088e83",
  "seed": 42,
  "duration_seconds": 30.0,
  "details": {
    "target_rate_hz": 5.0,
    "publish_interval_seconds": 0.2
  }
}
```

Valid event names are `scenario_started` and `scenario_ended`. Every event
requires a unique `event_id`, the device session, scenario, run ID, request ID,
seed, configured duration, and an object-valued `details` field. The matching end
event reuses the same experiment and request IDs and may record a completion
reason in `details`.

The device publishes `scenario_started` before the first affected telemetry and
`scenario_ended` before returning to normal telemetry. These events provide
ground truth for evaluation, but they are never joined into the feature vector
used by `fit()`, `predict()`, or real-time scoring.

The collector preserves `event_at_utc` for traceability but uses collector
receipt order and receipt time to activate and expire its raw compatibility
labels. This prevents device clock skew from extending or suppressing a run.

## 10. Device and collector flow

1. The device loads configuration, generates a session ID, and initializes its
   sequence at zero.
2. It creates an MQTT 3.1.1 client with its assigned identity and retained Last
   Will.
3. It connects with authentication, verifies CONNACK, subscribes to its command
   topic, and verifies SUBACK.
4. Only after SUBACK, it publishes retained `online` status.
5. It publishes one telemetry object per configured cycle and increments the
   sequence after each publish attempt.
6. A command starts a finite scenario; the device emits ordered start/end events
   on the experiment topic.
7. The collector timestamps, size-checks, parses, validates, deduplicates, and
   persists each broker-forwarded publication.
8. Controlled shutdown publishes retained `offline`; unexpected loss activates
   the broker Last Will.

For every telemetry publication, the collector records at least the original
topic, topic-derived device ID, collector receipt time, device timestamp,
payload byte length, session, sequence, QoS, retained and broker-DUP flags,
validation result, and bounded raw payload text.

## 11. Observation boundary

The collector is an MQTT subscriber, not a packet sniffer or broker audit-log
reader. It observes only publications that Mosquitto accepts and forwards to its
subscriptions. It cannot observe rejected authentication, denied publishes,
CONNECT packets, TCP session state, MQTT packets sent to other subscriptions, or
network-layer byte counts.

Mosquitto logs are the separate source for connection, authentication, and ACL
events. They are not silently merged into the Phase 2 dataset. Consequently,
claims and feature names must describe application-level MQTT publications, not
network packets or complete broker activity.

## 12. Error handling

| Condition | Required behavior |
|---|---|
| Broker unavailable | Retry with bounded backoff, log the outage, and never pretend a publish succeeded |
| Authentication or ACL failure | Report the rejection clearly; do not disable security to continue |
| Invalid UTF-8 or JSON on telemetry/status | Record bounded diagnostic metadata, mark invalid, and exclude from features |
| Invalid experiment event | Reject and log it; persisted bounded diagnostics remain an identified Phase 2 gap |
| Unsupported topic | Ignore and log; do not create an implicit device |
| Topic and payload identity mismatch | Mark invalid and exclude from normal feature processing |
| Payload over 4096 bytes | Reject before parsing, record size/source diagnostics, and do not persist an unbounded body |
| QoS 1 redelivery | Mark the application duplicate and exclude it from inter-arrival and feature counts |
| Out-of-order sequence | Preserve receipt order, record the anomaly for diagnostics, and do not reorder by device time |
| Collector queue overflow | Increment and log the process-local dropped count; treat the interval as an observation gap |
| Missing scenario end event | Leave the run visibly open and exclude or repair it explicitly during dataset preparation |
| Broker or collector restart | Reconnect; represent the unobserved interval as a data gap |

## 13. ESP32 compatibility

A replacement ESP32 must provide Wi-Fi or Ethernet, an MQTT 3.1.1 client, its
assigned username/client ID, the versioned topics, QoS and retention policies,
the same JSON schemas, session/sequence handling, UTC time, and a retained Last
Will. The collector and feature extractor then remain unchanged.

Replacing the simulator does not guarantee that an existing model generalizes
to the physical device. If its normal communication profile differs materially,
new normal runs must be collected for training and threshold calibration.

## 14. References

- [MQTT Version 3.1.1, OASIS Standard](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
- [Eclipse Paho Python client](https://eclipse.dev/paho/files/paho.mqtt.python/html/)
- [Eclipse Mosquitto authentication methods](https://mosquitto.org/documentation/authentication-methods/)
