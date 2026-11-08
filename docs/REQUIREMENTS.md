# System requirements and MVP scope

## Product goal

IoT Anomaly Detection System is a research prototype that observes real MQTT
traffic produced by virtual IoT devices. Phase 1 defines the system; Phase 2
provides the authenticated MQTT lab, device simulation, raw observation store,
CSV export, and repeatable scenario controls. The live monitoring dashboard is
implemented over that store. Machine-learning features, training, inference,
and evaluation remain Phase 3 and Phase 5 deliverables.

## Phase 1-2 functional requirements

| ID | Requirement | Verification |
|---|---|---|
| FR-01 | Run at least 10 independently identified MQTT clients | Broker log and collector device table |
| FR-02 | Publish telemetry and status; subscribe to device commands | Integration test and live database |
| FR-03 | Authenticate every MQTT client and enforce role ACLs | Negative and positive integration tests |
| FR-04 | Persist accepted MQTT observations in SQLite | Unit test and SQL readiness query |
| FR-05 | Record payload size and telemetry inter-arrival time | Storage unit tests and CSV export |
| FR-06 | Manually trigger K1-K4 with duration and seed | Scenario controller acknowledgement |
| FR-07 | Record scenario start/end and ground-truth labels | `scenario_events` and observation rows |
| FR-08 | Export raw observations, devices, and scenario events to CSV | Export CLI |
| FR-09 | Start the lab with Docker Compose | Compose validation and smoke test |
| FR-10 | Keep the simulator interface replaceable by an ESP32 publisher | MQTT protocol specification |

## Non-functional requirements

- Configuration and credentials come from environment variables.
- MQTT is bound to localhost and anonymous access is disabled.
- Data contracts are versioned and timestamps use UTC ISO 8601.
- Components log operational errors and reconnect where practical.
- Scenario traffic is limited to the isolated Docker lab.
- Runtime data, secrets, and generated models are not committed.

## Scope boundaries

Phase 2 does not claim that a burst or unusual payload is an attack. It records
the configured behavioral scenario as experimental ground truth. It also does
not infer broker-side connection attempts or rejected publishes from subscriber
traffic. Those events exist only in Mosquitto logs in this MVP.

## Acceptance gate for Phase 2

The phase passes when Compose starts a healthy authenticated broker, collector,
and simulator; at least ten devices appear in SQLite; accepted telemetry is
stored; K1-K4 can be acknowledged and labelled; CSV export works; and unit plus
MQTT integration tests pass.
