# MQTT protocol and message contracts

## Broker and identity

- Protocol: MQTT 5 over TCP inside the Compose network.
- Listener: container port 1883, bound to `127.0.0.1` on the host.
- Device username/client ID: `device-NNN`.
- Service identities: `collector` and `operator`.
- All application messages use QoS 1 and are non-retained.

## Topics and authorization

| Topic | Publisher | Subscriber | Purpose |
|---|---|---|---|
| `iot/{device_id}/telemetry` | matching device | collector | Sensor sample and communication observation source |
| `iot/{device_id}/status` | matching device | collector, operator | Online/heartbeat/scenario acknowledgement |
| `iot/{device_id}/command` | operator | matching device | Scenario control |
| `security/alerts` | collector (Phase 3) | dashboard (Phase 4) | Future ML alerts |

Mosquitto `%u` ACL patterns ensure a device can access only topics whose
`device_id` equals its username. The collector cannot issue device commands.

## Telemetry schema v1

```json
{
  "schema_version": "1.0",
  "message_id": "7b01b841-8d5f-4976-8868-e682f28a15f0",
  "device_id": "device-001",
  "timestamp": "2026-11-08T09:00:00.000000Z",
  "sequence": 42,
  "sensor": {"temperature_c": 24.3, "humidity_percent": 57.1},
  "device": {"firmware": "sim-1.0", "uptime_seconds": 120.5}
}
```

`padding` may be added only by K4 to alter payload size. It is opaque to the
collector. Device ID in the payload must match the topic identity.

## Status schema v1

```json
{
  "schema_version": "1.0",
  "message_id": "dd377e16-826a-43aa-84f8-8d7684680053",
  "device_id": "device-001",
  "timestamp": "2026-11-08T09:00:00.000000Z",
  "event": "scenario_started",
  "scenario": "message_burst",
  "scenario_run_id": "2a38c44f-3d39-467b-a3bf-a2850f3717b8",
  "request_id": "c410f117-93c7-40ca-9147-e57fa4088e83",
  "seed": 42,
  "duration_seconds": 30.0,
  "details": {"burst_multiplier": 12.0}
}
```

Valid events are `online`, `offline`, `heartbeat`, `scenario_started`, and
`scenario_ended`. Scenario-specific fields are omitted from ordinary status.

## Command schema v1

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

Allowed scenarios are `normal` (K1), `message_burst` (K2),
`abnormal_interval` (K3), and `abnormal_payload` (K4). Duration must be 1-3600
seconds. Seed is an integer and makes scenario randomness repeatable.
