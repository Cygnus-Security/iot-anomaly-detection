# Security model for the isolated lab

- The broker rejects anonymous access and loads salted password hashes generated
  at container startup from environment secrets.
- Explicit per-device ACL blocks restrict telemetry/status/event writes and
  command reads to the username's own topic namespace. Collector and operator
  accounts do not inherit device write permissions.
- The `integration-test` identity is restricted to its matching `test/` topic;
  the production collector does not subscribe to that namespace.
- `collector` can read IoT messages but cannot publish device data or alerts.
  `operator` can issue commands and read status plus experiment
  acknowledgements. The implemented analytics service reads the run-isolated
  SQLite database and writes local derived tables; it has no MQTT credential or
  broker publish permission.
- Ports 1883 and 8501 are exposed on loopback only. No public broker is used
  for load scenarios.
- `.env`, SQLite files, exports, broker data/logs, models, and generated
  `results/` evidence are ignored by Git.
- A joblib artifact is loaded only from the explicit controlled campaign path
  after SHA-256 and feature/window compatibility checks. Joblib files from an
  untrusted source must never be loaded because deserialization can execute
  code.
- This MVP uses one shared lab password across device identities to keep ESP32
  provisioning simple. Production must issue unique device secrets, enable TLS,
  rotate credentials, and use a device registry or authentication plugin.
- Broker logs are security-relevant but may contain client metadata. They are
  runtime artifacts and are not committed.
