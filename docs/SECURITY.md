# Security model for the isolated lab

- The broker rejects anonymous access and loads salted password hashes generated
  at container startup from environment secrets.
- Per-device ACL patterns restrict telemetry/status writes and command reads to
  the username's own topic namespace.
- The `integration-test` identity is restricted to its matching `test/` topic;
  the production collector does not subscribe to that namespace.
- `collector` can read IoT messages and will gain only the alert publication
  permission needed by Phase 3. `operator` can issue commands and read status.
- Ports 1883 and 8501 are exposed on loopback only. No public broker is used
  for load scenarios.
- `.env`, SQLite files, exports, broker data/logs, and models are ignored by Git.
- This MVP uses one shared lab password across device identities to keep ESP32
  provisioning simple. Production must issue unique device secrets, enable TLS,
  rotate credentials, and use a device registry or authentication plugin.
- Broker logs are security-relevant but may contain client metadata. They are
  runtime artifacts and are not committed.
