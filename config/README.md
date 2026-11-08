# Configuration

Runtime configuration is environment based so the same images can be used by
Compose and future hardware clients. `.env.example` documents every Phase 2
setting; `scripts/bootstrap_env.sh` creates a local `.env` with random secrets.

Message and topic contracts are specified in `docs/MQTT_PROTOCOL.md`. No secret
configuration belongs in this directory.
