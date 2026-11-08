#!/bin/sh
set -eu

if [ -e .env ]; then
  echo ".env already exists; refusing to overwrite it" >&2
  exit 1
fi
if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required to generate lab credentials" >&2
  exit 1
fi

umask 077
device_password="$(openssl rand -hex 24)"
collector_password="$(openssl rand -hex 24)"
operator_password="$(openssl rand -hex 24)"

{
  printf '%s\n' 'MQTT_HOST=mosquitto'
  printf '%s\n' 'MQTT_PORT=1883'
  printf '%s\n' 'MQTT_HOST_PORT=1883'
  printf '%s\n' 'MQTT_KEEPALIVE=60'
  printf 'DEVICE_PASSWORD=%s\n' "$device_password"
  printf 'COLLECTOR_PASSWORD=%s\n' "$collector_password"
  printf 'OPERATOR_PASSWORD=%s\n' "$operator_password"
  printf '%s\n' 'DEVICE_COUNT=12'
  printf '%s\n' 'DEVICE_BASE_INTERVAL_SECONDS=2.0'
  printf '%s\n' 'DEVICE_INTERVAL_JITTER_SECONDS=0.2'
  printf '%s\n' 'DEVICE_HEARTBEAT_SECONDS=30.0'
  printf '%s\n' 'SIMULATION_SEED=20261108'
  printf '%s\n' 'SQLITE_PATH=/app/data/runtime/iot_anomaly.db'
  printf '%s\n' 'COLLECTOR_QUEUE_SIZE=10000'
  printf '%s\n' 'LOG_LEVEL=INFO'
} > .env

echo "Created .env with random lab credentials (mode 600)."
