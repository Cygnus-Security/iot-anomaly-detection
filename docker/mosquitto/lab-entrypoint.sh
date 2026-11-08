#!/bin/sh
set -eu

: "${DEVICE_PASSWORD:?DEVICE_PASSWORD is required}"
: "${COLLECTOR_PASSWORD:?COLLECTOR_PASSWORD is required}"
: "${OPERATOR_PASSWORD:?OPERATOR_PASSWORD is required}"

device_count="${DEVICE_COUNT:-12}"
case "$device_count" in
  ''|*[!0-9]*)
    echo "DEVICE_COUNT must be a positive integer" >&2
    exit 64
    ;;
esac
if [ "$device_count" -lt 1 ] || [ "$device_count" -gt 999 ]; then
  echo "DEVICE_COUNT must be between 1 and 999" >&2
  exit 64
fi

runtime_dir=/mosquitto/runtime
password_file="$runtime_dir/passwords"
acl_file="$runtime_dir/acl"
mkdir -p "$runtime_dir" /mosquitto/data /mosquitto/log

mosquitto_passwd -b -c "$password_file" collector "$COLLECTOR_PASSWORD"
mosquitto_passwd -b "$password_file" operator "$OPERATOR_PASSWORD"
mosquitto_passwd -b "$password_file" integration-test "$DEVICE_PASSWORD"

i=1
while [ "$i" -le "$device_count" ]; do
  device_id="$(printf 'device-%03d' "$i")"
  mosquitto_passwd -b "$password_file" "$device_id" "$DEVICE_PASSWORD"
  i=$((i + 1))
done

{
  printf '%s\n' 'pattern write iot/%u/telemetry'
  printf '%s\n' 'pattern write iot/%u/status'
  printf '%s\n' 'pattern read iot/%u/command'
  printf '%s\n' 'pattern write test/%u/telemetry'
  printf '%s\n' 'user collector'
  printf '%s\n' 'topic read iot/+/telemetry'
  printf '%s\n' 'topic read iot/+/status'
  printf '%s\n' 'topic read test/+/telemetry'
  printf '%s\n' 'topic read $SYS/#'
  printf '%s\n' 'topic write security/alerts'
  printf '%s\n' 'user operator'
  printf '%s\n' 'topic write iot/+/command'
  printf '%s\n' 'topic read iot/+/status'
} > "$acl_file"

chown -R mosquitto:mosquitto "$runtime_dir" /mosquitto/data /mosquitto/log
chmod 0600 "$password_file"
chmod 0640 "$acl_file"

exec mosquitto -c /mosquitto/config/mosquitto.conf
