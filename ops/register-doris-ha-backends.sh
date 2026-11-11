#!/usr/bin/env bash
set -Eeuo pipefail
: "${DORIS_HOST:=doris-fe}"
: "${DORIS_PORT:=9030}"
: "${DORIS_USER:=root}"
: "${DORIS_PASSWORD:=}"
MYSQL=(mysql --protocol=TCP -h "$DORIS_HOST" -P "$DORIS_PORT" -u"$DORIS_USER")
if [[ -n "$DORIS_PASSWORD" ]]; then MYSQL+=("-p$DORIS_PASSWORD"); fi
for host in 172.30.0.11 172.30.0.14 172.30.0.15; do
  if ! "${MYSQL[@]}" -N -e "SHOW BACKENDS" | grep -Fq "$host"; then
    "${MYSQL[@]}" -e "ALTER SYSTEM ADD BACKEND '$host:9050'"
  fi
done
for attempt in $(seq 1 90); do
  output=$("${MYSQL[@]}" -N -e "SHOW BACKENDS" 2>/dev/null || true)
  alive=0
  for host in 172.30.0.11 172.30.0.14 172.30.0.15; do
    if grep -Fq "$host" <<<"$output"; then alive=$((alive+1)); fi
  done
  if [[ "$alive" -eq 3 ]]; then
    echo "All three Doris backends are registered. Verify Alive=true and tablet replicas before use."
    exit 0
  fi
  sleep 5
done
"${MYSQL[@]}" -e "SHOW BACKENDS" || true
echo "Timed out waiting for all three Doris backends" >&2
exit 1
