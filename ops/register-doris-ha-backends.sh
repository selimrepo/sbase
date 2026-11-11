#!/usr/bin/env bash
set -Eeuo pipefail
: "${DORIS_HOST:=doris-fe}"
: "${DORIS_PORT:=9030}"
: "${DORIS_USER:=root}"
: "${DORIS_PASSWORD:=}"
MYSQL=(mysql --protocol=TCP -h "$DORIS_HOST" -P "$DORIS_PORT" -u"$DORIS_USER")
if [[ -n "$DORIS_PASSWORD" ]]; then MYSQL+=("-p$DORIS_PASSWORD"); fi
until "${MYSQL[@]}" -e "SELECT 1" >/dev/null 2>&1; do sleep 5; done
for host in 172.30.0.11 172.30.0.14 172.30.0.15; do
  if ! "${MYSQL[@]}" -N -e "SHOW BACKENDS" | grep -Fq "$host"; then
    "${MYSQL[@]}" -e "ALTER SYSTEM ADD BACKEND '$host:9050'"
  fi
done
for attempt in $(seq 1 90); do
  output=$("${MYSQL[@]}" -N -e "SHOW BACKENDS" 2>/dev/null || true)
  alive=0
  for host in 172.30.0.11 172.30.0.14 172.30.0.15; do
    if awk -F '\t' -v host="$host" '($2==host && tolower($10)=="true"){found=1} END{exit !found}' <<<"$output"; then
      alive=$((alive+1))
    fi
  done
  if [[ "$alive" -eq 3 ]]; then
    "${MYSQL[@]}" -e 'ADMIN SET FRONTEND CONFIG ("default_replication_num" = "3")'
    echo "All three Doris backends are alive; default replication factor set to three."
    exit 0
  fi
  sleep 5
done
"${MYSQL[@]}" -e "SHOW BACKENDS" || true
echo "Timed out waiting for all three Doris backends" >&2
exit 1
