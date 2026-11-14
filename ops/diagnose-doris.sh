#!/usr/bin/env bash
# Diagnose the local SBase Doris FE/BE path without deleting volumes or changing cluster state.
set -u

failures=0

section() { printf '\n==== %s ====\n' "$1"; }
run() {
  "$@"
  status=$?
  if [ "$status" -ne 0 ]; then
    printf 'Command failed (exit %s):' "$status"
    printf ' %q' "$@"
    printf '\n'
    failures=$((failures + 1))
  fi
  return 0
}

section "Compose services"
run docker compose ps -a

section "Docker engine resources"
run docker info --format 'OS={{.OperatingSystem}} Architecture={{.Architecture}} CPUs={{.NCPU}} Memory={{.MemTotal}}'

section "Doris host kernel prerequisites"
if [ -r /proc/sys/vm/max_map_count ]; then
  map_count="$(cat /proc/sys/vm/max_map_count)"
  printf 'vm.max_map_count=%s (Doris BE requires at least 2000000)\n' "$map_count"
  if [ "$map_count" -lt 2000000 ]; then
    echo "WARNING: vm.max_map_count is below the recommended Doris BE minimum."
    failures=$((failures + 1))
  fi
else
  echo "Host vm.max_map_count is not visible from this shell; verify it in the Docker Engine Linux VM."
fi
if [ -r /proc/swaps ]; then
  if [ "$(awk 'NR > 1 { count++ } END { print count + 0 }' /proc/swaps)" -gt 0 ]; then
    echo "WARNING: swap is enabled in this Linux environment; Doris BE expects swap to be disabled."
    cat /proc/swaps
    failures=$((failures + 1))
  else
    echo "Swap is disabled in this Linux environment."
  fi
else
  echo "Host swap state is not visible from this shell; verify it in the Docker Engine Linux VM."
fi

section "Doris container health"
for service in doris-fe doris-be doris-init; do
  id="$(docker compose ps -q "$service" 2>/dev/null || true)"
  if [ -z "$id" ]; then
    echo "$service: container not found (was the default Compose stack started?)"
    failures=$((failures + 1))
    continue
  fi
  docker inspect --format "$service: state={{.State.Status}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}not-configured{{end}} exit={{.State.ExitCode}}" "$id"
done

section "Doris FE/BE/init logs"
run docker compose logs --no-color --timestamps --tail=160 doris-fe doris-be doris-init

section "FE backend registration and real tablet scan"
fe_id="$(docker compose ps -q doris-fe 2>/dev/null || true)"
network=""
if [ -n "$fe_id" ]; then
  network="$(docker inspect --format '{{range $name, $cfg := .NetworkSettings.Networks}}{{printf "%s" $name}}{{end}}' "$fe_id" 2>/dev/null || true)"
fi
if [ -z "$network" ]; then
  echo "Could not resolve the Compose network for doris-fe; SQL diagnostics skipped."
  failures=$((failures + 1))
else
  echo "Using Compose network: $network"
  mysql() {
    docker run --rm --network "$network" -e MYSQL_PWD="${DORIS_ROOT_PASSWORD:-}" mysql:8.4 \
      mysql --protocol=TCP --connect-timeout=5 -h doris-fe -P 9030 -uroot "$@"
  }
  run mysql -e 'SHOW FRONTENDS\\G'
  run mysql -e 'SHOW BACKENDS\\G'

  probe="sbase_diag_$(date +%s)_$$"
  cleanup_probe() {
    mysql -e "DROP TABLE IF EXISTS analytics.$probe" >/dev/null 2>&1 || true
  }
  trap cleanup_probe EXIT
  if mysql -e "CREATE TABLE analytics.$probe (id BIGINT NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\", \"storage_medium\"=\"HDD\")"; then
    echo "Temporary Doris table creation succeeded; checking a real BE scan."
    run mysql -e "SELECT COUNT(*) FROM analytics.$probe"
    cleanup_probe
    trap - EXIT
  else
    echo "ERROR: Doris could not place a tablet on an available backend."
    failures=$((failures + 1))
    cleanup_probe
    trap - EXIT
  fi
fi

section "Result"
if [ "$failures" -eq 0 ]; then
  echo "No diagnostic failures were detected. Review the backend row and logs above if the application still reports scanNode unavailable."
else
  echo "$failures diagnostic check(s) need attention. Preserve Doris volumes while investigating; this script does not delete or reset data."
fi
exit 0
