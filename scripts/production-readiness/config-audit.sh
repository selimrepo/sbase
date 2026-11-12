#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "\${BASH_SOURCE[0]}")/../.." && pwd)"
compose="$ROOT/docker-compose.yml"
application="$ROOT/src/main/resources/application.yml"
fail=0
check_absent() {
  local pattern="$1" file="$2" label="$3"
  if grep -En "$pattern" "$file"; then echo "FAIL $label: unsafe default found in $file"; fail=1
  else echo "PASS $label"; fi
}
check_present() {
  local pattern="$1" file="$2" label="$3"
  if grep -Eq "$pattern" "$file"; then echo "PASS $label"
  else echo "FAIL $label: required configuration not found in $file"; fail=1; fi
}
check_absent 'POSTGRES_PASSWORD:-sbase|SPRING_DATASOURCE_PASSWORD:sbase' "$compose" "Compose has no default database password"
check_absent 'password:.*\$\{SPRING_DATASOURCE_PASSWORD:sbase\}' "$application" "Spring has no default datasource password"
check_present 'ddl-auto: validate' "$application" "Hibernate schema validation enabled"
check_present 'include: health,info,metrics' "$application" "Actuator exposure is allow-listed"
check_present 'probes:[[:space:]]*$' "$application" "Actuator probe configuration present"
exit "$fail"
