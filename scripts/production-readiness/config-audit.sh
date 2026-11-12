#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose="$ROOT/docker-compose.yml"
application="$ROOT/src/main/resources/application.yml"
dev_compose="$ROOT/docker-compose.development.yml"
prod_compose="$ROOT/docker-compose.production.yml"
fail=0

check_absent() {
  local pattern="$1" file="$2" label="$3"
  if grep -En "$pattern" "$file"; then
    echo "FAIL $label: unsafe default found in $file"
    fail=1
  else
    echo "PASS $label"
  fi
}

check_present() {
  local pattern="$1" file="$2" label="$3"
  if grep -Eq "$pattern" "$file"; then
    echo "PASS $label"
  else
    echo "FAIL $label: required configuration not found in $file"
    fail=1
  fi
}

check_absent '\$\{POSTGRES_PASSWORD:-[^}]*\}|\$\{SPRING_DATASOURCE_PASSWORD:[^}]*\}' "$compose" "Compose has no default database password fallback"
check_present 'POSTGRES_PASSWORD: \$\{POSTGRES_PASSWORD:\?Set POSTGRES_PASSWORD in [.][e]nv' "$compose" "Compose requires PostgreSQL password from environment"
check_absent 'password:.*\$\{SPRING_DATASOURCE_PASSWORD:[^}]*\}' "$application" "Spring has no default datasource password"
check_present 'password: \$\{SPRING_DATASOURCE_PASSWORD\}' "$application" "Spring datasource password is environment-only"
check_present 'ddl-auto: validate' "$application" "Hibernate schema validation enabled"
check_present 'include: health,info,metrics' "$application" "Actuator exposure is allow-listed"
check_present 'probes:' "$application" "Actuator probe configuration present"
check_present 'SPRING_PROFILES_ACTIVE: development,docker' "$dev_compose" "Development Compose activates development profile"
check_present 'SPRING_PROFILES_ACTIVE: production,docker' "$prod_compose" "Production Compose activates production profile"
check_present 'image: \$\{SBASE_IMAGE:\?Set SBASE_IMAGE' "$prod_compose" "Production requires an explicit image"
check_present 'JWT_ISSUER_URI: \$\{JWT_ISSUER_URI:\?Set JWT_ISSUER_URI' "$prod_compose" "Production requires identity issuer"
check_present 'JWT_JWK_SET_URI: \$\{JWT_JWK_SET_URI:\?Set JWT_JWK_SET_URI' "$prod_compose" "Production requires JWK endpoint"

exit "$fail"
