#!/usr/bin/env bash
set -Eeuo pipefail
BASE_URL="\${SBASE_BASE_URL:-http://127.0.0.1:8080}"; BASE_URL="\${BASE_URL%/}"
TIMEOUT="\${SBASE_CURL_TIMEOUT_SECONDS:-10}"; fail=0
check() {
  local name="$1" url="$2" expected="\${3:-200}" code
  code="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' --max-time "$TIMEOUT" "$url" 2>/dev/null || true)"
  if [[ "$code" == "$expected" ]]; then printf 'PASS %-28s HTTP %s\n' "$name" "$code"
  else printf 'FAIL %-28s expected HTTP %s, got %s\n' "$name" "$expected" "\${code:-no response}"; fail=1; fi
}
check liveness "$BASE_URL/actuator/health/liveness"
check readiness "$BASE_URL/actuator/health/readiness"
if [[ -n "\${SBASE_CHECK_API_PATH:-}" ]]; then check "configured API endpoint" "$BASE_URL\${SBASE_CHECK_API_PATH}" "\${SBASE_CHECK_API_EXPECTED_STATUS:-200}"; fi
exit "$fail"
