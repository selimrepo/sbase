#!/usr/bin/env bash
set -Eeuo pipefail
BASE_URL="\${SBASE_BASE_URL:-http://127.0.0.1:8080}"; BASE_URL="\${BASE_URL%/}"
TIMEOUT="\${SBASE_CURL_TIMEOUT_SECONDS:-10}"; fail=0
request_code() { curl --silent --show-error --output /dev/null --write-out '%{http_code}' --max-time "$TIMEOUT" "$@" 2>/dev/null || true; }
report() { local status="$1" name="$2" detail="$3"; printf '%s %-34s %s\n' "$status" "$name" "$detail"; [[ "$status" == PASS ]] || fail=1; }

for key in POSTGRES_PASSWORD SPRING_DATASOURCE_PASSWORD JWT_ISSUER_URI JWT_JWK_SET_URI; do
  value="\${!key:-}"
  if [[ -z "$value" ]]; then report FAIL "$key" "not set"
  elif [[ "$value" =~ ^(sbase|password|changeme|change-me|admin|secret)$ ]]; then report FAIL "$key" "known placeholder"
  else report PASS "$key" "provided"; fi
done

if [[ "$BASE_URL" == https://* ]]; then report PASS "transport security" "HTTPS configured"
elif [[ "\${SBASE_ALLOW_INSECURE_HTTP:-false}" == true ]]; then echo "WARN HTTP allowed for isolated test"
else report FAIL "transport security" "use HTTPS; HTTP allowed only with SBASE_ALLOW_INSECURE_HTTP=true"; fi

for path in /actuator/env /actuator/configprops /actuator/heapdump /actuator/threaddump /actuator/metrics; do
  code="$(request_code "$BASE_URL$path")"
  if [[ "$code" == 401 || "$code" == 403 || "$code" == 404 ]]; then report PASS "anonymous $path" "HTTP $code"
  else report FAIL "anonymous $path" "HTTP \${code:-no response}; expected 401/403/404"; fi
done

if [[ -n "\${SBASE_PROTECTED_PATHS:-}" ]]; then
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    code="$(request_code "$BASE_URL$path")"
    if [[ "$code" == 401 || "$code" == 403 ]]; then report PASS "protected $path" "HTTP $code"
    else report FAIL "protected $path" "HTTP \${code:-no response}; expected 401/403"; fi
  done <<< "$SBASE_PROTECTED_PATHS"
else echo "INFO Set SBASE_PROTECTED_PATHS to newline-separated protected API routes."; fi

headers="$(curl --silent --show-error --head --max-time "$TIMEOUT" "$BASE_URL" 2>/dev/null | tr -d '\r' || true)"
for header in "x-content-type-options: nosniff" "x-frame-options:"; do
  if grep -qi "^$header" <<< "$headers"; then report PASS "header $header" "present"
  else report FAIL "header $header" "missing; configure application or ingress"; fi
done
exit "$fail"
