import http from 'k6/http';
import { check, sleep } from 'k6';
const base = (__ENV.SBASE_BASE_URL || 'http://127.0.0.1:8080').replace(/\/$/, '');
const path = __ENV.SBASE_PERF_PATH || '/actuator/health/readiness';
const token = __ENV.SBASE_BEARER_TOKEN || '';
const headers = token ? { Authorization: \`Bearer \${token}\` } : {};
export const options = {
  stages: [
    { duration: __ENV.K6_RAMP_UP || '2m', target: Number(__ENV.K6_VUS || 20) },
    { duration: __ENV.K6_HOLD || '5m', target: Number(__ENV.K6_VUS || 20) },
    { duration: '1m', target: 0 },
  ],
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<500', 'p(99)<1500'], checks: ['rate>0.99'] },
};
export default function () {
  const response = http.get(\`\${base}\${path}\`, { headers, tags: { endpoint: path } });
  check(response, { 'successful response': (r) => r.status >= 200 && r.status < 300, 'no server error': (r) => r.status < 500 });
  sleep(1);
}
