import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 5,
  duration: '2m',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<750', 'p(99)<1500'],
  },
};

const base = __ENV.SBASE_URL || 'http://localhost:8080';
const token = __ENV.SBASE_TOKEN;
if (!token) throw new Error('Set SBASE_TOKEN to a scoped staging token');

export default function () {
  const response = http.get(`${base}/actuator/health/readiness`);
  check(response, { 'readiness is healthy': r => r.status === 200 });
  const api = http.get(`${base}/api/datasets`, { headers: { Authorization: `Bearer ${token}` } });
  check(api, { 'dataset API responds': r => r.status === 200 });
  sleep(1);
}
