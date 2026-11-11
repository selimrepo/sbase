import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: Number(__ENV.VUS || 25),
  duration: __ENV.DURATION || '24h',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1200', 'p(99)<2500'],
  },
};
const base = __ENV.SBASE_URL || 'http://localhost:8080';
const token = __ENV.SBASE_TOKEN;
if (!token) throw new Error('Set SBASE_TOKEN to a scoped staging token');
export default function () {
  const response = http.get(`${base}/api/datasets`, { headers: { Authorization: `Bearer ${token}` } });
  check(response, { 'API responds': r => r.status === 200 });
  sleep(1);
}
