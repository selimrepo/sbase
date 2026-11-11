import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '2m', target: 20 },
    { duration: '5m', target: 100 },
    { duration: '3m', target: 200 },
    { duration: '2m', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.02'],
    http_req_duration: ['p(95)<1500', 'p(99)<3000'],
  },
};
const base = __ENV.SBASE_URL || 'http://localhost:8080';
const token = __ENV.SBASE_TOKEN;
if (!token) throw new Error('Set SBASE_TOKEN to a scoped staging token');
export default function () {
  const response = http.get(`${base}/api/datasets`, { headers: { Authorization: `Bearer ${token}` } });
  check(response, { 'API response is not server error': r => r.status < 500 });
  sleep(0.2);
}
