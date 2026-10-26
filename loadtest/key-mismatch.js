import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://localhost:8080';
const apiKey = __ENV.LATCHKEY_API_KEY;
const runId = __ENV.RUN_ID || `${Date.now()}`;
const sku = __ENV.SKU || 'demo-sku';
const outcomes = new Counter('latchkey_mismatch_outcomes');
const latency = new Trend('latchkey_mismatch_latency', true);

export const options = {
  scenarios: {
    mismatch: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 50),
      duration: __ENV.DURATION || '30s',
    },
  },
  thresholds: {
    http_req_duration: [`p(95)<${__ENV.P95_LIMIT_MS || 1000}`],
    checks: ['rate>0.95'],
  },
};

export default function () {
  if (!apiKey) throw new Error('Set LATCHKEY_API_KEY to a seeded tenant key');

  const body = JSON.stringify({ sku, quantity: 1 + ((__VU + __ITER) % 5) });
  const response = http.post(`${baseUrl}/v1/orders`, body, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
      'Idempotency-Key': `k6-mismatch-${runId}`,
    },
  });
  latency.add(response.timings.duration);
  const allowed = [201, 409, 422].includes(response.status);
  outcomes.add(1, { status: String(response.status) });
  check(response, {
    'mismatch response is winner, in-flight, or key-reuse conflict': () => allowed,
  });
}
