import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://localhost:8080';
const apiKey = __ENV.LATCHKEY_API_KEY;
const runId = __ENV.RUN_ID || `${Date.now()}`;
const sku = __ENV.SKU || 'demo-sku';
const quantity = Number(__ENV.QUANTITY || 1);
const outcomes = new Counter('latchkey_unique_key_outcomes');
const latency = new Trend('latchkey_unique_key_latency', true);

export const options = {
  scenarios: {
    unique_keys: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 20),
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

  const response = http.post(
    `${baseUrl}/v1/orders`,
    JSON.stringify({ sku, quantity }),
    {
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'Idempotency-Key': `k6-unique-${runId}-${__VU}-${__ITER}`,
      },
    },
  );
  latency.add(response.timings.duration);
  outcomes.add(1, { status: String(response.status) });
  check(response, { 'unique-key request creates an order': (r) => r.status === 201 });
}
