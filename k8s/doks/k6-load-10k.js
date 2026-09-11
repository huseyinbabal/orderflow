import http from 'k6/http';
import { check } from 'k6';

// TOTAL target rate. k6-operator (parallelism=N) splits this across N runner pods
// via execution segments — do NOT pre-divide here.
const TOTAL_RPS = Number(__ENV.TOTAL_RPS || 10000);
const SUSTAIN   = __ENV.SUSTAIN || '10m';

export const options = {
  scenarios: {
    ten_k: {
      executor: 'ramping-arrival-rate',
      timeUnit: '1s',
      startRate: Math.ceil(TOTAL_RPS / 10),
      preAllocatedVUs: 900,
      maxVUs: 3000,
      stages: [
        { target: Math.ceil(TOTAL_RPS / 4), duration: '20s' },
        { target: TOTAL_RPS,                duration: '40s' },
        { target: TOTAL_RPS,                duration: SUSTAIN },
        { target: 0,                        duration: '20s' },
      ],
    },
  },
  thresholds: {
    http_req_failed:   ['rate<0.02'],
    http_req_duration: ['p(95)<2000'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
};

const URL = 'http://orderflow.default.svc.cluster.local:8080/orders?broker=kafka';

export default function () {
  const payload = JSON.stringify({
    orderId: `o-${__VU}-${__ITER}`,
    customerId: `c-${Math.floor(Math.random() * 100000)}`,
    amount: +(Math.random() * 500).toFixed(2),
  });
  const res = http.post(URL, payload, { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'status 200': (r) => r.status === 200 });
}
