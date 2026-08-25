import http from 'k6/http';
import { check } from 'k6';

// Black Friday profile: warm-up → spike → sustain → cool-down.
// Arrival-rate executor pushes a fixed RPS regardless of response times,
// so a slowing app cannot hide behind fewer requests — lag becomes visible.
export const options = {
  scenarios: {
    black_friday: {
      executor: 'ramping-arrival-rate',
      startRate: 20,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 300,
      stages: [
        { target: 50, duration: '30s' },   // warm-up
        { target: 200, duration: '1m' },   // the spike
        { target: 200, duration: '1m' },   // sustain
        { target: 0, duration: '30s' },    // cool-down
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
  },
};

const URL = 'http://orderflow.default.svc.cluster.local:8080/orders';

export default function () {
  const payload = JSON.stringify({
    orderId: `o-${__VU}-${__ITER}`,
    customerId: `c-${Math.floor(Math.random() * 10000)}`,
    amount: +(Math.random() * 500).toFixed(2),
  });
  const res = http.post(URL, payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status 200': (r) => r.status === 200 });
}
