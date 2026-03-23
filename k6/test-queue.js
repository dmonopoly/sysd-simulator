import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const status2xx = new Counter('status_2xx');
const status5xx = new Counter('status_5xx');

const phase = (name, rate, startTime) => ({
  executor: 'constant-arrival-rate',
  rate,
  timeUnit: '1s',
  duration: '45s',
  startTime: startTime || '0s',
  preAllocatedVUs: Math.min(Math.ceil(rate * 2), 1200),
  maxVUs: Math.min(Math.ceil(rate * 6), 3000),
  exec: 'writeQueue',
  tags: { phase: name },
});

export const options = {
  scenarios: {
    rps_50: phase('50rps', 50),
    rps_150: phase('150rps', 150, '45s'),
    rps_300: phase('300rps', 300, '90s'),
    rps_500: phase('500rps', 500, '135s'),
  },
  thresholds: {
    http_req_duration: ['p(95)<5000'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

function buildOrderPayload() {
  return {
    user_id: ((__VU * 1000) + __ITER) % 500000,
    amount: Number((25 + ((__ITER % 20) * 3.75)).toFixed(2)),
    status: 'pending',
  };
}

export function writeQueue() {
  const body = JSON.stringify(buildOrderPayload());
  const res = http.post(`${BASE_URL}/write-queue`, body, {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /write-queue' },
  });
  if (res.status >= 200 && res.status < 300) {
    status2xx.add(1);
  }
  if (res.status >= 500) {
    status5xx.add(1);
  }
  check(res, {
    'status 202': (r) => r.status === 202,
  });
}

export function teardown() {
  console.log(
    '\n=== Post-run reminder ===\nPoll GET /status until database row counts stabilize (queue drain may lag behind request completion).\n'
  );
}
