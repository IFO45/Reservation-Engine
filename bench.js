import http from 'k6/http';
import { Counter } from 'k6/metrics';

// Configured by run_bench.py through environment variables.
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const EVENT_ID = __ENV.EVENT_ID || 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';
const VARIANT = __ENV.VARIANT || 'engine';
const SCENARIO = __ENV.SCENARIO || 'burst';
const OUT = __ENV.OUT || 'k6_summary.json';

const PATHS = {
  naive: '/bench/naive/hold',
  pg: '/bench/pg/hold',
  engine: '/api/reservations/hold',
};

const SCENARIOS = {
  // Correctness: 1,000 requests, up to 200 in flight, for 20 seats.
  burst: { executor: 'shared-iterations', vus: 200, iterations: 1000, maxDuration: '60s' },
  // Throughput when almost every request must be rejected (20 seats).
  soldout: { executor: 'constant-vus', vus: 100, duration: '15s' },
  // Throughput when every request succeeds (1,000,000 seats).
  stock: { executor: 'constant-vus', vus: 100, duration: '15s' },
};

export const options = {
  scenarios: { run: SCENARIOS[SCENARIO] },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'max'],
};

// 201 (granted) and 409 (sold out) are both expected outcomes, not failures.
http.setResponseCallback(http.expectedStatuses(201, 409));

const granted = new Counter('hold_201');
const rejected = new Counter('hold_409');
const other = new Counter('hold_other');

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

export default function () {
  const res = http.post(
    `${BASE_URL}${PATHS[VARIANT]}`,
    JSON.stringify({ event_id: EVENT_ID, user_id: uuid(), seats: 1 }),
    { headers: { 'Content-Type': 'application/json' } },
  );

  if (res.status === 201) granted.add(1);
  else if (res.status === 409) rejected.add(1);
  else other.add(1);
}

export function handleSummary(data) {
  return { [OUT]: JSON.stringify(data) };
}
