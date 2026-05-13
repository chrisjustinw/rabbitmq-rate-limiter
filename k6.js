import { check, sleep } from 'k6';
import http from 'k6/http';

export const options = {
  scenarios: {
    constant_rpm: {
      executor: 'constant-arrival-rate',
      rate: 100,         // 100 requests
      timeUnit: '1s',     // per second
      duration: '3m',     // test duration
      preAllocatedVUs: 150,
      maxVUs: 10000,
    },
  },
};

export default function () {
  const url = 'http://localhost:8080/publish';

  const payload = JSON.stringify({
    body: 'Hello, World!',
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  sleep(1);
}