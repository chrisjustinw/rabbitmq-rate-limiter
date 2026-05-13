import { check, sleep } from 'k6';
import http from 'k6/http';

export const options = {
  scenarios: {
    constant_rpm: {
      executor: 'constant-arrival-rate',
      rate: 5000,         // 5000 requests
      timeUnit: '1m',     // per minute
      duration: '3m',     // test duration
      preAllocatedVUs: 10,
      maxVUs: 50,
    },
  },
};

export default function () {
  const url = 'http://localhost:8080/publish';

  const payload = JSON.stringify({
    message: 'Hello, World!',
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