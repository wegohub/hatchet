import { getRunQueryRetryOptions } from './run-query-retry';
import { QueryClient } from '@tanstack/react-query';
import assert from 'node:assert/strict';
import { test } from 'node:test';

const notFound = Object.assign(new Error('run not found'), { status: 404 });

test('a triggered run stays pending until its analytics record is visible', async () => {
  const client = new QueryClient();
  const states: string[] = [];
  const unsubscribe = client.getQueryCache().subscribe((event) => {
    states.push(event.query.state.status);
  });
  let attempts = 0;

  try {
    const result = await client.fetchQuery({
      queryKey: ['delayed-run'],
      ...getRunQueryRetryOptions(true, () => false),
      queryFn: async () => {
        if (++attempts <= 3) {
          throw notFound;
        }
        return { status: 'COMPLETED' };
      },
    });

    assert.equal(result.status, 'COMPLETED');
    assert.equal(attempts, 4);
    assert.ok(states.includes('pending'));
    assert.ok(!states.includes('error'));
  } finally {
    unsubscribe();
    client.clear();
  }
});

test('a triggered run missing beyond the retry window becomes an error', async () => {
  const client = new QueryClient();
  let attempts = 0;

  try {
    await assert.rejects(
      client.fetchQuery({
        queryKey: ['missing-triggered-run'],
        ...getRunQueryRetryOptions(true, () => false),
        queryFn: async () => {
          attempts++;
          throw notFound;
        },
      }),
      notFound,
    );
    assert.equal(attempts, 11);
    assert.equal(
      client.getQueryState(['missing-triggered-run'])?.status,
      'error',
    );
  } finally {
    client.clear();
  }
});

test('ordinary missing runs and authorization failures are not retried', async () => {
  const client = new QueryClient();

  try {
    for (const [redirected, loaded, status] of [
      [false, false, 404],
      [true, true, 404],
      [true, false, 401],
      [true, false, 403],
    ] as const) {
      let attempts = 0;
      const error = Object.assign(new Error('request failed'), { status });
      await assert.rejects(
        client.fetchQuery({
          queryKey: ['unavailable-run', redirected, loaded, status],
          ...getRunQueryRetryOptions(redirected, () => loaded),
          queryFn: async () => {
            attempts++;
            throw error;
          },
        }),
        error,
      );
      assert.equal(attempts, 1);
    }
  } finally {
    client.clear();
  }
});
