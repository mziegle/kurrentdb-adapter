import test from 'node:test';
import assert from 'node:assert/strict';
import { compareStreams } from '../dist/application/compare.js';

test('compareStreams matches identical streams', () => {
  const event = {
    eventId: '1',
    eventType: 'thing-happened',
    revision: 0n,
    position: 100n,
    data: { a: 1 },
    metadata: { m: 1 },
  };

  const result = compareStreams('s-1', [event], [event]);
  assert.equal(result.matches, true);
  assert.equal(result.mismatches.length, 0);
});

test('compareStreams detects mismatches and ordering differences', () => {
  const result = compareStreams(
    's-2',
    [
      { eventId: 'a', eventType: 'created', revision: 0n, data: { v: 1 } },
      { eventId: 'b', eventType: 'updated', revision: 1n, data: { v: 2 } },
    ],
    [
      { eventId: 'b', eventType: 'updated', revision: 0n, data: { v: 2 } },
      { eventId: 'a', eventType: 'created', revision: 1n, data: { v: 1 } },
    ],
  );

  assert.equal(result.matches, false);
  assert.ok(result.mismatches.length > 0);
  assert.ok(result.mismatches.some((item) => item.reason === 'event_id_mismatch'));
});
