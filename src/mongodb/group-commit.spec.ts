import { GroupQueue } from './group-commit';

describe('isolated group queue', () => {
  it.each(['commit', 'unknown'] as const)(
    'prohibits isolation after %s',
    (resolution) => {
      const queue = new GroupQueue<string, string>({
        maxRequests: 1,
        maxEvents: 1,
        maxBytes: 1,
      });
      const member = queue.enqueue('a', 1, 1)!;
      const batch = queue.seal(1);
      const release = batch.retain();
      const attempt = batch.beginAttempt();
      batch.stage(attempt, member, 'ok');
      batch[resolution](attempt);
      expect(() => batch.isolate(attempt, release)).toThrow();
      release();
      expect(queue.retainedBytes).toBe(0);
    },
  );
  it('rejects foreign members and stale attempts without leaking capacity', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const first = queue.enqueue('first', 1, 1)!;
    const other = queue.enqueue('other', 1, 1)!;
    const batch = queue.seal(1);
    const release = batch.retain();
    const old = batch.beginAttempt();
    const attempt = batch.beginAttempt();
    expect(() => batch.isolate(old, release)).toThrow();
    const handoff = batch.isolate(attempt, release);
    expect(() => handoff.settle(other, { kind: 'aborted' })).toThrow();
    expect(() => batch.beginAttempt()).toThrow();
    expect(() => batch.commit(attempt)).toThrow();
    handoff.settle(first, { kind: 'aborted' });
    release();
    expect(queue.retainedBytes).toBe(1);
    expect(queue.cancel(other)).toBe(true);
    expect(queue.retainedBytes).toBe(0);
  });
  it('preserves settled members through cancellation and shutdown while retaining the owner', async () => {
    jest.useFakeTimers();
    try {
      const queue = new GroupQueue<string, string>({
        maxRequests: 2,
        maxEvents: 2,
        maxBytes: 2,
      });
      const controller = new AbortController();
      const remove = jest.spyOn(controller.signal, 'removeEventListener');
      const first = queue.enqueue('first', 1, 1, {
        signal: controller.signal,
        deadline: Date.now() + 50,
      })!;
      const second = queue.enqueue('second', 1, 1)!;
      const batch = queue.seal(2);
      const release = batch.retain();
      const attempt = batch.beginAttempt();
      const handoff = batch.isolate(attempt, release);
      handoff.settle(first, { kind: 'error', error: 'deleted' });
      expect(() => handoff.settle(first, { kind: 'aborted' })).toThrow();
      expect(() => release()).toThrow('pending');
      controller.abort();
      expect(queue.cancel(second)).toBe(false);
      const shutdown = queue.shutdown(10);
      jest.advanceTimersByTime(10);
      await shutdown;
      expect(first.outcome).toEqual({ kind: 'error', error: 'deleted' });
      expect(second.outcome).toEqual({ kind: 'unknown' });
      expect(() =>
        handoff.settle(second, { kind: 'committed', value: 'late' }),
      ).toThrow();
      expect(queue.retainedBytes).toBe(2);
      release();
      release();
      expect(queue.retainedBytes).toBe(0);
      expect(queue.activeGroups).toBe(0);
      expect(remove).toHaveBeenCalled();
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });
  it('requires the actual owner to authorize the abort handoff', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    queue.enqueue('a', 1, 1);
    const batch = queue.seal(1);
    const release = batch.retain();
    const attempt = batch.beginAttempt();
    expect(() => {
      batch.isolate(attempt, () => undefined);
    }).toThrow('owner');
    const handoff = batch.isolate(attempt, release);
    handoff.settle(batch.members[0], { kind: 'aborted' });
    release();
    expect(queue.retainedBytes).toBe(0);
  });
  it('hands off a tombstone error and a healthy result after confirmed abort', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const dead = queue.enqueue('dead', 1, 1)!;
    const healthy = queue.enqueue('healthy', 1, 1)!;
    const batch = queue.seal(2);
    const release = batch.retain();
    const attempt = batch.beginAttempt();
    batch.stage(attempt, healthy, 'rolled back');
    const handoff = batch.isolate(attempt, release);
    const error = new Error('stream deleted');
    handoff.settle(dead, { kind: 'error', error });
    expect(dead.outcome).toEqual({ kind: 'error', error });
    expect(healthy.outcome).toBeUndefined();
    expect(queue.retainedBytes).toBe(2);
    handoff.settle(healthy, { kind: 'committed', value: 'independent' });
    expect(healthy.outcome).toEqual({
      kind: 'committed',
      value: 'independent',
    });
    expect(queue.retainedBytes).toBe(2);
    release();
    expect(queue.retainedBytes).toBe(0);
    expect(queue.activeGroups).toBe(0);
  });
  it('times out multiple active groups and rejects late settlements', async () => {
    jest.useFakeTimers();
    try {
      const queue = new GroupQueue<string, string>({
        maxRequests: 3,
        maxEvents: 3,
        maxBytes: 3,
        maxActiveGroups: 2,
        maxRetainedBytes: 3,
      });
      const first = queue.enqueue('first', 1, 1)!;
      const batch1 = queue.seal(1);
      const attempt1 = batch1.beginAttempt();
      const second = queue.enqueue('second', 1, 1)!;
      const batch2 = queue.seal(1);
      const attempt2 = batch2.beginAttempt();
      const queued = queue.enqueue('queued', 1, 1)!;
      const shutdown = queue.shutdown(10);
      expect(queued.state).toBe('cancelled');
      batch1.abort(attempt1);
      jest.advanceTimersByTime(10);
      await shutdown;
      expect(first.outcome).toEqual({ kind: 'aborted' });
      expect(second.outcome).toEqual({ kind: 'unknown' });
      expect(() => batch1.abort(attempt1)).toThrow();
      expect(() => batch2.abort(attempt2)).toThrow();
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });
  it.each(['abort', 'unknown'] as const)(
    'retains capacity across retry and releases it exactly once on %s',
    (outcome) => {
      const queue = new GroupQueue<string, string>({
        maxRequests: 2,
        maxEvents: 2,
        maxBytes: 2,
        maxActiveGroups: 1,
        maxRetainedBytes: 1,
      });
      const member = queue.enqueue('first', 1, 1)!;
      const batch = queue.seal(1);
      const old = batch.beginAttempt();
      batch.stage(old, member, 'stale');
      const attempt = batch.beginAttempt();
      expect(queue.enqueue('blocked', 1, 1)).toBeUndefined();
      expect(() => batch[outcome](old)).toThrow();
      expect(queue.enqueue('still blocked', 1, 1)).toBeUndefined();
      batch[outcome](attempt);
      const next = queue.enqueue('next', 1, 1)!;
      expect(() => batch[outcome](attempt)).toThrow();
      expect(queue.enqueue('over capacity', 1, 1)).toBeUndefined();
      expect(queue.seal(1).members).toEqual([next]);
    },
  );
  it('releases queued bytes on cancellation and rejects oversized requests', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 10,
      maxRetainedBytes: 2,
    });
    expect(queue.enqueue('oversized', 1, 3)).toBeUndefined();
    const member = queue.enqueue('cancel', 1, 2)!;
    expect(queue.cancel(member)).toBe(true);
    expect(queue.cancel(member)).toBe(false);
    expect(queue.enqueue('replacement', 1, 2)).toBeDefined();
  });
  it('counts queued and sealed bytes until the active group settles', () => {
    const limits = {
      maxRequests: 3,
      maxEvents: 3,
      maxBytes: 10,
      maxRetainedBytes: 5,
    };
    const queue = new GroupQueue<string, string>(limits);
    const first = queue.enqueue('first', 1, 3)!;
    const batch = queue.seal(1);
    const second = queue.enqueue('second', 1, 2)!;
    expect(queue.enqueue('too much', 1, 1)).toBeUndefined();
    const attempt = batch.beginAttempt();
    batch.stage(attempt, first, 'ok');
    batch.commit(attempt);
    expect(queue.enqueue('fits', 1, 3)).toBeDefined();
    expect(second.state).toBe('queued');
  });
  it('seals distinct streams and preserves duplicate requests for the next group', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 4,
      maxEvents: 4,
      maxBytes: 4,
      maxActiveGroups: 1,
    });
    const a = queue.enqueue('a', 1, 1)!;
    const duplicate = queue.enqueue('a', 1, 1)!;
    const b = queue.enqueue('b', 1, 1)!;
    const batch = queue.seal(3, (value) => value);
    expect(batch.members).toEqual([a, b]);
    expect(duplicate.state).toBe('queued');
    const attempt = batch.beginAttempt();
    batch.stage(attempt, a, 'a');
    batch.stage(attempt, b, 'b');
    batch.commit(attempt);
    expect(queue.seal(3).members).toEqual([duplicate]);
  });

  it('keeps queued work FIFO when the sealed-group capacity is full', () => {
    const limits = {
      maxRequests: 3,
      maxEvents: 3,
      maxBytes: 3,
      maxActiveGroups: 1,
      maxRetainedBytes: 3,
    };
    const queue = new GroupQueue<string, string>(limits);
    const first = queue.enqueue('first', 1, 1)!;
    const active = queue.seal(1);
    const second = queue.enqueue('second', 1, 1)!;
    const third = queue.enqueue('third', 1, 1)!;
    expect(() => queue.seal(1)).toThrow('group capacity');
    expect(second.state).toBe('queued');
    const attempt = active.beginAttempt();
    active.stage(attempt, first, 'done');
    active.commit(attempt);
    expect(queue.seal(2).members).toEqual([second, third]);
  });
  it('drains shutdown immediately on settlement and clears its timer', async () => {
    jest.useFakeTimers();
    try {
      const queue = new GroupQueue<string, string>({
        maxRequests: 1,
        maxEvents: 1,
        maxBytes: 1,
      });
      const member = queue.enqueue('a', 1, 1)!;
      const batch = queue.seal(1);
      const attempt = batch.beginAttempt();
      batch.stage(attempt, member, 'done');
      const shutdown = queue.shutdown(100);
      expect(queue.shutdown(100)).toBe(shutdown);
      batch.commit(attempt);
      await shutdown;
      expect(member.outcome).toEqual({ kind: 'committed', value: 'done' });
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });

  it('excludes deadlines expired at seal even before timer callback runs', () => {
    jest.useFakeTimers();
    try {
      const queue = new GroupQueue<string, string>({
        maxRequests: 2,
        maxEvents: 2,
        maxBytes: 2,
      });
      const member = queue.enqueue('expired', 1, 1, {
        deadline: Date.now() + 10,
      })!;
      jest.setSystemTime(Date.now() + 10);
      expect(queue.seal(2).members).toEqual([]);
      expect(member.state).toBe('cancelled');
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });
  it('owns cancellation options even if caller mutates the options object', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    const first = new AbortController();
    const remove = jest.spyOn(first.signal, 'removeEventListener');
    const second = new AbortController();
    const options = { signal: first.signal };
    const member = queue.enqueue('a', 1, 1, options)!;
    options.signal = second.signal;
    first.abort();
    expect(member.state).toBe('cancelled');
    expect(remove).toHaveBeenCalled();
  });
  it('owns a snapshot of admission limits', () => {
    const limits = { maxRequests: 1, maxEvents: 1, maxBytes: 1 };
    const queue = new GroupQueue(limits);
    limits.maxRequests = 100;
    limits.maxEvents = 100;
    limits.maxBytes = 100;
    queue.enqueue('first', 1, 1);
    expect(queue.enqueue('second', 1, 1)).toBeUndefined();
  });
  it('bounds shutdown and classifies unresolved sealed work as unknown', async () => {
    jest.useFakeTimers();
    try {
      const queue = new GroupQueue<string, string>({
        maxRequests: 2,
        maxEvents: 2,
        maxBytes: 2,
      });
      const sealed = queue.enqueue('sealed', 1, 1)!;
      const batch = queue.seal(1);
      const attempt = batch.beginAttempt();
      const queued = queue.enqueue('queued', 1, 1)!;
      const shutdown = queue.shutdown(10);
      expect(queued.state).toBe('cancelled');
      expect(queue.enqueue('late', 1, 1)).toBeUndefined();
      jest.advanceTimersByTime(10);
      await shutdown;
      expect(sealed.outcome).toEqual({ kind: 'unknown' });
      expect(() => batch.commit(attempt)).toThrow();
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });
  it('cancels expired or aborted queued members and releases listeners at seal', () => {
    jest.useFakeTimers();
    try {
      const queue = new GroupQueue<string, string>({
        maxRequests: 3,
        maxEvents: 3,
        maxBytes: 3,
      });
      const expired = queue.enqueue('expired', 1, 1, {
        deadline: Date.now() + 10,
      })!;
      const controller = new AbortController();
      const remove = jest.spyOn(controller.signal, 'removeEventListener');
      const aborted = queue.enqueue('aborted', 1, 1, {
        signal: controller.signal,
      })!;
      controller.abort();
      jest.advanceTimersByTime(10);
      expect(expired.state).toBe('cancelled');
      expect(aborted.state).toBe('cancelled');
      const live = new AbortController();
      const removeLive = jest.spyOn(live.signal, 'removeEventListener');
      const sealed = queue.enqueue('sealed', 1, 1, { signal: live.signal })!;
      expect(queue.seal(3).members).toEqual([sealed]);
      live.abort();
      expect(sealed.state).toBe('sealed');
      expect(remove).toHaveBeenCalled();
      expect(removeLive).toHaveBeenCalled();
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });
  it('prevents callers from changing membership or state', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    const member = queue.enqueue('a', 1, 1)!;
    expect(() => {
      Object.assign(member, { state: 'settled' });
    }).toThrow();
    const batch = queue.seal(1);
    expect(() => {
      Array.prototype.pop.call(batch.members);
    }).toThrow();
    expect(() => {
      Object.assign(batch, { members: [] });
    }).toThrow();
    expect(member.state).toBe('sealed');
    expect(batch.members).toEqual([member]);
  });
  it('seals a bounded batch in admission order', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 2,
      maxEvents: 3,
      maxBytes: 10,
    });
    const first = queue.enqueue('a', 2, 4);
    const second = queue.enqueue('b', 1, 6);
    expect(queue.enqueue('c', 1, 1)).toBeUndefined();
    expect(queue.seal(2).members).toEqual([first, second]);
  });

  it('removes cancelled queued requests but keeps sealed requests until their outcome', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 2,
      maxEvents: 2,
      maxBytes: 2,
    });
    const queued = queue.enqueue('a', 1, 1)!;
    expect(queue.cancel(queued)).toBe(true);
    expect(queued.state).toBe('cancelled');
    const sealed = queue.enqueue('b', 1, 1)!;
    const batch = queue.seal(2);
    expect(queue.cancel(sealed)).toBe(false);
    expect(batch.members).toEqual([sealed]);
    expect(sealed.state).toBe('sealed');
  });

  it('discards retry results and publishes only the acknowledged attempt once', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    const member = queue.enqueue('a', 1, 1)!;
    const batch = queue.seal(1);
    const first = batch.beginAttempt();
    batch.stage(first, member, 'stale');
    const second = batch.beginAttempt();
    expect(() => batch.stage(first, member, 'late')).toThrow();
    batch.stage(second, member, 'committed');
    expect(member.outcome).toBeUndefined();
    batch.commit(second);
    expect(member.outcome).toEqual({ kind: 'committed', value: 'committed' });
    Object.assign(batch, { done: false });
    expect(() => batch.beginAttempt()).toThrow();
    expect(() => batch.commit(second)).toThrow();
  });

  it('settles an unknown commit without replaying or reporting staged success', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    const member = queue.enqueue('a', 1, 1)!;
    const batch = queue.seal(1);
    const attempt = batch.beginAttempt();
    batch.stage(attempt, member, 'maybe');
    batch.unknown(attempt);
    expect(member.outcome).toEqual({ kind: 'unknown' });
    expect(() => batch.beginAttempt()).toThrow();
    expect(() => batch.commit(attempt)).toThrow();
  });

  it('rejects invalid bounds and invalid request sizes', () => {
    expect(
      () => new GroupQueue({ maxRequests: 0, maxEvents: 1, maxBytes: 1 }),
    ).toThrow();
    const queue = new GroupQueue({ maxRequests: 1, maxEvents: 1, maxBytes: 1 });
    expect(() => queue.enqueue('bad', -1, 0)).toThrow();
    expect(() => queue.enqueue('bad', 1, NaN)).toThrow();
    expect(() => queue.seal(0)).toThrow();
  });

  it('classifies a definitive abort separately from an unknown commit', () => {
    const queue = new GroupQueue<string, string>({
      maxRequests: 1,
      maxEvents: 1,
      maxBytes: 1,
    });
    const member = queue.enqueue('a', 1, 1)!;
    const batch = queue.seal(1);
    const attempt = batch.beginAttempt();
    batch.abort(attempt);
    expect(member.outcome).toEqual({ kind: 'aborted' });
    expect(() => batch.beginAttempt()).toThrow();
  });
});
