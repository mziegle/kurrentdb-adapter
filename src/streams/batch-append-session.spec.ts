import { EventEmitter } from 'node:events';
import { BatchAppendReq, BatchAppendResp } from '../interfaces/streams';
import { BatchAppendSession } from './batch-append-session';
import { StreamsController } from './streams.controller';
import * as sizing from '../mongodb/request-size';
// eslint-disable-next-line @typescript-eslint/no-require-imports
import Long = require('long');

const controller = new StreamsController({} as never, {} as never);
const correlationKey = (id: BatchAppendReq['correlationId']): string =>
  controller['getBatchAppendCorrelationKey'](id);

class FakeBatchAppendCall extends EventEmitter {
  destroyed = false;
  ended = false;
  readonly writes: BatchAppendResp[] = [];
  destroyError: Error | undefined;
  writeError: Error | undefined;

  constructor() {
    super();
    this.on('error', () => this.end());
  }

  write(
    response: BatchAppendResp,
    callback: (error?: Error | null) => void,
  ): void {
    if (this.writeError) {
      callback(this.writeError);
      return;
    }

    this.writes.push(response);
    callback(null);
  }

  end(): void {
    this.ended = true;
  }

  destroy(error?: Error): void {
    this.destroyed = true;
    this.destroyError = error;
    this.emit('close');
  }
}

function createBatchAppendRequest(
  correlationId: string,
  eventType: string,
  isFinal: boolean,
): BatchAppendReq {
  return {
    correlationId: { string: correlationId },
    options: {
      streamIdentifier: {
        streamName: Buffer.from(`stream-${correlationId}`, 'utf8'),
      },
      noStream: {},
    },
    proposedMessages: [
      {
        id: { string: `${correlationId}-${eventType}` },
        metadata: {
          type: eventType,
          'content-type': 'application/json',
        },
        customMetadata: new Uint8Array(),
        data: Buffer.from(`{"type":"${eventType}"}`, 'utf8'),
      },
    ],
    isFinal,
  };
}

function createBatchAppendResponse(correlationId: string): BatchAppendResp {
  return {
    correlationId: { string: correlationId },
    streamIdentifier: {
      streamName: Buffer.from(`stream-${correlationId}`, 'utf8'),
    },
    noStream: {},
    success: {
      currentRevision: 0,
      position: {
        commitPosition: 1,
        preparePosition: 1,
      },
    },
  };
}

function createDeferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });

  return { promise, resolve };
}

async function flushAsyncWork(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

describe('BatchAppendSession', () => {
  it('fails closed on a malformed rejection stream identifier', () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    const request = createBatchAppendRequest('bad-name', 'event', true);
    request.options!.streamIdentifier!.streamName = { byteLength: 5 } as never;
    request.proposedMessages[0].data = Buffer.alloc(17 * 1024 * 1024);
    expect(() => session.onData(request)).not.toThrow();
    expect(call.ended).toBe(true);
    expect(call.writes).toHaveLength(0);
  });
  it('fails closed without queuing a rejection when the shared budget cannot cover it', () => {
    const spy = jest.spyOn(sizing, 'requestSize');
    const sessions: BatchAppendSession[] = [];
    const make = (call = new FakeBatchAppendCall()) => {
      const session = new BatchAppendSession(
        call as never,
        async () => {},
        (id) => id?.string ?? '',
        (e) => e as Error,
      );
      sessions.push(session);
      return session;
    };
    for (let i = 0; i < 4; i++) {
      const request = createBatchAppendRequest(String(i), 'event', false);
      request.proposedMessages[0].data = Buffer.alloc(15 * 1024 * 1024);
      make().onData(request);
    }
    const probe = make();
    spy.mockClear();
    probe.onData(createBatchAppendRequest('probe', 'event', false));
    const remaining = spy.mock.calls[1][1];
    probe.onClose();
    const filler = createBatchAppendRequest('filler', 'event', false);
    filler.options!.streamIdentifier!.streamName = new Uint8Array(1);
    filler.proposedMessages[0].customMetadata = new Uint8Array(0);
    filler.proposedMessages[0].data = new Uint8Array(0);
    const overhead = sizing.requestSize(filler, Infinity, true);
    filler.proposedMessages[0].data = new Uint8Array(
      remaining - overhead - 512,
    );
    const fillSession = make();
    fillSession.onData(filler);
    expect(fillSession['pendingMessages'].size).toBe(1);
    const call = new FakeBatchAppendCall();
    const rejectedSession = make(call);
    const rejected = createBatchAppendRequest('rejected', 'event', true);
    rejected.options!.streamIdentifier!.streamName = new Uint8Array(4096);
    rejected.proposedMessages[0].data = Buffer.alloc(17 * 1024 * 1024);
    rejectedSession.onData(rejected);
    expect(call.ended).toBe(true);
    expect(call.writes).toHaveLength(0);
    expect(rejectedSession['rejectionEntries'].size).toBe(0);
    sessions.forEach((session) => session.onClose());
    spy.mockRestore();
  });
  it('drops queued rejections on close but keeps an in-flight response charged until callback', async () => {
    const spy = jest.spyOn(sizing, 'requestSize');
    const probe = () => {
      const peer = new BatchAppendSession(
        new FakeBatchAppendCall() as never,
        async () => {},
        (id) => id?.string ?? '',
        (e) => e as Error,
      );
      spy.mockClear();
      peer.onData(createBatchAppendRequest('probe', 'event', false));
      const limit = spy.mock.calls[1][1];
      peer.onClose();
      return limit;
    };
    const baseline = probe();
    const calls = [new FakeBatchAppendCall(), new FakeBatchAppendCall()];
    const callbacks: ((error?: Error | null) => void)[] = [];
    const sessions = calls.map((call) => {
      jest.spyOn(call, 'write').mockImplementation((_response, callback) => {
        callbacks.push(callback);
      });
      return new BatchAppendSession(
        call as never,
        async () => {},
        (id) => id?.string ?? '',
        (e) => e as Error,
      );
    });
    const payload = Buffer.alloc(17 * 1024 * 1024);
    for (const session of sessions) {
      for (let i = 0; i < 64; i++) {
        const request = createBatchAppendRequest(String(i), 'event', true);
        request.options!.streamIdentifier!.streamName = new Uint8Array(4096);
        request.proposedMessages[0].data = payload;
        session.onData(request);
      }
    }
    await flushAsyncWork();
    expect(callbacks).toHaveLength(2);
    expect(baseline - probe()).toBeGreaterThan(128 * 4096);
    sessions.forEach((session) => session.onClose());
    const afterClose = probe();
    expect(baseline - afterClose).toBeGreaterThanOrEqual(2 * 4096);
    expect(baseline - afterClose).toBeLessThan(128 * 4096);
    callbacks.forEach((callback) => callback(null));
    await flushAsyncWork();
    sessions.forEach((session) => session.onClose());
    expect(probe()).toBe(baseline);
    spy.mockRestore();
  });
  it('charges stalled rejection responses across sessions until their write callback', async () => {
    const spy = jest.spyOn(sizing, 'requestSize');
    const call = new FakeBatchAppendCall();
    let finish!: (error?: Error | null) => void;
    jest.spyOn(call, 'write').mockImplementation((_response, callback) => {
      finish = callback;
    });
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (e) => e as Error,
    );
    const probe = () => {
      const peer = new BatchAppendSession(
        new FakeBatchAppendCall() as never,
        async () => {},
        (id) => id?.string ?? '',
        (e) => e as Error,
      );
      spy.mockClear();
      peer.onData(createBatchAppendRequest('probe', 'event', false));
      const limit = spy.mock.calls[1][1];
      peer.onClose();
      return limit;
    };
    const baseline = probe();
    const request = createBatchAppendRequest('oversized', 'event', true);
    request.options!.streamIdentifier!.streamName = new Uint8Array(4096);
    request.proposedMessages[0].data = Buffer.alloc(17 * 1024 * 1024);
    session.onData(request);
    await flushAsyncWork();
    expect(baseline - probe()).toBeGreaterThanOrEqual(4096);
    finish(null);
    await flushAsyncWork();
    expect(probe()).toBeGreaterThan(baseline - 4096);
    session.onClose();
    expect(probe()).toBe(baseline);
    spy.mockRestore();
  });
  it('does not dispatch a structured alias of a tombstoned UUID and preserves peers and response id', async () => {
    const call = new FakeBatchAppendCall();
    const handler = jest.fn(async (group: BatchAppendReq[]) => {
      void group;
      await Promise.resolve();
    });
    const session = new BatchAppendSession(
      call as never,
      handler,
      correlationKey,
      (e) => e as Error,
    );
    const id = '00000000-0000-0001-0000-000000000002';
    const rejected = createBatchAppendRequest(id, 'large', false);
    rejected.proposedMessages[0].data = Buffer.alloc(17 * 1024 * 1024);
    session.onData(rejected);
    const alias = createBatchAppendRequest(id, 'late', true);
    alias.correlationId = {
      structured: { mostSignificantBits: 1, leastSignificantBits: 2 },
    };
    session.onData(alias);
    session.onData(
      createBatchAppendRequest(
        '00000000-0000-0001-0000-000000000003',
        'peer',
        true,
      ),
    );
    await flushAsyncWork();
    expect(call.ended).toBe(false);
    expect(call.writes[0].correlationId?.string).toBe(id);
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0][0][0].correlationId?.string).toBe(
      '00000000-0000-0001-0000-000000000003',
    );
    session.onClose();
  });

  it('fails closed on active UUID aliases without dispatching twice', async () => {
    const call = new FakeBatchAppendCall();
    const held = createDeferred<void>();
    const handler = jest.fn(() => held.promise);
    const session = new BatchAppendSession(
      call as never,
      handler,
      correlationKey,
      (e) => e as Error,
    );
    const id = '00000000-0000-0001-0000-000000000002';
    session.onData(createBatchAppendRequest(id, 'first', true));
    const alias = createBatchAppendRequest(id, 'duplicate', true);
    alias.correlationId = {
      structured: { mostSignificantBits: 1, leastSignificantBits: 2 },
    };
    session.onData(alias);
    expect(call.ended).toBe(true);
    expect(handler).toHaveBeenCalledTimes(1);
    expect(call.writes).toHaveLength(0);
    held.resolve();
    await flushAsyncWork();
  });

  it('fails closed before dispatch on malformed correlation UUIDs', () => {
    const call = new FakeBatchAppendCall();
    const handler = jest.fn(async () => {});
    const session = new BatchAppendSession(
      call as never,
      handler,
      correlationKey,
      (e) => e as Error,
    );
    const request = createBatchAppendRequest('not-a-uuid', 'event', true);
    session.onData(request);
    expect(call.destroyed).toBe(true);
    expect(handler).not.toHaveBeenCalled();
    expect(call.writes).toHaveLength(0);
  });

  it('normalizes signed and unsigned Long bits and validates representations', () => {
    const structured = (most: unknown, least: unknown) =>
      ({
        structured: { mostSignificantBits: most, leastSignificantBits: least },
      }) as never;
    expect(
      correlationKey(
        structured(
          Long.fromString('18446744073709551615', true),
          Long.fromInt(-1),
        ),
      ),
    ).toBe(correlationKey({ string: 'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF' }));
    expect(
      correlationKey(
        structured(Long.fromString('9223372036854775808', true), 0),
      ),
    ).toBe(correlationKey({ string: '80000000-0000-0000-0000-000000000000' }));
    expect(correlationKey(structured(-1, -9223372036854775808n))).toBe(
      correlationKey({ string: 'ffffffff-ffff-ffff-8000-000000000000' }),
    );
    for (const bad of [
      undefined,
      {},
      { string: '' },
      { string: 'not-a-uuid' },
      { string: '00000000-0000-0001-0000-000000000002', structured: {} },
      structured(1.5, 0),
      structured(Number.MAX_SAFE_INTEGER + 1, 0),
      structured('18446744073709551616', 0),
      structured('-9223372036854775809', 0),
      structured({}, 0),
    ]) {
      expect(() => correlationKey(bad as never)).toThrow();
    }
  });
  it('tombstones non-final overflow for the lifetime of the stream while peers continue', async () => {
    const call = new FakeBatchAppendCall();
    const handleGroup = jest.fn(async (group: BatchAppendReq[]) => {
      await Promise.resolve();
      void group;
    });
    const session = new BatchAppendSession(
      call as never,
      handleGroup,
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    session.onData(createBatchAppendRequest('peer', 'first', false));
    session.onData(createBatchAppendRequest('bad', 'first', false));
    const overflow = createBatchAppendRequest('bad', 'overflow', false);
    overflow.proposedMessages[0].data = Buffer.alloc(17 * 1024 * 1024);
    session.onData(overflow);
    session.onData(createBatchAppendRequest('bad', 'late-final', true));
    session.onData(createBatchAppendRequest('bad', 'duplicate', false));
    session.onData(createBatchAppendRequest('bad', 'duplicate-final', true));
    session.onData(createBatchAppendRequest('peer', 'final', true));
    await flushAsyncWork();
    expect(call.ended).toBe(false);
    expect(call.writes).toHaveLength(1);
    expect(call.writes[0]).toMatchObject({
      correlationId: { string: 'bad' },
      error: { code: 2, message: 'Append was not dispatched.' },
    });
    expect(handleGroup).toHaveBeenCalledTimes(1);
    expect(handleGroup.mock.calls[0][0][0].correlationId?.string).toBe('peer');
    session.onClose();
  });
  it('fails closed only after 64 lifetime tombstones, even after rejection writes finish', async () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    const large = Buffer.alloc(17 * 1024 * 1024);
    for (let i = 0; i < 64; i++) {
      const request = createBatchAppendRequest(String(i), 'event', false);
      request.proposedMessages[0].data = large;
      session.onData(request);
      await flushAsyncWork();
    }
    expect(call.writes).toHaveLength(64);
    session.onData(createBatchAppendRequest('0', 'late', true));
    expect(call.ended).toBe(false);
    const extra = createBatchAppendRequest('extra', 'event', false);
    extra.proposedMessages[0].data = large;
    session.onData(extra);
    expect(call.ended).toBe(true);
    expect(call.writes).toHaveLength(64);
  });

  it('keeps a tombstone past the incomplete-group timeout without closing the stream', async () => {
    jest.useFakeTimers();
    try {
      const call = new FakeBatchAppendCall();
      const handler = jest.fn(async () => {});
      const session = new BatchAppendSession(
        call as never,
        handler,
        (id) => id?.string ?? '',
        (error) => error as Error,
      );
      session.onData(createBatchAppendRequest('bad', 'first', false));
      const overflow = createBatchAppendRequest('bad', 'large', false);
      overflow.proposedMessages[0].data = Buffer.alloc(17 * 1024 * 1024);
      session.onData(overflow);
      jest.advanceTimersByTime(60_000);
      session.onData(createBatchAppendRequest('bad', 'late', true));
      await Promise.resolve();
      expect(call.ended).toBe(false);
      expect(handler).not.toHaveBeenCalled();
      expect(call.writes).toHaveLength(1);
      session.onClose();
    } finally {
      jest.useRealTimers();
    }
  });

  it('fails closed on reuse of an active correlation without issuing a false rejection', async () => {
    const call = new FakeBatchAppendCall();
    const held = createDeferred<void>();
    const handler = jest.fn(() => held.promise);
    const session = new BatchAppendSession(
      call as never,
      handler,
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    session.onData(createBatchAppendRequest('active', 'first', true));
    session.onData(createBatchAppendRequest('active', 'duplicate', true));
    expect(call.ended).toBe(true);
    expect(call.writes).toHaveLength(0);
    expect(handler).toHaveBeenCalledTimes(1);
    held.resolve();
    await flushAsyncWork();
  });

  it('rejects an overflowing final fragment without dispatching it or disturbing another group', async () => {
    const call = new FakeBatchAppendCall();
    const handleGroup = jest.fn(async (group: BatchAppendReq[]) => {
      await Promise.resolve();
      void group;
    });
    const session = new BatchAppendSession(
      call as never,
      handleGroup,
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    const other = createBatchAppendRequest('other', 'event', false);
    const first = createBatchAppendRequest('rejected', 'event', false);
    first.proposedMessages[0].data = Buffer.alloc(8 * 1024 * 1024);
    session.onData(other);
    session.onData(first);
    const final = createBatchAppendRequest('rejected', 'event', true);
    final.options = undefined;
    final.proposedMessages[0].data = Buffer.alloc(9 * 1024 * 1024);
    session.onData(final);
    await flushAsyncWork();
    expect(call.ended).toBe(false);
    expect(handleGroup).not.toHaveBeenCalled();
    expect(call.writes).toHaveLength(1);
    expect(call.writes[0]).toMatchObject({
      correlationId: { string: 'rejected' },
      error: { code: 2, message: 'Append was not dispatched.' },
    });
    expect(Buffer.from(call.writes[0].streamIdentifier!.streamName)).toEqual(
      first.options!.streamIdentifier!.streamName,
    );
    expect(session['pendingMessages'].size).toBe(1);
    expect(session['retainedBytes']).toBeLessThan(8 * 1024 * 1024);
    session.onData(createBatchAppendRequest('other', 'last', true));
    await flushAsyncWork();
    expect(handleGroup).toHaveBeenCalledTimes(1);
    expect(handleGroup.mock.calls[0][0]).toHaveLength(2);
    session.onClose();
    expect(session['retainedBytes']).toBe(0);
  });
  it('waits for a correlated rejection write before ending input', async () => {
    const call = new FakeBatchAppendCall();
    let finishWrite!: (error?: Error | null) => void;
    jest.spyOn(call, 'write').mockImplementation((_response, callback) => {
      finishWrite = callback;
    });
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    const request = createBatchAppendRequest('oversized', 'event', true);
    request.proposedMessages[0].data = Buffer.alloc(17 * 1024 * 1024);
    session.onData(request);
    session.onEnd();
    await flushAsyncWork();
    expect(call.ended).toBe(false);
    finishWrite(null);
    await flushAsyncWork();
    expect(call.ended).toBe(true);
  });

  it('fails closed when rejection responses accumulate beyond their bound', () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    const large = Buffer.alloc(17 * 1024 * 1024);
    for (let i = 0; i < 65; i++) {
      const request = createBatchAppendRequest(String(i), 'event', true);
      request.proposedMessages[0].data = large;
      session.onData(request);
    }
    expect(call.ended).toBe(true);
  });

  it('expires abandoned incomplete groups so other sessions can use the budget', () => {
    jest.useFakeTimers();
    try {
      const call = new FakeBatchAppendCall();
      const session = new BatchAppendSession(
        call as never,
        async () => {},
        (id) => id?.string ?? '',
        (error) => error as Error,
      );
      session.onData(createBatchAppendRequest('abandoned', 'event', false));
      jest.advanceTimersByTime(30_000);
      expect(call.ended).toBe(true);
      expect(session['retainedBytes']).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });
  it('shares backing charges across sessions and retains them through closed active cleanup', async () => {
    const calls = Array.from({ length: 8 }, () => new FakeBatchAppendCall());
    const cleanup = createDeferred<void>();
    const sessions = calls.map(
      (call, index) =>
        new BatchAppendSession(
          call as never,
          (_group, _respond, _started, retainUntil) => {
            if (index === 0) retainUntil(cleanup.promise);
            return Promise.resolve();
          },
          (id) => id?.string ?? '',
          (error) => error as Error,
        ),
    );
    const shared = Buffer.alloc(15 * 1024 * 1024);
    for (let i = 0; i < 2; i++) {
      const request = createBatchAppendRequest(String(i), 'event', i === 0);
      request.proposedMessages[0].data = shared.subarray(0, 1);
      sessions[i].onData(request);
    }
    sessions[0].onClose();
    sessions[1].onClose();
    await flushAsyncWork();
    for (let i = 2; i < 5; i++) {
      const request = createBatchAppendRequest(String(i), 'event', false);
      request.proposedMessages[0].data = Buffer.alloc(15 * 1024 * 1024);
      sessions[i].onData(request);
    }
    const sharedAgain = createBatchAppendRequest('shared', 'event', false);
    sharedAgain.proposedMessages[0].data = shared.subarray(0, 1);
    sessions[7].onData(sharedAgain);
    expect(calls[7].ended).toBe(false);
    sessions[7].onClose();
    const overflow = createBatchAppendRequest('overflow', 'event', false);
    overflow.proposedMessages[0].data = Buffer.alloc(15 * 1024 * 1024);
    sessions[5].onData(overflow);
    expect(calls[4].ended).toBe(false);
    expect(calls[5].ended).toBe(false);
    expect(sessions[5]['pendingMessages'].size).toBe(0);
    cleanup.resolve();
    await flushAsyncWork();
    const replacement = createBatchAppendRequest('replacement', 'event', false);
    replacement.proposedMessages[0].data = Buffer.alloc(15 * 1024 * 1024);
    sessions[6].onData(replacement);
    expect(calls[6].ended).toBe(false);
    sessions.forEach((session) => session.onClose());
  });
  it('bounds incomplete requests across sessions and reuses released capacity', () => {
    const calls = Array.from({ length: 6 }, () => new FakeBatchAppendCall());
    const sessions = calls.map(
      (call) =>
        new BatchAppendSession(
          call as never,
          async () => {},
          (id) => id?.string ?? '',
          (error) => error as Error,
        ),
    );
    for (let i = 0; i < 5; i++) {
      const request = createBatchAppendRequest(String(i), 'event', false);
      request.proposedMessages[0].data = Buffer.alloc(15 * 1024 * 1024);
      sessions[i].onData(request);
    }
    expect(calls[3].ended).toBe(false);
    expect(calls[4].ended).toBe(false);
    expect(sessions[4]['pendingMessages'].size).toBe(0);
    sessions[0].onClose();
    const request = createBatchAppendRequest('replacement', 'event', false);
    request.proposedMessages[0].data = Buffer.alloc(15 * 1024 * 1024);
    sessions[5].onData(request);
    expect(calls[5].ended).toBe(false);
    sessions.forEach((session) => session.onClose());
  });
  it('fails closed without throwing when the correlation id is malformed', () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      () => {
        throw new Error('Missing correlation id');
      },
      (error) => error as Error,
    );
    const request = createBatchAppendRequest('a', 'event', false);
    request.correlationId = undefined;
    expect(() => session.onData(request)).not.toThrow();
    expect(call.destroyed).toBe(true);
    expect(session['pendingMessages'].size).toBe(0);
  });
  it('fails closed before retaining unbounded incomplete correlations', () => {
    const call = new FakeBatchAppendCall();
    const handleGroup = jest.fn(async () => {});
    const session = new BatchAppendSession(
      call as never,
      handleGroup,
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    for (let i = 0; i < 200; i++) {
      session.onData(createBatchAppendRequest(String(i), 'event', false));
    }
    expect(call.ended).toBe(true);
    expect(handleGroup).not.toHaveBeenCalled();
  });

  it('rejects only the group exceeding the byte budget', () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    for (const id of ['a', 'b', 'c']) {
      const request = createBatchAppendRequest(id, 'event', false);
      request.proposedMessages[0].data = Buffer.alloc(6 * 1024 * 1024);
      session.onData(request);
    }
    expect(call.ended).toBe(false);
    expect(session['pendingMessages'].size).toBe(2);
    session.onClose();
  });

  it('charges shared backing once across fragments and groups until the last handler settles', async () => {
    const call = new FakeBatchAppendCall();
    const held = createDeferred<void>();
    const session = new BatchAppendSession(
      call as never,
      () => held.promise,
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    const shared = Buffer.alloc(10 * 1024 * 1024);
    for (const [id, final] of [
      ['a', false],
      ['a', true],
      ['b', false],
    ] as const) {
      const request = createBatchAppendRequest(id, 'event', final);
      request.proposedMessages[0].data = shared.subarray(0, 1);
      session.onData(request);
    }
    expect(call.ended).toBe(false);
    held.resolve();
    await flushAsyncWork();
    const request = createBatchAppendRequest('c', 'event', false);
    request.proposedMessages[0].data = Buffer.alloc(7 * 1024 * 1024);
    session.onData(request);
    expect(call.ended).toBe(false);
    expect(session['pendingMessages'].size).toBe(1);
    session.onClose();
    expect(session['retainedBytes']).toBe(0);
  });

  it('keeps shared active storage charged after incomplete groups close and releases it on settlement', async () => {
    const call = new FakeBatchAppendCall();
    const held = createDeferred<void>();
    const session = new BatchAppendSession(
      call as never,
      () => held.promise,
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    const shared = Buffer.alloc(8 * 1024 * 1024);
    for (const [id, final] of [
      ['active', true],
      ['pending', false],
    ] as const) {
      const request = createBatchAppendRequest(id, 'event', final);
      request.proposedMessages[0].data = shared.subarray(0, 1);
      session.onData(request);
    }
    session.onClose();
    expect(session['retainedBytes']).toBeGreaterThanOrEqual(shared.byteLength);
    held.resolve();
    await flushAsyncWork();
    expect(session['retainedBytes']).toBe(0);
    expect(session['retainedBackings'].size).toBe(0);
  });

  it('accounts for backing storage retained by a small byte-array view', () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    const request = createBatchAppendRequest('a', 'event', false);
    request.proposedMessages[0].data = Buffer.alloc(20 * 1024 * 1024).subarray(
      0,
      1,
    );
    session.onData(request);
    expect(call.ended).toBe(false);
    expect(session['pendingMessages'].size).toBe(0);
    session.onClose();
  });

  it('bounds the total retained event count across fragments', () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    for (let i = 0; i < 101; i++) {
      const request = createBatchAppendRequest('a', 'event', false);
      request.proposedMessages = Array.from({ length: 100 }, () => ({
        ...request.proposedMessages[0],
        id: { ...request.proposedMessages[0].id },
        metadata: { ...request.proposedMessages[0].metadata },
        data: Buffer.alloc(0),
        customMetadata: Buffer.alloc(0),
      }));
      session.onData(request);
      if (i < 100) expect(call.ended).toBe(false);
    }
    expect(call.ended).toBe(false);
    expect(session['retainedEvents']).toBe(0);
    session.onClose();
  });

  it('counts dispatched groups until their handlers settle', async () => {
    const call = new FakeBatchAppendCall();
    const held = createDeferred<void>();
    const session = new BatchAppendSession(
      call as never,
      () => held.promise,
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    for (let i = 0; i < 65; i++) {
      session.onData(createBatchAppendRequest(String(i), 'event', true));
    }
    await flushAsyncWork();
    expect(call.ended).toBe(false);
    expect(call.writes).toHaveLength(1);
    expect(call.writes[0].correlationId).toEqual({
      string: '64',
      structured: undefined,
    });
    held.resolve();
    await flushAsyncWork();
    session.onClose();
  });

  it('clears incomplete fragments immediately on stream error', () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    session.onData(createBatchAppendRequest('a', 'event', false));
    session.onError(new Error('cancelled'));
    expect(session['retainedBytes']).toBe(0);
    expect(session['retainedEvents']).toBe(0);
    expect(session['pendingMessages'].size).toBe(0);
  });

  it('releases incomplete groups at input end while active writes settle', () => {
    const call = new FakeBatchAppendCall();
    const held = createDeferred<void>();
    const session = new BatchAppendSession(
      call as never,
      () => held.promise,
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    session.onData(createBatchAppendRequest('active', 'event', true));
    session.onData(createBatchAppendRequest('pending', 'event', false));
    session.onEnd();
    expect(session['pendingMessages'].size).toBe(0);
    held.resolve();
  });

  it('reuses accounting after completed groups settle', async () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      async () => {},
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    for (let i = 0; i < 100; i++) {
      session.onData(createBatchAppendRequest(String(i), 'event', true));
      await flushAsyncWork();
    }
    expect(call.destroyed).toBe(false);
    expect(session['retainedBytes']).toBe(0);
    expect(session['retainedEvents']).toBe(0);
  });

  it('fails closed and releases incomplete groups if a handler throws synchronously', () => {
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      () => {
        throw new Error('handler failed');
      },
      (id) => id?.string ?? '',
      (error) => error as Error,
    );
    session.onData(createBatchAppendRequest('pending', 'event', false));
    expect(() =>
      session.onData(createBatchAppendRequest('active', 'event', true)),
    ).not.toThrow();
    expect(call.destroyed).toBe(true);
    expect(session['retainedBytes']).toBe(0);
  });

  it('keeps interleaved correlation groups isolated before dispatching them', async () => {
    const firstGroup = createDeferred<BatchAppendResp>();
    const secondGroup = createDeferred<BatchAppendResp>();
    const handleGroup = jest.fn<
      Promise<void>,
      [BatchAppendReq[], (response: BatchAppendResp) => Promise<void>]
    >((requestGroup, enqueueResponse) => {
      const correlationId = requestGroup[0]?.correlationId?.string;

      if (correlationId === 'alpha') {
        return firstGroup.promise.then((response) => enqueueResponse(response));
      }

      if (correlationId === 'beta') {
        return secondGroup.promise.then((response) =>
          enqueueResponse(response),
        );
      }

      throw new Error(
        `Unexpected correlation id ${correlationId ?? 'missing'}`,
      );
    });
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      handleGroup,
      (correlationId) => {
        if (!correlationId?.string) {
          throw new Error('Missing correlation id');
        }

        return correlationId.string;
      },
      (error) => error as Error,
    );

    session.onData(createBatchAppendRequest('alpha', 'alpha-1', false));
    session.onData(createBatchAppendRequest('beta', 'beta-1', false));
    session.onData(createBatchAppendRequest('beta', 'beta-2', true));
    session.onData(createBatchAppendRequest('alpha', 'alpha-2', true));
    session.onEnd();

    await flushAsyncWork();

    expect(handleGroup).toHaveBeenCalledTimes(2);
    expect(handleGroup.mock.calls[0]?.[0]).toMatchObject([
      {
        correlationId: { string: 'beta' },
        isFinal: false,
        proposedMessages: [{ metadata: { type: 'beta-1' } }],
      },
      {
        correlationId: { string: 'beta' },
        isFinal: true,
        proposedMessages: [{ metadata: { type: 'beta-2' } }],
      },
    ]);
    expect(handleGroup.mock.calls[1]?.[0]).toMatchObject([
      {
        correlationId: { string: 'alpha' },
        isFinal: false,
        proposedMessages: [{ metadata: { type: 'alpha-1' } }],
      },
      {
        correlationId: { string: 'alpha' },
        isFinal: true,
        proposedMessages: [{ metadata: { type: 'alpha-2' } }],
      },
    ]);
    expect(call.ended).toBe(false);

    secondGroup.resolve(createBatchAppendResponse('beta'));
    await flushAsyncWork();
    expect(call.writes).toEqual([createBatchAppendResponse('beta')]);
    expect(call.ended).toBe(false);

    firstGroup.resolve(createBatchAppendResponse('alpha'));
    await flushAsyncWork();
    expect(call.writes).toEqual([
      createBatchAppendResponse('beta'),
      createBatchAppendResponse('alpha'),
    ]);
    expect(call.ended).toBe(true);
  });

  it('does not write or end the stream after the duplex call closes', async () => {
    const deferredResponse = createDeferred<BatchAppendResp>();
    const handleGroup = jest.fn<
      Promise<void>,
      [BatchAppendReq[], (response: BatchAppendResp) => Promise<void>]
    >((_, enqueueResponse) =>
      deferredResponse.promise.then((response) => enqueueResponse(response)),
    );
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      handleGroup,
      (correlationId) => {
        if (!correlationId?.string) {
          throw new Error('Missing correlation id');
        }

        return correlationId.string;
      },
      (error) => error as Error,
    );

    session.onData(createBatchAppendRequest('alpha', 'alpha-1', true));
    session.onEnd();

    await flushAsyncWork();
    session.onClose();

    deferredResponse.resolve(createBatchAppendResponse('alpha'));
    await flushAsyncWork();

    expect(handleGroup).toHaveBeenCalledTimes(1);
    expect(call.writes).toEqual([]);
    expect(call.ended).toBe(false);
    expect(call.destroyed).toBe(false);
  });
});
