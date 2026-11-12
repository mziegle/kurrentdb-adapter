import { EventEmitter } from 'node:events';
import { Metadata } from '@grpc/grpc-js';
import { StreamsController } from './streams.controller';

class Call extends EventEmitter {
  metadata = new Metadata();
  cancelled = false;
  destroyed = false;
  deadline: number = Infinity;
  getDeadline() {
    return this.deadline;
  }
  write(_response: unknown, callback: () => void) {
    callback();
  }
  end() {}
  destroy() {
    this.destroyed = true;
    this.emit('close');
  }
}
const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
const deferred = () => {
  let resolve!: (value: never) => void;
  const promise = new Promise<never>((done) => {
    resolve = done;
  });
  return { promise, resolve };
};
const stats = { startOperation: () => ({ succeeded() {}, failed() {} }) };

describe('write context from gRPC streams', () => {
  it.each(['delete', 'tombstone'] as const)(
    'cleans up unary %s listeners on backend failure',
    async (method) => {
      const write = jest.fn().mockRejectedValue(new Error('backend failed'));
      const controller = new StreamsController(
        { [method]: write } as never,
        stats as never,
      );
      const call = new Call();
      call.deadline = Date.now() + 30000;
      const invoke = controller[method] as unknown as (
        request: unknown,
        metadata: Metadata,
        call: Call,
      ) => Promise<unknown>;
      const result = Reflect.apply(invoke, controller, [
        {},
        call.metadata,
        call,
      ]);
      await expect(result).rejects.toThrow();
      expect(call.listenerCount('cancelled')).toBe(0);
      expect(call.listenerCount('close')).toBe(0);
      expect(call.listenerCount('error')).toBe(0);
    },
  );
  it.each(['delete', 'tombstone'] as const)(
    'propagates unary %s deadline and cancellation until settlement',
    async (method) => {
      const pending = deferred();
      const write = jest.fn<Promise<never>, [unknown, unknown]>(
        () => pending.promise,
      );
      const controller = new StreamsController(
        { [method]: write } as never,
        stats as never,
      );
      const call = new Call();
      call.deadline = Date.now() + 30000;
      const invoke = controller[method] as unknown as (
        request: unknown,
        metadata: Metadata,
        call: Call,
      ) => Promise<unknown>;
      const result = Reflect.apply(invoke, controller, [
        {},
        call.metadata,
        call,
      ]);
      const context = write.mock.calls[0][1] as {
        signal: AbortSignal;
        deadline: number;
      };
      expect(context.deadline).toBe(call.deadline);
      expect(context.signal.aborted).toBe(false);
      call.emit('cancelled');
      expect(context.signal.aborted).toBe(true);
      pending.resolve({} as never);
      await result;
      expect(call.listenerCount('cancelled')).toBe(0);
      expect(call.listenerCount('close')).toBe(0);
      expect(call.listenerCount('error')).toBe(0);
    },
  );
  it('propagates batch cancellation without a deadline after input half-close', () => {
    const batchAppend = jest.fn<Promise<never>, [unknown, unknown]>(
      () => deferred().promise,
    );
    const controller = new StreamsController(
      { batchAppend } as never,
      stats as never,
    );
    const call = new Call();
    controller.batchAppend(call as never);
    call.emit('data', { correlationId: { string: 'a' }, isFinal: true });
    call.emit('end');
    const context = batchAppend.mock.calls[0][1] as {
      signal: AbortSignal;
      deadline?: number;
    };
    expect(context.deadline).toBeUndefined();
    expect(context.signal.aborted).toBe(false);
    call.emit('cancelled');
    expect(context.signal.aborted).toBe(true);
  });
  it('expires an append at its gRPC deadline and removes listeners after settlement', async () => {
    jest.useFakeTimers();
    try {
      const pending = deferred();
      const append = jest.fn<Promise<never>, [unknown, unknown]>(
        () => pending.promise,
      );
      const controller = new StreamsController(
        { append } as never,
        stats as never,
      );
      const call = new Call();
      call.deadline = Date.now() + 1000;
      controller.append(call as never, jest.fn());
      call.emit('end');
      const context = append.mock.calls[0][1] as { signal: AbortSignal };
      jest.advanceTimersByTime(1000);
      expect(context.signal.aborted).toBe(true);
      expect(call.listenerCount('cancelled')).toBe(0);
      pending.resolve({} as never);
      await Promise.resolve();
    } finally {
      jest.useRealTimers();
    }
  });
  it('uses the absolute batch deadline even with an infinite gRPC deadline', () => {
    const batchAppend = jest.fn<Promise<never>, [unknown, unknown]>(
      () => deferred().promise,
    );
    const controller = new StreamsController(
      { batchAppend } as never,
      stats as never,
    );
    const call = new Call();
    controller.batchAppend(call as never);
    const deadline = Date.now() + 30000;
    call.emit('data', {
      correlationId: { string: 'a' },
      options: {
        deadline21100: {
          seconds: Math.floor(deadline / 1000),
          nanos: (deadline % 1000) * 1000000,
        },
      },
      isFinal: true,
    });
    const context = batchAppend.mock.calls[0][1] as {
      signal: AbortSignal;
      deadline: number;
    };
    expect(context.deadline).toBe(deadline);
    expect(context.signal.aborted).toBe(false);
    call.emit('close');
    expect(context.signal.aborted).toBe(true);
  });
  it.each(['deadline', 'deadline21100'] as const)(
    'rounds fractional %s down without affecting another correlation',
    (field) => {
      jest.useFakeTimers();
      try {
        jest.setSystemTime(1700000000000);
        const batchAppend = jest.fn<Promise<never>, [unknown, unknown]>(
          () => deferred().promise,
        );
        const controller = new StreamsController(
          { batchAppend } as never,
          stats as never,
        );
        const call = new Call();
        controller.batchAppend(call as never);
        call.emit('data', {
          correlationId: { string: 'a' },
          options: {
            [field]: {
              seconds: field === 'deadline' ? 1 : 1700000001,
              nanos: 999999,
            },
          },
          isFinal: true,
        });
        call.emit('data', {
          correlationId: { string: 'b' },
          options: { deadline: { seconds: 2, nanos: 1000001 } },
          isFinal: true,
        });
        const a = batchAppend.mock.calls[0][1] as {
          deadline: number;
          signal: AbortSignal;
        };
        const b = batchAppend.mock.calls[1][1] as {
          deadline: number;
          signal: AbortSignal;
        };
        expect(a.deadline).toBe(1700000001000);
        expect(b.deadline).toBe(1700000002001);
        jest.advanceTimersByTime(1000);
        expect(a.signal.aborted).toBe(true);
        expect(b.signal.aborted).toBe(false);
        expect(call.destroyed).toBe(false);
        call.emit('close');
      } finally {
        jest.useRealTimers();
      }
    },
  );
  it('uses each batch group deadline from the first fragment, including buffering time', () => {
    jest.useFakeTimers();
    try {
      jest.setSystemTime(1700000000000);
      const batchAppend = jest.fn<Promise<never>, [unknown, unknown]>(
        () => deferred().promise,
      );
      const controller = new StreamsController(
        { batchAppend } as never,
        stats as never,
      );
      const call = new Call();
      call.deadline = Date.now() + 10000;
      controller.batchAppend(call as never);
      call.emit('data', {
        correlationId: { string: 'a' },
        options: { deadline: { seconds: 2, nanos: 0 } },
        isFinal: false,
      });
      jest.advanceTimersByTime(3000);
      call.emit('data', { correlationId: { string: 'a' }, isFinal: true });
      const context = batchAppend.mock.calls[0][1] as {
        signal: AbortSignal;
        deadline: number;
      };
      expect(context.deadline).toBe(1700000002000);
      expect(context.signal.aborted).toBe(true);
      call.emit('close');
    } finally {
      jest.useRealTimers();
    }
  });
  it('keeps batch correlation contexts alive independently until each backend write settles', async () => {
    const alpha = deferred();
    const beta = deferred();
    const batchAppend = jest.fn<
      Promise<never>,
      [{ correlationId: { string: string } }[], unknown]
    >((messages: { correlationId: { string: string } }[]) =>
      messages[0].correlationId.string === 'alpha'
        ? alpha.promise
        : beta.promise,
    );
    const controller = new StreamsController(
      { batchAppend } as never,
      stats as never,
    );
    const call = new Call();
    controller.batchAppend(call as never);
    call.emit('data', { correlationId: { string: 'alpha' }, isFinal: true });
    call.emit('data', { correlationId: { string: 'beta' }, isFinal: true });
    call.emit('end');
    const a = batchAppend.mock.calls[0][1] as { signal: AbortSignal };
    const b = batchAppend.mock.calls[1][1] as { signal: AbortSignal };
    expect(a.signal).not.toBe(b.signal);
    expect(a.signal.aborted).toBe(false);
    alpha.resolve({} as never);
    await tick();
    call.emit('close');
    expect(a.signal.aborted).toBe(false);
    expect(b.signal.aborted).toBe(true);
    beta.resolve({} as never);
    await tick();
  });
  it('passes append cancellation and deadline to the backend after input half-close', async () => {
    const pending = deferred();
    const append = jest.fn<Promise<never>, [unknown, unknown]>(
      () => pending.promise,
    );
    const controller = new StreamsController(
      { append } as never,
      stats as never,
    );
    const call = new Call();
    call.deadline = Date.now() + 30000;
    controller.append(call as never, jest.fn());
    call.emit('end');
    const context = append.mock.calls[0]?.[1] as {
      signal: AbortSignal;
      deadline: number;
    };
    expect(context.deadline).toBe(call.deadline);
    expect(context.signal.aborted).toBe(false);
    call.emit('close'); // A readable stream may auto-close after normal EOF.
    expect(context.signal.aborted).toBe(false);
    call.emit('cancelled');
    expect(context.signal.aborted).toBe(true);
    pending.resolve({} as never);
    await tick();
  });
});
