import { SerialAdmission } from './serial-admission';

describe('test-only serial admission', () => {
  it('does not execute a cancelled queued operation', async () => {
    const admission = new SerialAdmission(2);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const first = admission.run(() => gate);
    const controller = new AbortController();
    let ran = false;
    const run = admission.run.bind(admission) as (
      operation: () => Promise<void>,
      signal: AbortSignal,
    ) => Promise<void>;
    const queued = run(() => {
      ran = true;
      return Promise.resolve();
    }, controller.signal);
    const outcome = queued.then(
      () => 'executed',
      () => 'cancelled',
    );
    controller.abort();
    release();
    await first;
    expect(await outcome).toBe('cancelled');
    expect(ran).toBe(false);
  });
  it('serializes calls, rejects overflow, and continues after failure', async () => {
    const admission = new SerialAdmission(2);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const order: string[] = [];
    const first = admission.run(async () => {
      order.push('first');
      await gate;
      throw new Error('failed');
    });
    const firstResult = first.catch((error: Error) => error.message);
    const second = admission.run(() => {
      order.push('second');
      return Promise.resolve(42);
    });
    await expect(admission.run(() => Promise.resolve(0))).rejects.toThrow(
      'admission full',
    );
    await Promise.resolve();
    expect(order).toEqual(['first']);
    release();
    expect(await firstResult).toBe('failed');
    expect(await second).toBe(42);
    expect(order).toEqual(['first', 'second']);
    expect(await admission.run(() => Promise.resolve(3))).toBe(3);
  });
});
