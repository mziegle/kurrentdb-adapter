import { TransactionAdmission } from './transaction-admission';

describe('TransactionAdmission', () => {
  it('bounds count and bytes including the owner and recovers on release', async () => {
    const mutex = new TransactionAdmission(2, 5);
    const owner = await mutex.acquire(3);
    await expect(mutex.acquire(3)).rejects.toMatchObject({ code: 'capacity' });
    const waiting = mutex.acquire(2);
    await expect(mutex.acquire(0)).rejects.toMatchObject({ code: 'capacity' });
    owner();
    (await waiting)();
    (await mutex.acquire(5))();
  });
  it('removes cancelled queued owners without releasing the current owner', async () => {
    const mutex = new TransactionAdmission(2, 2);
    const owner = await mutex.acquire(1);
    const controller = new AbortController();
    const cancelled = mutex.acquire(1, { signal: controller.signal });
    const rejection = expect(cancelled).rejects.toMatchObject({
      code: 'cancelled',
    });
    controller.abort();
    await rejection;
    let granted = false;
    const next = mutex.acquire(1).then((release) => {
      granted = true;
      return release;
    });
    await Promise.resolve();
    expect(granted).toBe(false);
    owner();
    (await next)();
  });
  it('checks elapsed deadlines before granting even if the timer has not fired', async () => {
    jest.useFakeTimers();
    try {
      jest.setSystemTime(1000);
      const mutex = new TransactionAdmission(2, 2);
      const owner = await mutex.acquire(1);
      const pending = mutex.acquire(1, { deadline: 1010 });
      const rejected = expect(pending).rejects.toMatchObject({
        code: 'cancelled',
      });
      jest.setSystemTime(1010);
      owner();
      await rejected;
      (await mutex.acquire(2))();
    } finally {
      jest.useRealTimers();
    }
  });
  it('shutdown rejects waiters but preserves the owner until its late release', async () => {
    const mutex = new TransactionAdmission(2, 2);
    const owner = await mutex.acquire(1);
    const waiting = mutex.acquire(1);
    const rejected = expect(waiting).rejects.toMatchObject({ code: 'closed' });
    const drained = mutex.shutdown();
    let done = false;
    void drained.then(() => {
      done = true;
    });
    await rejected;
    await Promise.resolve();
    expect(done).toBe(false);
    await expect(mutex.acquire(1)).rejects.toMatchObject({ code: 'closed' });
    owner();
    owner();
    await drained;
    expect(done).toBe(true);
  });
  it('validates limits and reservation sizes without corrupting accounting', async () => {
    expect(() => new TransactionAdmission(0, 1)).toThrow();
    expect(() => new TransactionAdmission(1, Infinity)).toThrow();
    const mutex = new TransactionAdmission(2, 2);
    expect(() => mutex.acquire(-1)).toThrow();
    expect(() => mutex.acquire(NaN)).toThrow();
    expect(() => mutex.acquire(1, { deadline: NaN })).toThrow();
    (await mutex.acquire(2))();
  });
  it('a duplicate late release cannot unlock the next owner', async () => {
    const mutex = new TransactionAdmission(3, 3);
    const first = await mutex.acquire(1);
    const second = mutex.acquire(1);
    let thirdGranted = false;
    const third = mutex.acquire(1).then((release) => {
      thirdGranted = true;
      return release;
    });
    first();
    const current = await second;
    first();
    await Promise.resolve();
    expect(thirdGranted).toBe(false);
    current();
    (await third)();
  });
  it('reclaims elapsed waiters before overload decisions even when timers are delayed', async () => {
    jest.useFakeTimers();
    try {
      jest.setSystemTime(1000);
      const mutex = new TransactionAdmission(2, 2);
      const owner = await mutex.acquire(1);
      const expired = mutex.acquire(1, { deadline: 1001 });
      const rejected = expect(expired).rejects.toMatchObject({
        code: 'cancelled',
      });
      jest.setSystemTime(1001);
      const next = mutex.acquire(1);
      await rejected;
      owner();
      (await next)();
    } finally {
      jest.useRealTimers();
    }
  });
  it('does not revoke ownership when an acquired request is aborted', async () => {
    const mutex = new TransactionAdmission(2, 2);
    const controller = new AbortController();
    const owner = await mutex.acquire(1, { signal: controller.signal });
    controller.abort();
    let acquired = false;
    const next = mutex.acquire(1).then((release) => {
      acquired = true;
      return release;
    });
    await Promise.resolve();
    expect(acquired).toBe(false);
    owner();
    (await next)();
  });
  it('actively expires a waiter and immediately frees its reservation', async () => {
    jest.useFakeTimers();
    try {
      jest.setSystemTime(1000);
      const mutex = new TransactionAdmission(2, 2);
      const owner = await mutex.acquire(1);
      const waiting = mutex.acquire(1, { deadline: 1010 });
      const rejected = expect(waiting).rejects.toMatchObject({
        code: 'cancelled',
      });
      jest.advanceTimersByTime(10);
      await rejected;
      const next = mutex.acquire(1);
      owner();
      (await next)();
    } finally {
      jest.useRealTimers();
    }
  });
  it('grants one owner at a time in FIFO order', async () => {
    const mutex = new TransactionAdmission(3, 10);
    const first = await mutex.acquire(1);
    const order: number[] = [];
    const second = mutex.acquire(1).then((lease) => {
      order.push(2);
      return lease;
    });
    const third = mutex.acquire(1).then((lease) => {
      order.push(3);
      return lease;
    });
    expect(order).toEqual([]);
    first();
    const next = await second;
    expect(order).toEqual([2]);
    next();
    (await third)();
    expect(order).toEqual([2, 3]);
  });
});
