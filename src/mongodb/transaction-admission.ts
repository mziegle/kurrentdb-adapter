type Waiter = {
  bytes: number;
  resolve: (release: () => void) => void;
  cleanup: () => void;
  expire: () => boolean;
  close: () => void;
};

/** Isolated process-local admission; the owner releases only after cleanup. */
export class TransactionAdmission {
  private readonly queue: Waiter[] = [];
  private active = false;
  private count = 0;
  private bytes = 0;
  private closed = false;
  private shutdownPromise?: Promise<void>;
  private resolveShutdown?: () => void;
  constructor(
    private readonly maxCount: number,
    private readonly maxBytes: number,
  ) {
    if (
      !Number.isSafeInteger(maxCount) ||
      maxCount < 1 ||
      !Number.isSafeInteger(maxBytes) ||
      maxBytes < 0
    )
      throw new Error('invalid admission limits');
  }

  acquire(
    bytes: number,
    options: { signal?: AbortSignal; deadline?: number } = {},
  ): Promise<() => void> {
    if (
      !Number.isSafeInteger(bytes) ||
      bytes < 0 ||
      (options.deadline !== undefined &&
        !Number.isSafeInteger(options.deadline))
    )
      throw new Error('invalid admission request');
    if (this.closed)
      return Promise.reject(
        Object.assign(new Error('admission closed'), { code: 'closed' }),
      );
    if (
      options.signal?.aborted ||
      (options.deadline !== undefined && options.deadline <= Date.now())
    )
      return Promise.reject(
        Object.assign(new Error('admission cancelled'), { code: 'cancelled' }),
      );
    for (const waiter of [...this.queue]) waiter.expire();
    if (this.count >= this.maxCount || bytes > this.maxBytes - this.bytes)
      return Promise.reject(
        Object.assign(new Error('admission capacity'), { code: 'capacity' }),
      );
    this.count++;
    this.bytes += bytes;
    return new Promise((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const remove = (code: 'closed' | 'cancelled') => {
        const index = this.queue.indexOf(waiter);
        if (index < 0) return;
        this.queue.splice(index, 1);
        waiter.cleanup();
        this.count--;
        this.bytes -= bytes;
        reject(Object.assign(new Error(`admission ${code}`), { code }));
      };
      const cancel = () => remove('cancelled');
      const waiter: Waiter = {
        bytes,
        resolve,
        close: () => remove('closed'),
        cleanup: () => {
          options.signal?.removeEventListener('abort', cancel);
          if (timer !== undefined) clearTimeout(timer);
        },
        expire: () => {
          if (
            options.signal?.aborted ||
            (options.deadline !== undefined && options.deadline <= Date.now())
          ) {
            cancel();
            return true;
          }
          return false;
        },
      };
      this.queue.push(waiter);
      options.signal?.addEventListener('abort', cancel, { once: true });
      const schedule = () => {
        if (options.deadline === undefined || waiter.expire()) return;
        timer = setTimeout(
          schedule,
          Math.min(options.deadline - Date.now(), 2147483647),
        );
      };
      schedule();
      this.drain();
    });
  }

  shutdown(): Promise<void> {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.closed = true;
    this.shutdownPromise = new Promise((resolve) => {
      this.resolveShutdown = resolve;
    });
    while (this.queue.length) this.queue[0].close();
    if (!this.active) this.resolveShutdown?.();
    return this.shutdownPromise;
  }

  private drain(): void {
    if (this.active) return;
    while (this.queue.length) {
      const waiter = this.queue[0];
      if (waiter.expire()) continue;
      this.queue.shift();
      waiter.cleanup();
      this.active = true;
      let released = false;
      waiter.resolve(() => {
        if (released) return;
        released = true;
        this.active = false;
        this.count--;
        this.bytes -= waiter.bytes;
        if (this.closed) this.resolveShutdown?.();
        this.drain();
      });
      return;
    }
  }
}
