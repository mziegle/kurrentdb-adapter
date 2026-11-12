import { Batch, GroupQueue, Limits, Member } from './group-commit';
import { GroupDispatchError } from './group-dispatcher';

type Pending<T, R> = {
  member: Member<T, R>;
  resolve: (value: R) => void;
  reject: (reason: unknown) => void;
  cleanup: () => void;
  expire: () => void;
};

/** Experimental opt-in dispatcher. The runner must finalize before any session work. */
export class CooperativeDispatcher<T, R> {
  private readonly queue: GroupQueue<T, R>;
  private readonly pending = new Map<Member<T, R>, Pending<T, R>>();
  private timer?: ReturnType<typeof setTimeout>;
  private running = false;
  private closed = false;
  private shutdownPromise?: Promise<void>;

  constructor(
    private readonly limits: Limits,
    private readonly waitMs: number,
    private readonly key: (value: T) => string,
    private readonly runner: (
      finalize: () => Batch<T, R> | undefined,
    ) => Promise<void>,
  ) {
    if (!Number.isSafeInteger(waitMs) || waitMs < 0 || waitMs > 2147483647)
      throw new Error('invalid wait window');
    this.queue = new GroupQueue(limits);
  }

  enqueue(
    value: T,
    events: number,
    bytes: number,
    options: { signal?: AbortSignal; deadline?: number } = {},
  ): Promise<R> {
    const { signal, deadline } = options;
    if (deadline !== undefined && !Number.isSafeInteger(deadline))
      throw new Error('invalid deadline');
    if (this.closed) return Promise.reject(new GroupDispatchError('closed'));
    if (signal?.aborted || (deadline !== undefined && deadline <= Date.now()))
      return Promise.reject(new GroupDispatchError('cancelled'));
    for (const pending of this.pending.values()) pending.expire();
    const member = this.queue.enqueue(value, events, bytes);
    if (!member) return Promise.reject(new GroupDispatchError('capacity'));
    const result = new Promise<R>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const cleanup = () => {
        if (timer !== undefined) clearTimeout(timer);
        signal?.removeEventListener('abort', cancel);
      };
      const cancel = () => {
        if (!this.pending.has(member)) return;
        const removed = this.queue.cancel(member);
        cleanup();
        this.pending.delete(member);
        reject(new GroupDispatchError(removed ? 'cancelled' : 'unknown'));
        if (![...this.pending.keys()].some((item) => item.state === 'queued'))
          this.clearTimer();
      };
      const expire = () => {
        if (
          signal?.aborted ||
          (deadline !== undefined && Date.now() >= deadline)
        )
          cancel();
      };
      this.pending.set(member, { member, resolve, reject, cleanup, expire });
      signal?.addEventListener('abort', cancel, { once: true });
      const schedule = () => {
        if (deadline === undefined) return;
        const remaining = deadline - Date.now();
        if (remaining <= 0) cancel();
        else timer = setTimeout(schedule, Math.min(remaining, 2147483647));
      };
      schedule();
    });
    if (!this.pending.has(member)) return result;
    if (!this.timer && !this.running)
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.dispatch();
      }, this.waitMs);
    const queued = [...this.pending.keys()].filter(
      (item) => item.state === 'queued',
    );
    if (
      queued.length >= this.limits.maxRequests ||
      queued.reduce((n, m) => n + m.events, 0) >= this.limits.maxEvents ||
      queued.reduce((n, m) => n + m.bytes, 0) >= this.limits.maxBytes
    )
      this.dispatch();
    return result;
  }

  private clearTimer(): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
  }

  private settle(): void {
    for (const [member, pending] of this.pending) {
      if (member.state !== 'settled') continue;
      pending.expire();
      if (!this.pending.has(member)) continue;
      pending.cleanup();
      this.pending.delete(member);
      const outcome = member.outcome;
      if (outcome?.kind === 'committed') pending.resolve(outcome.value);
      else if (outcome?.kind === 'error') pending.reject(outcome.error);
      else
        pending.reject(
          new GroupDispatchError(
            outcome?.kind === 'aborted' ? 'aborted' : 'unknown',
          ),
        );
    }
  }

  private dispatch(): void {
    if (this.running || this.closed) return;
    this.clearTimer();
    this.running = true;
    let batch: Batch<T, R> | undefined;
    let release: (() => void) | undefined;
    let called = false;
    let finished = false;
    const finalize = () => {
      if (called || finished) throw new Error('finalize already used');
      called = true;
      if (this.closed) return undefined;
      for (const pending of this.pending.values()) pending.expire();
      batch = this.queue.seal(this.limits.maxRequests, this.key);
      if (!batch.members.length) {
        batch = undefined;
        return undefined;
      }
      release = batch.retain();
      return batch;
    };
    let cleaned = false;
    void Promise.resolve()
      .then(() => this.runner(finalize))
      .then(() => {
        cleaned = true;
      })
      .catch(() => undefined)
      .then(() => {
        finished = true;
        if (!batch && !this.closed) {
          for (const [member, pending] of this.pending) {
            if (!this.queue.cancel(member)) continue;
            pending.cleanup();
            this.pending.delete(member);
            pending.reject(new GroupDispatchError('aborted'));
          }
        }
        if (batch && !batch.settled) batch.timeout();
        this.settle();
        if (cleaned) release?.();
        this.running = false;
        if (
          !this.closed &&
          [...this.pending.keys()].some((member) => member.state === 'queued')
        )
          this.dispatch();
      });
  }

  shutdown(timeout: number): Promise<void> {
    if (this.shutdownPromise) return this.shutdownPromise;
    if (!Number.isSafeInteger(timeout) || timeout < 0 || timeout > 2147483647)
      throw new Error('invalid shutdown timeout');
    this.closed = true;
    this.clearTimer();
    const draining = this.queue.shutdown(timeout);
    for (const [member, pending] of this.pending) {
      if (member.state !== 'cancelled') continue;
      pending.cleanup();
      this.pending.delete(member);
      pending.reject(new GroupDispatchError('closed'));
    }
    this.shutdownPromise = draining.then(() => this.settle());
    return this.shutdownPromise;
  }
}
