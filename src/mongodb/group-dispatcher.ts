import { Batch, GroupQueue, Limits, Member } from './group-commit';

export class GroupDispatchError extends Error {
  constructor(
    readonly code: 'capacity' | 'closed' | 'cancelled' | 'aborted' | 'unknown',
    cause?: unknown,
  ) {
    super(`group dispatch ${code}`, { cause });
  }
}

type Pending<T, R> = {
  member: Member<T, R>;
  resolve: (value: R) => void;
  reject: (error: unknown) => void;
  cleanup: () => void;
  expireIfDue: () => void;
};

/** An isolated dispatcher. The injected runner may delegate to runMongoGroup. */
export class GroupDispatcher<T, R> {
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
    private readonly runner: (batch: Batch<T, R>) => Promise<void>,
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
    if (
      options.deadline !== undefined &&
      !Number.isSafeInteger(options.deadline)
    )
      throw new Error('invalid deadline');
    if (this.closed) return Promise.reject(new GroupDispatchError('closed'));
    if (
      options.signal?.aborted ||
      (options.deadline !== undefined && options.deadline <= Date.now())
    )
      return Promise.reject(new GroupDispatchError('cancelled'));
    for (const item of this.pending.values()) item.expireIfDue();
    const member = this.queue.enqueue(value, events, bytes);
    if (!member) return Promise.reject(new GroupDispatchError('capacity'));
    const promise = new Promise<R>((resolve, reject) => {
      let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
      const onAbort = () => {
        if (this.queue.cancel(member)) {
          this.pending.get(member)?.cleanup();
          this.pending.delete(member);
          reject(new GroupDispatchError('cancelled'));
          if (!this.pending.size) this.clearTimer();
        } else {
          this.pending.get(member)?.cleanup();
          this.pending.delete(member);
          reject(new GroupDispatchError('unknown'));
        }
      };
      const cleanup = () => {
        if (deadlineTimer !== undefined) clearTimeout(deadlineTimer);
        options.signal?.removeEventListener('abort', onAbort);
      };
      const expireIfDue = () => {
        if (
          options.signal?.aborted ||
          (options.deadline !== undefined && Date.now() >= options.deadline)
        )
          onAbort();
      };
      this.pending.set(member, {
        member,
        resolve,
        reject,
        cleanup,
        expireIfDue,
      });
      options.signal?.addEventListener('abort', onAbort, { once: true });
      const schedule = () => {
        if (options.deadline === undefined) return;
        const remaining = options.deadline - Date.now();
        if (remaining <= 0) onAbort();
        else
          deadlineTimer = setTimeout(schedule, Math.min(remaining, 2147483647));
      };
      schedule();
    });
    if (!this.pending.has(member)) return promise;
    if (!this.timer)
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.dispatch();
      }, this.waitMs);
    const queued = [...this.pending.values()].filter(
      (item) => item.member.state === 'queued',
    );
    if (
      queued.length >= this.limits.maxRequests ||
      queued.reduce((sum, item) => sum + item.member.events, 0) >=
        this.limits.maxEvents ||
      queued.reduce((sum, item) => sum + item.member.bytes, 0) >=
        this.limits.maxBytes
    )
      this.dispatch();
    return promise;
  }
  private clearTimer(): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
  }
  private dispatch(): void {
    if (this.running || this.closed) return;
    this.clearTimer();
    for (const item of this.pending.values()) item.expireIfDue();
    let batch: Batch<T, R>;
    try {
      batch = this.queue.seal(this.limits.maxRequests, this.key);
    } catch (error) {
      const code =
        this.queue.activeGroups >=
        (this.limits.maxActiveGroups ?? this.limits.maxRequests)
          ? 'capacity'
          : 'unknown';
      for (const [member, pending] of this.pending) {
        if (!this.queue.cancel(member)) continue;
        pending.cleanup();
        this.pending.delete(member);
        pending.reject(new GroupDispatchError(code, error));
      }
      return;
    }
    if (!batch.members.length) return;
    this.running = true;
    void Promise.resolve()
      .then(() => this.runner(batch))
      .catch((error: unknown) => {
        if (!batch.settled) batch.timeout();
        return error;
      })
      .then((error: unknown) => {
        if (!batch.settled) batch.timeout();
        for (const member of batch.members) {
          const pending = this.pending.get(member);
          if (!pending) continue;
          pending.expireIfDue();
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
                error,
              ),
            );
        }
        this.running = false;
        if (
          !this.closed &&
          [...this.pending.values()].some(
            (item) => item.member.state === 'queued',
          )
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
      if (member.state === 'cancelled') {
        pending.cleanup();
        this.pending.delete(member);
        pending.reject(new GroupDispatchError('closed'));
      }
    }
    this.shutdownPromise = draining.then(() => {
      for (const [member, pending] of this.pending) {
        if (member.state !== 'settled') continue;
        pending.expireIfDue();
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
    });
    return this.shutdownPromise;
  }
}
