export type Limits = {
  maxRequests: number;
  maxEvents: number;
  maxBytes: number;
  maxActiveGroups?: number;
  maxRetainedBytes?: number;
};
export type Member<T, R> = {
  readonly value: T;
  readonly events: number;
  readonly bytes: number;
  readonly state: 'queued' | 'sealed' | 'cancelled' | 'settled';
  readonly outcome?:
    | { kind: 'committed'; value: R }
    | { kind: 'unknown' }
    | { kind: 'aborted' }
    | { kind: 'error'; error: unknown };
};

type Internal<R> = {
  state: Member<unknown, R>['state'];
  outcome?: Member<unknown, R>['outcome'];
};
const states = new WeakMap<object, Internal<unknown>>();
function internal<T, R>(member: Member<T, R>): Internal<R> {
  const state = states.get(member);
  if (!state) throw new Error('invalid member');
  return state as Internal<R>;
}

const batchKey = Symbol();
declare const borrowedBrand: unique symbol;
export type BorrowedOwnership = { readonly [borrowedBrand]: true };
const borrowedOwners = new WeakMap<
  object,
  { batch: object; owner: () => void }
>();
const timedOutBatches = new WeakSet<object>();
export function hasBatchTimedOut(batch: object): boolean {
  return timedOutBatches.has(batch);
}
export class Batch<T, R> {
  #attempt = 0;
  #done = false;
  #isolating = false;
  #staged = new Map<Member<T, R>, R>();
  #onDone: () => void;
  #holds = 0;
  #owner?: () => void;
  #released = false;
  readonly members: readonly Member<T, R>[];
  constructor(key: symbol, members: Member<T, R>[], onDone: () => void) {
    this.#onDone = onDone;
    if (key !== batchKey) throw new Error('invalid batch owner');
    this.members = Object.freeze([...members]);
    Object.defineProperty(this, 'members', {
      writable: false,
      configurable: false,
    });
  }
  retain(): () => void {
    if (this.#done || this.#holds)
      throw new Error('batch already owned or settled');
    this.#holds++;
    let released = false;
    const release = () => {
      if (released) return;
      if (this.#isolating && !this.#done) throw new Error('isolation pending');
      released = true;
      this.#owner = undefined;
      this.#holds--;
      this.#releaseIfDone();
    };
    this.#owner = release;
    return release;
  }
  borrow(owner: () => void): BorrowedOwnership {
    if (this.#owner !== owner) throw new Error('invalid owner');
    const token = Object.freeze({}) as BorrowedOwnership;
    borrowedOwners.set(token, { batch: this, owner });
    return token;
  }
  ownsBorrow(ownership: BorrowedOwnership): boolean {
    const borrowed = borrowedOwners.get(ownership);
    return borrowed?.batch === this && borrowed.owner === this.#owner;
  }
  #releaseIfDone(): void {
    if (this.#done && !this.#holds && !this.#released) {
      this.#released = true;
      this.#onDone();
    }
  }
  get settled(): boolean {
    return this.#done;
  }
  beginAttempt(): number {
    if (this.#done || this.#isolating) throw new Error('batch settled');
    this.#staged.clear();
    return ++this.#attempt;
  }
  private check(attempt: number): void {
    if (this.#done || this.#isolating || !attempt || attempt !== this.#attempt)
      throw new Error('invalid attempt');
  }
  /** Caller asserts the entire attempt is definitively aborted, never uncertain. */
  isolate(
    attempt: number,
    owner: () => void,
  ): {
    settle: (
      member: Member<T, R>,
      outcome:
        | { kind: 'committed'; value: R }
        | { kind: 'error'; error: unknown }
        | { kind: 'aborted' }
        | { kind: 'unknown' },
    ) => void;
  } {
    this.check(attempt);
    if (!this.#holds || this.#owner !== owner || !this.members.length)
      throw new Error('isolation requires ownership');
    this.#isolating = true;
    this.#staged.clear();
    return Object.freeze({
      settle: (
        member: Member<T, R>,
        outcome:
          | { kind: 'committed'; value: R }
          | { kind: 'error'; error: unknown }
          | { kind: 'aborted' }
          | { kind: 'unknown' },
      ) => {
        if (
          this.#done ||
          !this.#holds ||
          !this.members.includes(member) ||
          member.state !== 'sealed'
        )
          throw new Error('invalid member settlement');
        if (
          !outcome ||
          !['committed', 'error', 'aborted', 'unknown'].includes(outcome.kind)
        )
          throw new Error('invalid outcome');
        internal(member).outcome = Object.freeze({ ...outcome });
        internal(member).state = 'settled';
        if (this.members.every((item) => item.state === 'settled')) {
          this.#done = true;
          this.#releaseIfDone();
        }
      },
    });
  }
  timeout(): void {
    timedOutBatches.add(this);
    if (this.#done) return;
    this.#done = true;
    this.#staged.clear();
    for (const member of this.members) {
      if (member.state !== 'sealed') continue;
      internal(member).outcome = Object.freeze({ kind: 'unknown' });
      internal(member).state = 'settled';
    }
    this.#releaseIfDone();
  }
  stage(attempt: number, member: Member<T, R>, result: R): void {
    this.check(attempt);
    if (!this.members.includes(member) || this.#staged.has(member))
      throw new Error('invalid member');
    this.#staged.set(member, result);
  }
  commit(attempt: number): void {
    this.check(attempt);
    if (this.#staged.size !== this.members.length)
      throw new Error('incomplete attempt');
    this.#done = true;
    this.#releaseIfDone();
    for (const member of this.members) {
      internal(member).outcome = Object.freeze({
        kind: 'committed',
        value: this.#staged.get(member)!,
      });
      internal(member).state = 'settled';
    }
    this.#staged.clear();
  }
  abort(attempt: number): void {
    this.check(attempt);
    this.#done = true;
    this.#releaseIfDone();
    this.#staged.clear();
    for (const member of this.members) {
      internal(member).outcome = Object.freeze({ kind: 'aborted' });
      internal(member).state = 'settled';
    }
  }
  unknown(attempt: number): void {
    this.check(attempt);
    this.#done = true;
    this.#releaseIfDone();
    this.#staged.clear();
    for (const member of this.members) {
      internal(member).outcome = Object.freeze({ kind: 'unknown' });
      internal(member).state = 'settled';
    }
  }
}

export class GroupQueue<T, R> {
  #members: Member<T, R>[] = [];
  #closed = false;
  #active = new Set<Batch<T, R>>();
  #shutdownPromise?: Promise<void>;
  #drain?: () => void;
  #watchers = new Map<
    Member<T, R>,
    { cleanup: () => void; expired: () => boolean }
  >();
  #limits: Limits;
  #maxActiveGroups: number;
  #maxRetainedBytes: number;
  #retainedBytes = 0;
  get activeGroups(): number {
    return this.#active.size;
  }
  get retainedBytes(): number {
    return this.#retainedBytes;
  }
  constructor(limits: Limits) {
    for (const limit of [
      limits.maxRequests,
      limits.maxEvents,
      limits.maxBytes,
      limits.maxActiveGroups ?? limits.maxRequests,
      limits.maxRetainedBytes ?? limits.maxBytes,
    ]) {
      if (!Number.isSafeInteger(limit) || limit <= 0)
        throw new Error('invalid limit');
    }
    this.#limits = { ...limits };
    this.#maxActiveGroups = limits.maxActiveGroups ?? limits.maxRequests;
    this.#maxRetainedBytes = limits.maxRetainedBytes ?? limits.maxBytes;
  }
  enqueue(
    value: T,
    events: number,
    bytes: number,
    options: { signal?: AbortSignal; deadline?: number } = {},
  ): Member<T, R> | undefined {
    if (this.#closed) return undefined;
    options = { ...options };
    if (
      options.deadline !== undefined &&
      !Number.isSafeInteger(options.deadline)
    )
      throw new Error('invalid deadline');
    if (
      !Number.isSafeInteger(events) ||
      events <= 0 ||
      !Number.isSafeInteger(bytes) ||
      bytes < 0
    )
      throw new Error('invalid size');
    if (
      this.#members.length >= this.#limits.maxRequests ||
      this.#members.reduce((n, m) => n + m.events, 0) + events >
        this.#limits.maxEvents ||
      this.#members.reduce((n, m) => n + m.bytes, 0) + bytes >
        this.#limits.maxBytes ||
      bytes > this.#maxRetainedBytes - this.#retainedBytes
    )
      return undefined;
    const state: Internal<R> = { state: 'queued' };
    const member: Member<T, R> = Object.freeze({
      value,
      events,
      bytes,
      get state() {
        return state.state;
      },
      get outcome() {
        return state.outcome;
      },
    });
    states.set(member, state);
    this.#members.push(member);
    this.#retainedBytes += bytes;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = () =>
      !!options.signal?.aborted ||
      (options.deadline !== undefined && Date.now() >= options.deadline);
    const onAbort = () => {
      this.cancel(member);
    };
    const cleanup = () => {
      if (timer !== undefined) clearTimeout(timer);
      options.signal?.removeEventListener('abort', onAbort);
      this.#watchers.delete(member);
    };
    this.#watchers.set(member, { cleanup, expired });
    options.signal?.addEventListener('abort', onAbort, { once: true });
    const schedule = () => {
      if (expired()) {
        this.cancel(member);
        return;
      }
      if (options.deadline !== undefined)
        timer = setTimeout(
          schedule,
          Math.min(options.deadline - Date.now(), 2147483647),
        );
    };
    schedule();
    return member;
  }
  shutdown(timeout: number): Promise<void> {
    if (!Number.isSafeInteger(timeout) || timeout < 0 || timeout > 2147483647)
      throw new Error('invalid shutdown timeout');
    if (this.#shutdownPromise) return this.#shutdownPromise;
    this.#closed = true;
    for (const member of [...this.#members]) this.cancel(member);
    if (!this.#active.size) return (this.#shutdownPromise = Promise.resolve());
    this.#shutdownPromise = new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        for (const batch of [...this.#active]) {
          batch.timeout();
        }
        this.#drain?.();
      }, timeout);
      this.#drain = () => {
        clearTimeout(timer);
        this.#drain = undefined;
        resolve();
      };
    });
    return this.#shutdownPromise;
  }
  cancel(member: Member<T, R>): boolean {
    const index = this.#members.indexOf(member);
    if (index < 0) return false;
    this.#members.splice(index, 1);
    this.#retainedBytes -= member.bytes;
    this.#watchers.get(member)?.cleanup();
    internal(member).state = 'cancelled';
    return true;
  }
  seal(max: number, distinctKey?: (value: T) => string): Batch<T, R> {
    if (this.#closed) throw new Error('queue closed');
    if (!Number.isSafeInteger(max) || max <= 0)
      throw new Error('invalid batch size');
    for (const member of [...this.#members]) {
      if (this.#watchers.get(member)?.expired()) this.cancel(member);
    }
    if (this.#members.length && this.#active.size >= this.#maxActiveGroups)
      throw new Error('group capacity');
    const selected = new Set<Member<T, R>>();
    const keys = new Set<string>();
    for (const member of this.#members) {
      if (selected.size >= max) break;
      if (distinctKey) {
        const key = distinctKey(member.value);
        if (keys.has(key)) continue;
        keys.add(key);
      }
      selected.add(member);
    }
    const members = [...selected];
    this.#members = this.#members.filter((member) => !selected.has(member));
    for (const member of members) {
      this.#watchers.get(member)?.cleanup();
      internal(member).state = 'sealed';
    }
    const batch = new Batch(batchKey, members, () => {
      this.#active.delete(batch);
      for (const member of members) this.#retainedBytes -= member.bytes;
      if (!this.#active.size) this.#drain?.();
    });
    if (members.length) this.#active.add(batch);
    return batch;
  }
}
