// Test-only, process-local admission. Capacity includes the running call.
export class SerialAdmission {
  private pending = 0;
  private tail: Promise<void> = Promise.resolve();

  constructor(private readonly capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1)
      throw new Error('invalid admission capacity');
  }

  run<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    if (this.pending >= this.capacity)
      return Promise.reject(new Error('admission full'));
    this.pending++;
    const result = this.tail.then(() => {
      if (signal?.aborted) throw new Error('admission cancelled');
      return operation();
    });
    this.tail = result.then(
      () => {
        this.pending--;
      },
      () => {
        this.pending--;
      },
    );
    return result;
  }
}
