import { ChangeStream, Db, Document, Timestamp } from 'mongodb';

type Waiter = {
  reject: (error: unknown) => void;
  resolve: () => void;
};

/**
 * A process-wide wake-up source for MongoDB subscriptions.
 *
 * Revisions and global positions remain the authoritative subscription
 * cursors. The change stream only tells subscribers to query those cursors
 * again, so no MongoDB resume token escapes into the KurrentDB protocol.
 */
export class MongoSubscriptionNotifier {
  private changeStream?: ChangeStream<Document>;
  private consumeTask?: Promise<void>;
  private failure?: Error;
  private closed = false;
  private version = 0;
  private readonly waiters = new Set<Waiter>();

  async start(db: Db): Promise<void> {
    if (this.changeStream) return;
    if (this.closed) throw new Error('MongoDB subscription notifier is closed');

    const hello: Document = await db.command({ hello: 1 });
    const operationTime: unknown = hello.operationTime;
    if (!(operationTime instanceof Timestamp)) {
      throw new Error(
        'MongoDB did not provide an operation time for subscription change streams',
      );
    }

    this.changeStream = db.watch(
      [
        {
          $match: {
            'ns.coll': {
              $in: ['stream_events', 'streams', 'retention'],
            },
          },
        },
      ],
      { startAtOperationTime: operationTime },
    );
    this.consumeTask = this.consume(this.changeStream);
  }

  observe(): number {
    return this.version;
  }

  waitForChange(observedVersion: number): Promise<void> {
    if (this.failure !== undefined) return Promise.reject(this.failure);
    if (this.closed || this.version !== observedVersion) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve, reject) => {
      const waiter = { resolve, reject };
      this.waiters.add(waiter);

      // Recheck after registration so a change cannot land between the first
      // version check and adding the waiter.
      if (this.failure !== undefined) {
        this.waiters.delete(waiter);
        reject(this.failure);
      } else if (this.closed || this.version !== observedVersion) {
        this.waiters.delete(waiter);
        resolve();
      }
    });
  }

  async close(): Promise<void> {
    if (this.closed) {
      await this.consumeTask;
      return;
    }
    this.closed = true;
    this.resolveWaiters();
    await this.changeStream?.close();
    await this.consumeTask;
  }

  private async consume(changeStream: ChangeStream<Document>): Promise<void> {
    try {
      for await (const change of changeStream) {
        void change;
        if (this.closed) break;
        this.version += 1;
        this.resolveWaiters();
      }
    } catch (error) {
      if (this.closed) return;
      const failure =
        error instanceof Error
          ? error
          : new Error('MongoDB change stream failed');
      this.failure = failure;
      for (const waiter of this.waiters) waiter.reject(failure);
      this.waiters.clear();
    }
  }

  private resolveWaiters(): void {
    for (const waiter of this.waiters) waiter.resolve();
    this.waiters.clear();
  }
}
