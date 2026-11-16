import { Db, Document, Timestamp } from 'mongodb';
import { MongoSubscriptionNotifier } from './mongodb-subscription-notifier';

class FakeChangeStream {
  private readonly changes: Document[] = [];
  private closed = false;
  private wake?: () => void;

  push(change: Document): void {
    this.changes.push(change);
    this.wake?.();
  }

  close(): Promise<void> {
    this.closed = true;
    this.wake?.();
    return Promise.resolve();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<Document> {
    while (!this.closed) {
      if (this.changes.length === 0) {
        await new Promise<void>((resolve) => {
          this.wake = resolve;
        });
        this.wake = undefined;
      }
      if (this.closed) return;
      const change = this.changes.shift();
      if (change) yield change;
    }
  }
}

function createFixture(): {
  changeStream: FakeChangeStream;
  db: Db;
  watch: jest.Mock;
} {
  const changeStream = new FakeChangeStream();
  const watch = jest.fn().mockReturnValue(changeStream);
  const db = {
    command: jest.fn().mockResolvedValue({
      operationTime: new Timestamp({ t: 1, i: 1 }),
    }),
    watch,
  } as unknown as Db;
  return { changeStream, db, watch };
}

test('uses one database change stream to wake subscription waiters', async () => {
  const { changeStream, db, watch } = createFixture();
  const notifier = new MongoSubscriptionNotifier();
  await notifier.start(db);
  const observed = notifier.observe();
  const waiting = notifier.waitForChange(observed);

  changeStream.push({ operationType: 'insert' });
  await waiting;

  expect(notifier.observe()).toBe(observed + 1);
  expect(watch).toHaveBeenCalledWith(
    [
      {
        $match: {
          'ns.coll': {
            $in: ['stream_events', 'streams', 'retention'],
          },
        },
      },
    ],
    { startAtOperationTime: new Timestamp({ t: 1, i: 1 }) },
  );
  await notifier.close();
});

test('does not miss a change that arrives before the waiter is registered', async () => {
  const { changeStream, db } = createFixture();
  const notifier = new MongoSubscriptionNotifier();
  await notifier.start(db);
  const observed = notifier.observe();

  changeStream.push({ operationType: 'insert' });
  await new Promise<void>((resolve) => setImmediate(resolve));

  await expect(notifier.waitForChange(observed)).resolves.toBeUndefined();
  await notifier.close();
});

test('closing releases idle subscription waiters', async () => {
  const { db } = createFixture();
  const notifier = new MongoSubscriptionNotifier();
  await notifier.start(db);
  const waiting = notifier.waitForChange(notifier.observe());

  await notifier.close();

  await expect(waiting).resolves.toBeUndefined();
});
