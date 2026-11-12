import { Db, MongoClient } from 'mongodb';

export type Token = { owner: string; epoch: number };
type Fence = {
  _id: string;
  owner: string | null;
  epoch: number;
  writes: number;
};
const concern = { w: 'majority' as const, j: true };

// Explicit compare-and-swap takeover only; no clock-based expiry is assumed.
export async function acquire(
  db: Db,
  owner: string | null,
  epoch: number,
  next: string,
): Promise<Token> {
  if (
    !next ||
    !Number.isSafeInteger(epoch) ||
    epoch < 0 ||
    epoch >= Number.MAX_SAFE_INTEGER
  )
    throw new Error('invalid fence token');
  const doc = await db
    .collection<Fence>('fence')
    .findOneAndUpdate(
      { _id: 'writer', owner, epoch },
      { $set: { owner: next }, $inc: { epoch: 1 } },
      { returnDocument: 'after', writeConcern: concern },
    );
  if (!doc) throw new Error('fence acquisition rejected');
  return { owner: next, epoch: doc.epoch };
}

export async function protectedWrite(
  client: MongoClient,
  db: Db,
  token: Token,
  name: string,
  beforeFence?: () => Promise<void>,
  beforeCommit?: () => Promise<void>,
): Promise<void> {
  const session = client.startSession();
  try {
    session.startTransaction({ writeConcern: concern });
    await beforeFence?.();
    const fence = await db.collection<Fence>('fence').findOneAndUpdate(
      {
        _id: 'writer',
        owner: token.owner,
        epoch: token.epoch,
        writes: { $lt: Number.MAX_SAFE_INTEGER },
      },
      { $inc: { writes: 1 } },
      { session, returnDocument: 'after' },
    );
    if (!fence) throw new Error('fence rejected');
    await db
      .collection('events')
      .insertOne({ name, position: fence.writes }, { session });
    await beforeCommit?.();
    await session.commitTransaction();
  } finally {
    await session.endSession();
  }
}
