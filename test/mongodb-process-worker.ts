// Disposable child processes for the fixed-leader crash experiment only.
import { MongoClient } from 'mongodb';
import { acquire, protectedWrite, Token } from './mongodb-fenced-tracer';
import { startLeader, submit } from './mongodb-fixed-leader';

async function main() {
  const [role, arg, rawToken] = process.argv.slice(2);
  if (role === 'client') {
    const outcome = await submit(Number(arg), 'staged-old', 30000);
    process.send?.({ kind: 'result', outcome });
    return;
  }
  const mongo = new MongoClient(arg, { serverSelectionTimeoutMS: 5000 });
  await mongo.connect();
  const db = mongo.db('crash');
  const token = JSON.parse(rawToken) as Token;
  if (role === 'leader' || role === 'leader-committed') {
    const gate = async (kind: string) => {
      process.send?.({ kind });
      await new Promise<void>(() => {});
    };
    const leader = await startLeader(
      mongo,
      db,
      token,
      1,
      undefined,
      role === 'leader' ? () => gate('staged') : undefined,
      role === 'leader-committed' ? () => gate('committed-gate') : undefined,
    );
    process.send?.({ kind: 'ready', port: leader.port });
    return;
  }
  try {
    const next = await acquire(db, token.owner, token.epoch, 'new');
    await protectedWrite(mongo, db, next, 'new-1');
    await protectedWrite(mongo, db, next, 'new-2');
    process.send?.({ kind: 'completed', epoch: next.epoch });
  } finally {
    await mongo.close();
  }
}
void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
