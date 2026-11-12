// Disposable test tracer only. This is not a production RPC protocol.
import { createConnection, createServer, Socket } from 'node:net';
import { Db, MongoClient } from 'mongodb';
import { protectedWrite, Token } from './mongodb-fenced-tracer';
import { SerialAdmission } from './serial-admission';

export type Outcome = 'committed' | 'rejected' | 'unknown';

export async function startLeader(
  client: MongoClient,
  db: Db,
  token: Token,
  capacity: number,
  beforeFence?: () => Promise<void>,
  beforeCommit?: () => Promise<void>,
  afterCommit?: () => Promise<void>,
) {
  const admission = new SerialAdmission(capacity);
  let active = 0;
  let peak = 0;
  const sockets = new Set<Socket>();
  const pending = new Set<Promise<void>>();
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});
    let input = '';
    socket.on('data', (chunk: Buffer) => {
      input += chunk.toString('utf8');
      if (input.length > 1024) {
        socket.destroy();
        return;
      }
      const end = input.indexOf('\n');
      if (end < 0) return;
      socket.removeAllListeners('data');
      const name = input.slice(0, end);
      if (!name) {
        socket.end('rejected\n');
        return;
      }
      const task = admission
        .run(async () => {
          active++;
          peak = Math.max(peak, active);
          try {
            await protectedWrite(
              client,
              db,
              token,
              name,
              beforeFence,
              beforeCommit,
            );
            await afterCommit?.();
          } finally {
            active--;
          }
        })
        .then(
          () => {
            socket.end('committed\n');
          },
          (error: Error) => {
            socket.end(
              error.message === 'admission full' ? 'rejected\n' : 'unknown\n',
            );
          },
        );
      pending.add(task);
      void task.then(() => pending.delete(task));
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (!address || typeof address === 'string')
    throw new Error('missing address');
  return {
    port: address.port,
    maxActive: () => peak,
    close: async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
        for (const socket of sockets) socket.destroy();
      });
      await Promise.all(pending);
    },
  };
}

export function submit(
  port: number,
  name: string,
  timeout: number,
): Promise<Outcome> {
  return new Promise((resolve) => {
    let connected = false;
    let settled = false;
    let response = '';
    const socket = createConnection({ port, host: '127.0.0.1' });
    const finish = (result: Outcome) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(result);
    };
    socket.setTimeout(timeout, () =>
      finish(connected ? 'unknown' : 'rejected'),
    );
    socket.on('connect', () => {
      connected = true;
      socket.write(`${name}\n`);
    });
    socket.on('data', (data: Buffer) => {
      response += data.toString('utf8');
      if (response.includes('\n')) {
        const value = response.split('\n')[0];
        finish(
          value === 'committed' || value === 'rejected' ? value : 'unknown',
        );
      }
    });
    socket.on('error', () => finish(connected ? 'unknown' : 'rejected'));
    socket.on('close', () => finish(connected ? 'unknown' : 'rejected'));
  });
}
