import { spawn, spawnSync, ChildProcess } from 'node:child_process';
import { connect } from 'node:net';
import { join } from 'node:path';
import {
  KurrentDBClient,
  jsonEvent,
  NO_STREAM,
} from '@kurrent/kurrentdb-client';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { getFreePort } from './util/get-free-port';

describe('actual adapter gRPC receive limit', () => {
  jest.setTimeout(120000);
  let mongo: StartedTestContainer;
  let server: ChildProcess;
  let client: KurrentDBClient;
  let port: number;
  let output = '';
  let payloadLogged = false;

  beforeAll(async () => {
    mongo = await new GenericContainer('mongo:7')
      .withCommand(['mongod', '--replSet', 'rs0', '--bind_ip_all'])
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage('Waiting for connections'))
      .start();
    const init = await mongo.exec([
      'mongosh',
      '--quiet',
      '--eval',
      "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]}); while (!db.hello().isWritablePrimary) sleep(200)",
    ]);
    if (init.exitCode !== 0) throw new Error(init.output);
    port = await getFreePort();
    const internalPort = await getFreePort();
    server = spawn(
      process.execPath,
      ['-r', 'ts-node/register', join(__dirname, '../src/main.ts')],
      {
        cwd: join(__dirname, '..'),
        env: {
          ...process.env,
          EVENT_STORE_BACKEND: 'mongodb',
          MONGODB_URI: `mongodb://${mongo.getHost()}:${mongo.getMappedPort(27017)}/?directConnection=true`,
          MONGODB_DATABASE: 'grpc_limit',
          GRPC_URL: `127.0.0.1:${port}`,
          INTERNAL_GRPC_URL: `127.0.0.1:${internalPort}`,
          GRPC_MAX_RECEIVE_MESSAGE_BYTES: '65536',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    );
    const capture = (chunk: Buffer) => {
      const text = chunk.toString();
      payloadLogged ||= (output.slice(-999) + text).includes('x'.repeat(1000));
      output = (output + text).slice(-16384);
    };
    server.stdout!.on('data', capture);
    server.stderr!.on('data', capture);
    const until = Date.now() + 30000;
    while (true) {
      if (server.exitCode !== null) throw new Error(`server exited: ${output}`);
      const ready = await new Promise<boolean>((resolve) => {
        const socket = connect(port, '127.0.0.1');
        socket.once('connect', () => {
          socket.destroy();
          resolve(true);
        });
        socket.once('error', () => resolve(false));
      });
      if (ready) break;
      if (Date.now() > until)
        throw new Error(`server startup timed out: ${output}`);
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    client = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
  });

  afterAll(async () => {
    await client?.dispose();
    if (server && server.exitCode === null) {
      const exited = new Promise<void>((resolve) =>
        server.once('exit', () => resolve()),
      );
      server.kill('SIGTERM');
      await exited;
    }
    await mongo?.stop();
  });

  it('fails startup on an unsafe limit without logging the supplied value', () => {
    const invalid = '9'.repeat(10000);
    const result = spawnSync(
      process.execPath,
      ['-r', 'ts-node/register', join(__dirname, '../src/main.ts')],
      {
        cwd: join(__dirname, '..'),
        env: { ...process.env, GRPC_MAX_RECEIVE_MESSAGE_BYTES: invalid },
        encoding: 'utf8',
        timeout: 15000,
      },
    );
    expect(result.status).toBe(1);
    const logs = result.stdout + result.stderr;
    expect(logs).toContain('GRPC_MAX_RECEIVE_MESSAGE_BYTES');
    expect(logs.length).toBeLessThan(5000);
    expect(logs).not.toContain(invalid);
  });

  it('preserves eight parallel fragmented official-client appends', async () => {
    const groups = Array.from({ length: 8 }, (_, group) =>
      Array.from({ length: 40 }, (_, index) =>
        jsonEvent({
          type: 'fragment',
          data: { group, index, payload: 'v'.repeat(200) },
        }),
      ),
    );
    const results = await Promise.all(
      groups.map((events, group) =>
        client.appendToStream(`parallel-${group}`, events, {
          streamState: NO_STREAM,
          batchAppendSize: 700,
          deadline: 10000,
        }),
      ),
    );
    expect(results.every((result) => result.success)).toBe(true);
    for (const [group, events] of groups.entries()) {
      const ids: string[] = [];
      for await (const { event } of client.readStream(`parallel-${group}`))
        if (event) ids.push(event.id);
      expect(ids).toEqual(events.map((event) => event.id));
    }
  });

  it('rejects an oversized message and permits a later normal append and read', async () => {
    const oversized = jsonEvent({
      type: 'large',
      data: { payload: 'x'.repeat(100000) },
    });
    const result = await Promise.race([
      client
        .appendToStream('oversized', oversized, {
          streamState: NO_STREAM,
          deadline: 3000,
        })
        .then(
          () => ({ ok: true, error: '' }),
          (error: Error) => ({ ok: false, error: String(error) }),
        ),
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error('oversized append did not settle')),
          8000,
        ).unref(),
      ),
    ]);
    expect(result.ok).toBe(false);
    expect(result.error.length).toBeLessThan(1000);
    expect(result.error).toMatch(
      /RESOURCE_EXHAUSTED|received message larger|larger than max|exhausted/i,
    );
    await client.dispose();
    client = KurrentDBClient.connectionString`kurrentdb://127.0.0.1:${port}?tls=false`;
    const event = jsonEvent({ type: 'normal', data: { value: 42 } });
    expect(
      (await client.appendToStream('normal', event, { streamState: NO_STREAM }))
        .success,
    ).toBe(true);
    const ids: string[] = [];
    for await (const { event: read } of client.readStream('normal'))
      if (read) ids.push(read.id);
    expect(ids).toEqual([event.id]);
    expect(payloadLogged).toBe(false);
  });
});
