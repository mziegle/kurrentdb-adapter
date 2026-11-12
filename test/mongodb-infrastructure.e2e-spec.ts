import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';
import { MongoClient } from 'mongodb';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { MongoInfrastructureService } from '../src/mongodb/mongodb-infrastructure.service';

describe('MongoDB event-store infrastructure', () => {
  jest.setTimeout(120_000);
  let container: StartedTestContainer;
  let uri: string;

  beforeAll(async () => {
    container = await new GenericContainer('mongo:7')
      .withCommand(['mongod', '--replSet', 'rs0', '--bind_ip_all'])
      .withExposedPorts(27017)
      .withWaitStrategy(Wait.forLogMessage('Waiting for connections'))
      .start();
    const result = await container.exec([
      'mongosh',
      '--quiet',
      '--eval',
      "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})",
    ]);
    if (result.exitCode !== 0) throw new Error(result.output);
    const election = await container.exec([
      'mongosh',
      '--quiet',
      '--eval',
      'while (!db.hello().isWritablePrimary) { sleep(200); }',
    ]);
    if (election.exitCode !== 0) throw new Error(election.output);
    uri = `mongodb://${container.getHost()}:${container.getMappedPort(27017)}/?directConnection=true`;
  });

  afterAll(async () => {
    await container?.stop();
  });

  it('does not silently downgrade custom acknowledgement policies', () => {
    expect(() => new MongoInfrastructureService(`${uri}&w=3`, 'test')).toThrow(
      /write concern/i,
    );
  });

  it('commits transactions with majority and journal acknowledgement despite weak URI settings', async () => {
    const infrastructure = new MongoInfrastructureService(
      `${uri}&w=1&journal=false`,
      'durability_test',
    );
    const client = infrastructure.getClient();
    Object.assign(client.options, { monitorCommands: true });
    const commits: object[] = [];
    client.on('commandStarted', (event) => {
      if (event.commandName === 'commitTransaction')
        commits.push(event.command);
    });
    try {
      await infrastructure.connect();
      const session = client.startSession();
      try {
        await session.withTransaction(async () => {
          await client
            .db('durability_test')
            .collection('events')
            .insertOne({ value: 'committed' }, { session });
        });
      } finally {
        await session.endSession();
      }
      expect(commits).toHaveLength(1);
      expect(commits[0]).toMatchObject({
        writeConcern: { w: 'majority', j: true },
      });
    } finally {
      await infrastructure.close();
    }
  });

  it('retains committed data and reconnects after a network outage', async () => {
    const infrastructure = new MongoInfrastructureService(
      `${uri}&serverSelectionTimeoutMS=2000&heartbeatFrequencyMS=500&socketTimeoutMS=2000`,
      'restart_test',
    );
    const client = infrastructure.getClient();
    try {
      await infrastructure.connect();
      const events = client.db('restart_test').collection('events');
      const session = client.startSession();
      try {
        await session.withTransaction(async () => {
          await events.insertOne({ value: 'durable' }, { session });
        });
        await expect(
          session.withTransaction(async () => {
            await events.insertOne({ value: 'aborted' }, { session });
            throw new Error('abort transaction');
          }),
        ).rejects.toThrow('abort transaction');
      } finally {
        await session.endSession();
      }
      const docker = promisify(execFile);
      await docker('docker', ['pause', container.getId()]);
      try {
        await expect(client.db('admin').command({ ping: 1 })).rejects.toThrow();
      } finally {
        await docker('docker', ['unpause', container.getId()]);
      }
      expect(await events.find().toArray()).toEqual([
        expect.objectContaining({ value: 'durable' }),
      ]);
      await events.insertOne({ value: 'after-restart' });
      expect(await events.countDocuments()).toBe(2);
    } finally {
      await infrastructure.close();
    }
  });

  it('upgrades the legacy unique event ID index without deleting events', async () => {
    const client = new MongoClient(uri);
    const infrastructure = new MongoInfrastructureService(uri, 'upgrade_test');
    try {
      await client.connect();
      const events = client.db('upgrade_test').collection('stream_events');
      await events.createIndex({ eventId: 1 }, { unique: true });
      await events.insertOne({
        streamName: 'a',
        generation: 0,
        revision: 0,
        eventId: 'same',
        position: 1,
      });
      await infrastructure.connect();
      await events.insertOne({
        streamName: 'b',
        generation: 0,
        revision: 0,
        eventId: 'same',
        position: 2,
      });
      expect(await events.countDocuments({ eventId: 'same' })).toBe(2);
      expect(
        (await events.indexes()).some(
          (index) => index.name === 'eventId_1' && index.unique,
        ),
      ).toBe(false);
    } finally {
      await infrastructure.close();
      await client.close();
    }
  });

  it('initializes indexes and supports atomic transactions on a replica set', async () => {
    const infrastructure = new MongoInfrastructureService(uri, 'adapter_test');
    const client = new MongoClient(uri);
    try {
      await infrastructure.connect();
      await client.connect();
      const db = client.db('adapter_test');
      const indexes = await db.collection('stream_events').indexes();
      expect(indexes).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            key: { streamName: 1, generation: 1, revision: 1 },
            unique: true,
          }),

          expect.objectContaining({ key: { position: 1 }, unique: true }),
        ]),
      );
      const session = client.startSession();
      try {
        await expect(
          session.withTransaction(async () => {
            await db.collection('stream_events').insertOne(
              {
                streamName: 'test',
                generation: 0,
                revision: 0,
                eventId: 'one',
                position: 1,
              },
              { session },
            );
            throw new Error('abort');
          }),
        ).rejects.toThrow('abort');
        expect(await db.collection('stream_events').countDocuments()).toBe(0);
      } finally {
        await session.endSession();
      }
    } finally {
      await client.close();
      await infrastructure.close();
    }
  });
});
