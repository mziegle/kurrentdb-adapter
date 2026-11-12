import { MongoClient, MongoServerError } from 'mongodb';

/** Connection and index initialization for the future MongoDB event store. */
export class MongoInfrastructureService {
  private readonly client: MongoClient;

  constructor(
    uri: string,
    private readonly databaseName: string,
  ) {
    // A numeric or tagged policy may be stronger than majority. Refuse to
    // silently replace one with a weaker or incomparable policy.
    const requested = new MongoClient(uri).options.writeConcern?.w;
    if (
      requested !== undefined &&
      requested !== 'majority' &&
      requested !== 0 &&
      requested !== 1
    ) {
      throw new Error(
        'Custom MongoDB write concern cannot be safely overridden',
      );
    }
    this.client = new MongoClient(uri, {
      writeConcern: { w: 'majority', j: true },
    });
  }

  async connect(): Promise<void> {
    try {
      await this.client.connect();
      const events = this.client
        .db(this.databaseName)
        .collection('stream_events');
      await events.createIndex(
        { streamName: 1, generation: 1, revision: 1 },
        { unique: true },
      );
      await events.createIndex({ position: 1 }, { unique: true });
      // Only remove the index created by older adapter versions. Other indexes
      // may belong to the operator and must not be dropped implicitly.
      const indexes = await events.indexes();
      const legacy = indexes.find((index) => index.name === 'eventId_1');
      if (
        legacy?.unique &&
        Object.keys(legacy.key).length === 1 &&
        legacy.key.eventId === 1
      ) {
        try {
          await events.dropIndex('eventId_1');
        } catch (error) {
          // Another upgraded instance may have dropped it concurrently.
          if (!(error instanceof MongoServerError) || error.code !== 27)
            throw error;
        }
      }
    } catch (error) {
      await this.client.close();
      throw error;
    }
  }

  getClient(): MongoClient {
    return this.client;
  }

  getDatabaseName(): string {
    return this.databaseName;
  }

  async close(): Promise<void> {
    await this.client.close();
  }
}
