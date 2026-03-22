import { Test, TestingModule } from '@nestjs/testing';
import { INestMicroservice } from '@nestjs/common';
import { AppModule } from '../src/app.module';
import { KurrentDBClient, jsonEvent } from '@kurrent/kurrentdb-client';
import { Transport } from '@nestjs/microservices';
import { join } from 'node:path';

describe('Streams', () => {
  let app: INestMicroservice;
  let client: KurrentDBClient;

  beforeEach(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestMicroservice({
      transport: Transport.GRPC,
      options: {
        package: 'event_store.client.streams',
        protoPath: join(__dirname, '../src/protos/streams.proto'),
        url: 'localhost:2113',
      },
    });

    await app.listen();

    client = KurrentDBClient.connectionString`kurrentdb://admin:changeit@localhost:2113?tls=false`;
  });

  it('write event', async () => {
    // exercise
    const streamName = 'booking-abc123';

    const event = jsonEvent({
      type: 'test',
      data: {
        foo: 'bar',
      },
    });

    const result = await client.appendToStream(streamName, event);

    console.log(result);
    // verify
    expect(result).toHaveProperty('success');
    expect(result.success).toHaveProperty('currentRevision', 1);
  });
});
