import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { StreamsContractBackend } from './contract-test-context';

type RawStreamsClient = grpc.Client & {
  read(request: object): grpc.ClientReadableStream<object>;
};

const packageDefinition = protoLoader.loadSync(
  join(__dirname, '../../proto/Grpc/streams.proto'),
  {
    includeDirs: [join(__dirname, '../../proto/Grpc')],
    longs: Number,
    enums: Number,
    defaults: true,
    oneofs: true,
  },
);

const loadedDefinition = grpc.loadPackageDefinition(
  packageDefinition,
) as unknown as {
  event_store: {
    client: {
      streams: {
        Streams: new (
          address: string,
          credentials: grpc.ChannelCredentials,
        ) => RawStreamsClient;
      };
    };
  };
};

function createRawStreamsClient(address: string): RawStreamsClient {
  return new loadedDefinition.event_store.client.streams.Streams(
    address,
    grpc.credentials.createInsecure(),
  );
}

function createReadOptionsBase() {
  return {
    readDirection: 0,
    resolveLinks: false,
    uuidOption: {
      structured: {},
    },
    controlOption: {
      compatibility: 0,
    },
  };
}

function createStreamIdentifier(streamName: string) {
  return {
    streamName: Buffer.from(streamName, 'utf8'),
  };
}

function waitForReadError(
  client: RawStreamsClient,
  request: object,
): Promise<grpc.ServiceError> {
  return new Promise<grpc.ServiceError>((resolve, reject) => {
    const call = client.read(request);

    call.on('data', (response) => {
      reject(
        new Error(
          `Expected read request to fail, but received ${JSON.stringify(response)} instead.`,
        ),
      );
    });

    call.on('error', (error: grpc.ServiceError) => {
      resolve(error);
    });

    call.on('end', () => {
      reject(
        new Error('Expected read request to fail, but the call completed.'),
      );
    });
  });
}

async function expectInvalidArgument(
  client: RawStreamsClient,
  request: object,
  details: string,
): Promise<void> {
  const error = await waitForReadError(client, request);

  expect(error.code).toBe(grpc.status.INVALID_ARGUMENT);
  expect(error.details).toBe(details);
}

export function registerReadValidationContractSuite(
  backendName: string,
  setupBackend: () => Promise<StreamsContractBackend>,
): void {
  describe(`Read validation contract: ${backendName}`, () => {
    jest.setTimeout(120_000);

    let backend: StreamsContractBackend;
    let rawClient: RawStreamsClient;

    beforeAll(async () => {
      backend = await setupBackend();
      rawClient = createRawStreamsClient(backend.getGrpcAddress());
    });

    afterAll(async () => {
      rawClient?.close();
      await backend?.dispose();
    });

    it('rejects filtered named-stream reads as an invalid combination', async () => {
      const streamName = `${backendName.toLowerCase()}-invalid-read-${randomUUID()}`;

      await expectInvalidArgument(
        rawClient,
        {
          options: {
            ...createReadOptionsBase(),
            count: 1,
            stream: {
              streamIdentifier: createStreamIdentifier(streamName),
              start: {},
            },
            filter: {
              streamIdentifier: {
                prefix: [streamName],
              },
              count: {},
              checkpointIntervalMultiplier: 1,
            },
          },
        },
        'The combination of (Stream, Count, Forwards, Filter) is invalid.',
      );
    });

    it('rejects filtered named-stream subscriptions as an invalid combination', async () => {
      const streamName = `${backendName.toLowerCase()}-invalid-subscription-filter-${randomUUID()}`;

      await expectInvalidArgument(
        rawClient,
        {
          options: {
            ...createReadOptionsBase(),
            subscription: {},
            stream: {
              streamIdentifier: createStreamIdentifier(streamName),
              start: {},
            },
            filter: {
              streamIdentifier: {
                prefix: [streamName],
              },
              count: {},
              checkpointIntervalMultiplier: 1,
            },
          },
        },
        'The combination of (Stream, Subscription, Forwards, Filter) is invalid.',
      );
    });

    it('rejects backwards subscriptions as an invalid combination', async () => {
      const streamName = `${backendName.toLowerCase()}-invalid-subscription-backwards-${randomUUID()}`;

      await expectInvalidArgument(
        rawClient,
        {
          options: {
            ...createReadOptionsBase(),
            readDirection: 1,
            subscription: {},
            noFilter: {},
            stream: {
              streamIdentifier: createStreamIdentifier(streamName),
              start: {},
            },
          },
        },
        'The combination of (Stream, Subscription, Backwards, NoFilter) is invalid.',
      );
    });

    it('rejects reads without a stream or $all target as invalid argument', async () => {
      await expectInvalidArgument(
        rawClient,
        {
          options: {
            ...createReadOptionsBase(),
            count: 1,
            noFilter: {},
          },
        },
        "'None' is not a valid EventStore.Client.Streams.ReadReq+Types+Options+StreamOptionOneofCase",
      );
    });

    it('rejects subscriptions without a stream or $all target as invalid argument', async () => {
      await expectInvalidArgument(
        rawClient,
        {
          options: {
            ...createReadOptionsBase(),
            subscription: {},
            noFilter: {},
          },
        },
        "'None' is not a valid EventStore.Client.Streams.ReadReq+Types+Options+StreamOptionOneofCase",
      );
    });
  });
}
