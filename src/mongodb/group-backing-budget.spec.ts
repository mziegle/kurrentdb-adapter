import { AppendReq } from '../interfaces/streams';
import { MongoEventStoreService } from './mongodb-event-store.service';

test('group append charges each shared view for its full backing', async () => {
  const previous = { ...process.env };
  process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
  process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER = 'true';
  const service = new MongoEventStoreService();
  process.env = previous;
  const enqueue = jest
    .spyOn(service['groupDispatcher']!, 'enqueue')
    .mockResolvedValue({
      kind: 'result',
      value: {} as never,
    });
  const backing = Buffer.allocUnsafeSlow(128 * 1024);
  const request = [
    {
      options: {
        streamIdentifier: { streamName: Buffer.from('stream') },
        any: {},
      },
      proposedMessage: {
        data: backing.subarray(0, 1),
        customMetadata: backing.subarray(1, 2),
      },
    },
  ] as unknown as AppendReq[];
  try {
    await service.append(request);
    await service.append(request);
    expect(enqueue).toHaveBeenCalledTimes(2);
    for (const call of enqueue.mock.calls)
      expect(call[2]).toBeGreaterThanOrEqual(2 * backing.byteLength);
  } finally {
    await service.onModuleDestroy();
  }
});

test('group append rejects a tiny view retaining an oversized backing allocation', async () => {
  const previous = { ...process.env };
  process.env.MONGODB_EXPERIMENTAL_GROUP_COMMIT = 'true';
  process.env.MONGODB_GROUP_COMMIT_ACKNOWLEDGE_SINGLE_ADAPTER = 'true';
  const service = new MongoEventStoreService();
  process.env = previous;
  const dispatcher = service['groupDispatcher']!;
  const enqueue = jest.spyOn(dispatcher, 'enqueue').mockResolvedValue({
    kind: 'result',
    value: {} as never,
  });
  const request = [
    {
      options: {
        streamIdentifier: { streamName: Buffer.from('stream') },
        any: {},
      },
      proposedMessage: {
        data: Buffer.allocUnsafeSlow(8 * 1024 * 1024).subarray(0, 1),
      },
    },
  ] as unknown as AppendReq[];
  try {
    await expect(service.append(request)).rejects.toMatchObject({
      code: 'capacity',
    });
    expect(enqueue).not.toHaveBeenCalled();
  } finally {
    await service.onModuleDestroy();
  }
});
