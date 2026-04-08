import { EventEmitter } from 'node:events';
import { BatchAppendReq, BatchAppendResp } from './interfaces/streams';
import { BatchAppendSession } from './batch-append-session';

class FakeBatchAppendCall extends EventEmitter {
  destroyed = false;
  ended = false;
  readonly writes: BatchAppendResp[] = [];
  destroyError: Error | undefined;
  writeError: Error | undefined;

  write(
    response: BatchAppendResp,
    callback: (error?: Error | null) => void,
  ): void {
    if (this.writeError) {
      callback(this.writeError);
      return;
    }

    this.writes.push(response);
    callback(null);
  }

  end(): void {
    this.ended = true;
  }

  destroy(error?: Error): void {
    this.destroyed = true;
    this.destroyError = error;
    this.emit('close');
  }
}

function createBatchAppendRequest(
  correlationId: string,
  eventType: string,
  isFinal: boolean,
): BatchAppendReq {
  return {
    correlationId: { string: correlationId },
    options: {
      streamIdentifier: {
        streamName: Buffer.from(`stream-${correlationId}`, 'utf8'),
      },
      noStream: {},
    },
    proposedMessages: [
      {
        id: { string: `${correlationId}-${eventType}` },
        metadata: {
          type: eventType,
          'content-type': 'application/json',
        },
        customMetadata: new Uint8Array(),
        data: Buffer.from(`{"type":"${eventType}"}`, 'utf8'),
      },
    ],
    isFinal,
  };
}

function createBatchAppendResponse(correlationId: string): BatchAppendResp {
  return {
    correlationId: { string: correlationId },
    streamIdentifier: {
      streamName: Buffer.from(`stream-${correlationId}`, 'utf8'),
    },
    noStream: {},
    success: {
      currentRevision: 0,
      position: {
        commitPosition: 1,
        preparePosition: 1,
      },
    },
  };
}

function createDeferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });

  return { promise, resolve };
}

async function flushAsyncWork(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

describe('BatchAppendSession', () => {
  it('keeps interleaved correlation groups isolated before dispatching them', async () => {
    const firstGroup = createDeferred<BatchAppendResp>();
    const secondGroup = createDeferred<BatchAppendResp>();
    const handleGroup = jest.fn<
      Promise<void>,
      [BatchAppendReq[], (response: BatchAppendResp) => Promise<void>]
    >((requestGroup, enqueueResponse) => {
      const correlationId = requestGroup[0]?.correlationId?.string;

      if (correlationId === 'alpha') {
        return firstGroup.promise.then((response) => enqueueResponse(response));
      }

      if (correlationId === 'beta') {
        return secondGroup.promise.then((response) =>
          enqueueResponse(response),
        );
      }

      throw new Error(
        `Unexpected correlation id ${correlationId ?? 'missing'}`,
      );
    });
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      handleGroup,
      (correlationId) => {
        if (!correlationId?.string) {
          throw new Error('Missing correlation id');
        }

        return correlationId.string;
      },
      (error) => error as Error,
    );

    session.onData(createBatchAppendRequest('alpha', 'alpha-1', false));
    session.onData(createBatchAppendRequest('beta', 'beta-1', false));
    session.onData(createBatchAppendRequest('beta', 'beta-2', true));
    session.onData(createBatchAppendRequest('alpha', 'alpha-2', true));
    session.onEnd();

    await flushAsyncWork();

    expect(handleGroup).toHaveBeenCalledTimes(2);
    expect(handleGroup.mock.calls[0]?.[0]).toMatchObject([
      {
        correlationId: { string: 'beta' },
        isFinal: false,
        proposedMessages: [{ metadata: { type: 'beta-1' } }],
      },
      {
        correlationId: { string: 'beta' },
        isFinal: true,
        proposedMessages: [{ metadata: { type: 'beta-2' } }],
      },
    ]);
    expect(handleGroup.mock.calls[1]?.[0]).toMatchObject([
      {
        correlationId: { string: 'alpha' },
        isFinal: false,
        proposedMessages: [{ metadata: { type: 'alpha-1' } }],
      },
      {
        correlationId: { string: 'alpha' },
        isFinal: true,
        proposedMessages: [{ metadata: { type: 'alpha-2' } }],
      },
    ]);
    expect(call.ended).toBe(false);

    secondGroup.resolve(createBatchAppendResponse('beta'));
    await flushAsyncWork();
    expect(call.writes).toEqual([createBatchAppendResponse('beta')]);
    expect(call.ended).toBe(false);

    firstGroup.resolve(createBatchAppendResponse('alpha'));
    await flushAsyncWork();
    expect(call.writes).toEqual([
      createBatchAppendResponse('beta'),
      createBatchAppendResponse('alpha'),
    ]);
    expect(call.ended).toBe(true);
  });

  it('does not write or end the stream after the duplex call closes', async () => {
    const deferredResponse = createDeferred<BatchAppendResp>();
    const handleGroup = jest.fn<
      Promise<void>,
      [BatchAppendReq[], (response: BatchAppendResp) => Promise<void>]
    >((_, enqueueResponse) =>
      deferredResponse.promise.then((response) => enqueueResponse(response)),
    );
    const call = new FakeBatchAppendCall();
    const session = new BatchAppendSession(
      call as never,
      handleGroup,
      (correlationId) => {
        if (!correlationId?.string) {
          throw new Error('Missing correlation id');
        }

        return correlationId.string;
      },
      (error) => error as Error,
    );

    session.onData(createBatchAppendRequest('alpha', 'alpha-1', true));
    session.onEnd();

    await flushAsyncWork();
    session.onClose();

    deferredResponse.resolve(createBatchAppendResponse('alpha'));
    await flushAsyncWork();

    expect(handleGroup).toHaveBeenCalledTimes(1);
    expect(call.writes).toEqual([]);
    expect(call.ended).toBe(false);
    expect(call.destroyed).toBe(false);
  });
});
