import { Controller, Inject } from '@nestjs/common';
import {
  GrpcMethod,
  GrpcStreamCall,
  RpcException,
} from '@nestjs/microservices';
import {
  AppendReq,
  AppendResp,
  BatchAppendReq,
  BatchAppendResp,
  DeleteReq,
  DeleteResp,
  ReadReq,
  ReadResp,
  TombstoneReq,
  TombstoneResp,
} from '../interfaces/streams';
import { Observable } from 'rxjs';
import {
  EVENT_STORE_BACKEND,
  EventStoreBackend,
} from '../event-store/event-store-backend';
import {
  InvalidArgumentServiceError,
  StreamDeletedServiceError,
} from '../event-store/event-store.errors';
import { AdapterStatsService } from '../operations/adapter-stats.service';
import {
  Metadata,
  ServerReadableStream,
  ServerUnaryCall,
  ServerWritableStream,
  sendUnaryData,
  status,
} from '@grpc/grpc-js';
import { ServerDuplexStream } from '@grpc/grpc-js';
import {
  extractGrpcMetadata,
  logHotPath,
  logStreamRequestMessage,
  summarizeGrpcMetadata,
} from '../shared/debug-log';
import { appLogger } from '../shared/logger';
import { BatchAppendSession } from './batch-append-session';
import { createWriteContext } from './write-context';
import { GroupDispatchError } from '../mongodb/group-dispatcher';
import { PostgresProtocolService } from '../postgres/postgres-protocol.service';

@Controller()
export class StreamsController {
  constructor(
    @Inject(EVENT_STORE_BACKEND)
    private readonly eventStore: EventStoreBackend,
    private readonly stats: AdapterStatsService,
  ) {}

  @GrpcMethod('Streams', 'read')
  read(
    request: ReadReq,
    metadata?: Metadata,
    call?: ServerWritableStream<ReadReq, ReadResp>,
  ): Observable<ReadResp> {
    logHotPath('gRPC Streams.Read', {
      summary: [
        summarizeGrpcMetadata(metadata),
        this.summarizeReadRequest(request),
      ]
        .filter(Boolean)
        .join(' '),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    const operation = this.stats.startOperation('read');
    return new Observable<ReadResp>((subscriber) => {
      let cancelled = false;
      let completed = false;
      const cancelHandler = () => {
        cancelled = true;
      };

      call?.on('cancelled', cancelHandler);

      void (async () => {
        try {
          if (request.options?.subscription) {
            for await (const response of this.eventStore.subscribeToStream(
              request,
              () => cancelled,
            )) {
              if (cancelled) {
                return;
              }

              subscriber.next(response);
            }

            if (!cancelled) {
              operation.succeeded();
              completed = true;
              subscriber.complete();
            }

            return;
          }

          const responses = await this.eventStore.read(request);
          appLogger.debug(
            {
              event: 'grpc_response',
              rpc: 'Streams.Read',
              responseCount: responses.length,
              summary: this.summarizeReadResponses(responses),
            },
            'gRPC Streams.Read resolved',
          );
          for (const response of responses) {
            subscriber.next(response);
          }

          operation.succeeded();
          completed = true;
          subscriber.complete();
        } catch (error: unknown) {
          operation.failed();
          if (!cancelled) {
            subscriber.error(new RpcException(this.mapServiceError(error)));
          }
        }
      })();

      return () => {
        if (cancelled && !completed) {
          operation.failed();
        }
        cancelled = true;
        call?.off('cancelled', cancelHandler);
      };
    });
  }

  private summarizeReadRequest(request: ReadReq): string {
    const options = request.options;
    if (!options) {
      return 'options=missing';
    }

    if (options.all) {
      const boundary =
        options.all.position !== undefined
          ? `position=${options.all.position.commitPosition}/${options.all.position.preparePosition}`
          : options.all.start !== undefined
            ? 'from=start'
            : options.all.end !== undefined
              ? 'from=end'
              : 'from=unset';
      const mode = options.subscription ? 'subscription' : 'count';
      const count =
        options.count !== undefined ? ` count=${String(options.count)}` : '';
      return `$all ${mode} ${boundary}${count}`;
    }

    const streamName = request.options?.stream?.streamIdentifier?.streamName;
    if (streamName) {
      return `stream=${Buffer.from(streamName).toString('utf8')}`;
    }

    return 'streamOption=none';
  }

  private summarizeReadResponses(responses: ReadResp[]): string {
    if (responses.length === 0) {
      return 'summary=empty';
    }

    const parts = responses.map((response) => {
      if (response.event) {
        const recordedEvent = response.event.event;
        const streamName = recordedEvent?.streamIdentifier?.streamName
          ? Buffer.from(recordedEvent.streamIdentifier.streamName).toString(
              'utf8',
            )
          : 'unknown';
        const streamRevision =
          recordedEvent?.streamRevision !== undefined
            ? String(recordedEvent.streamRevision)
            : 'unknown';
        return `event:${streamName}@${streamRevision}`;
      }

      if (response.caughtUp) {
        if (response.caughtUp.position) {
          return `caughtUp:${response.caughtUp.position.commitPosition}`;
        }

        if (response.caughtUp.streamRevision !== undefined) {
          return `caughtUp:${String(response.caughtUp.streamRevision)}`;
        }

        return 'caughtUp:empty';
      }

      if (response.streamNotFound) {
        const streamName = response.streamNotFound.streamIdentifier?.streamName
          ? Buffer.from(
              response.streamNotFound.streamIdentifier.streamName,
            ).toString('utf8')
          : 'unknown';
        return `streamNotFound:${streamName}`;
      }

      if (response.firstStreamPosition !== undefined) {
        return `firstStreamPosition:${String(response.firstStreamPosition)}`;
      }

      if (response.lastStreamPosition !== undefined) {
        return `lastStreamPosition:${String(response.lastStreamPosition)}`;
      }

      return 'other';
    });

    return `summary=${parts.join(',')}`;
  }

  @GrpcStreamCall('Streams', 'append')
  append(
    call: ServerReadableStream<AppendReq, AppendResp>,
    callback: sendUnaryData<AppendResp>,
  ): void {
    logHotPath('gRPC Streams.Append', {
      summary: summarizeGrpcMetadata(call.metadata),
      trace: {
        metadata: extractGrpcMetadata(call.metadata),
      },
    });
    const operation = this.stats.startOperation('append');
    const writeContext = createWriteContext(call, undefined, false);
    let completed = false;
    const messages: AppendReq[] = [];

    call.on('data', (message: AppendReq) => {
      logStreamRequestMessage('gRPC Streams.Append', message, {
        rpc: 'Streams.Append',
      });
      messages.push(message);
    });

    call.on('end', () => {
      this.eventStore
        .append(messages, writeContext.context)
        .then((response) => {
          completed = true;
          operation.succeeded();
          callback(null, response);
        })
        .catch((error: unknown) => {
          completed = true;
          operation.failed();
          callback(this.mapServiceError(error), null);
        })
        .finally(writeContext.dispose);
    });

    call.on('error', (error) => {
      if (!completed) {
        completed = true;
        operation.failed();
      }
      callback(error, null);
      writeContext.dispose();
    });
  }

  @GrpcMethod('Streams', 'delete')
  delete(
    request: DeleteReq,
    metadata?: Metadata,
    call?: ServerUnaryCall<DeleteReq, DeleteResp>,
  ): Promise<DeleteResp> | Observable<DeleteResp> | DeleteResp {
    logHotPath('gRPC Streams.Delete', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    const operation = this.stats.startOperation('delete');
    const writeContext = call ? createWriteContext(call) : undefined;
    return (async () => {
      try {
        const response = await this.eventStore.delete(
          request,
          writeContext?.context,
        );
        operation.succeeded();
        return response;
      } catch (error: unknown) {
        operation.failed();
        throw new RpcException(this.mapServiceError(error));
      } finally {
        writeContext?.dispose();
      }
    })();
  }

  @GrpcMethod('Streams', 'tombstone')
  tombstone(
    request: TombstoneReq,
    metadata?: Metadata,
    call?: ServerUnaryCall<TombstoneReq, TombstoneResp>,
  ): Promise<TombstoneResp> | Observable<TombstoneResp> | TombstoneResp {
    logHotPath('gRPC Streams.Tombstone', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    const operation = this.stats.startOperation('tombstone');
    const writeContext = call ? createWriteContext(call) : undefined;
    return (async () => {
      try {
        const response = await this.eventStore.tombstone(
          request,
          writeContext?.context,
        );
        operation.succeeded();
        return response;
      } catch (error: unknown) {
        operation.failed();
        throw new RpcException(this.mapServiceError(error));
      } finally {
        writeContext?.dispose();
      }
    })();
  }

  @GrpcStreamCall('Streams', 'batchAppend')
  batchAppend(call: ServerDuplexStream<BatchAppendReq, BatchAppendResp>): void {
    logHotPath('gRPC Streams.BatchAppend', {
      summary: summarizeGrpcMetadata(call.metadata),
      trace: {
        metadata: extractGrpcMetadata(call.metadata),
      },
    });
    const session = new BatchAppendSession(
      call,
      (requestGroup, enqueueResponse, startedAt, retainUntil) =>
        this.handleBatchAppendGroup(
          call,
          requestGroup,
          enqueueResponse,
          startedAt,
          retainUntil,
        ),
      (correlationId) => this.getBatchAppendCorrelationKey(correlationId),
      (error) => this.mapServiceError(error),
    );

    call.on('data', (message: BatchAppendReq) => {
      logStreamRequestMessage('gRPC Streams.BatchAppend', message, {
        rpc: 'Streams.BatchAppend',
      });
      session.onData(message);
    });
    call.on('end', () => {
      session.onEnd();
    });
    call.on('cancelled', () => {
      session.onClose();
    });
    call.on('close', () => {
      session.onClose();
    });
    call.on('error', (error) => {
      session.onError(error);
    });
  }

  private async handleBatchAppendGroup(
    call: ServerDuplexStream<BatchAppendReq, BatchAppendResp>,
    requestGroup: BatchAppendReq[],
    enqueueResponse: (response: BatchAppendResp) => Promise<void>,
    startedAt: number,
    retainUntil: (completion: Promise<void>) => void,
  ): Promise<void> {
    const operation = this.stats.startOperation('batchAppend');
    const options = requestGroup.find((request) => request.options)?.options;
    // protobufjs preserves underscores before digits in this legacy field.
    const absoluteDeadline =
      options?.deadline21100 ??
      (
        options as typeof options & {
          deadline_21_10_0?: NonNullable<typeof options>['deadline21100'];
        }
      )?.deadline_21_10_0;
    const time = absoluteDeadline ?? options?.deadline;
    const groupDeadline = time
      ? (absoluteDeadline ? 0 : startedAt) +
        Number(time.seconds) * 1000 +
        // Floor before adding the epoch: floating-point rounding at epoch scale
        // could otherwise extend a sub-millisecond deadline.
        Math.floor(Number(time.nanos ?? 0) / 1_000_000)
      : undefined;
    const writeContext = createWriteContext(call, groupDeadline);
    writeContext.context.retainUntil = retainUntil;
    try {
      let response: BatchAppendResp;
      try {
        response = await this.eventStore.batchAppend(
          requestGroup,
          writeContext.context,
        );
      } finally {
        writeContext.dispose();
      }
      if (call.destroyed) {
        operation.succeeded();
        return;
      }

      await enqueueResponse(response);
      operation.succeeded();
    } catch (error: unknown) {
      operation.failed();
      const correlationId = requestGroup[0]?.correlationId;
      const streamIdentifier = options?.streamIdentifier;
      if (
        error instanceof GroupDispatchError &&
        correlationId &&
        streamIdentifier?.streamName
      ) {
        // The client recognizes Timeout and Unknown, but has no correlated
        // capacity/cancellation error. Never label a sealed outcome as timeout:
        // it can still commit after its caller's deadline.
        await enqueueResponse({
          correlationId,
          streamIdentifier,
          streamPosition: options?.streamPosition,
          noStream: options?.noStream,
          any: options?.any,
          streamExists: options?.streamExists,
          error:
            error.code === 'cancelled' &&
            writeContext.context.deadline !== undefined &&
            Date.now() >= writeContext.context.deadline
              ? new PostgresProtocolService().createBatchAppendTimeoutStatus()
              : new PostgresProtocolService().createBatchAppendUnknownStatus(
                  error.code === 'capacity' || error.code === 'closed'
                    ? 'Append was not dispatched.'
                    : undefined,
                ),
        });
        return;
      }
      if (
        error instanceof StreamDeletedServiceError &&
        correlationId &&
        streamIdentifier?.streamName &&
        Buffer.from(streamIdentifier.streamName).toString('utf8') ===
          error.streamName
      ) {
        await enqueueResponse({
          correlationId,
          streamIdentifier,
          streamPosition: options?.streamPosition,
          noStream: options?.noStream,
          any: options?.any,
          streamExists: options?.streamExists,
          error:
            new PostgresProtocolService().createBatchAppendStreamDeletedStatus(
              streamIdentifier.streamName,
            ),
        });
        return;
      }
      if (!call.destroyed) {
        call.destroy(this.mapServiceError(error));
      }
    }
  }

  private getBatchAppendCorrelationKey(
    correlationId: BatchAppendReq['correlationId'],
  ): string {
    const invalid = () => new Error('Invalid batch append correlation id.');
    if (
      !correlationId ||
      (correlationId.string !== undefined) ===
        (correlationId.structured !== undefined)
    )
      throw invalid();
    if (correlationId.string !== undefined) {
      if (
        typeof correlationId.string !== 'string' ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
          correlationId.string,
        )
      )
        throw invalid();
      return correlationId.string.replace(/-/g, '').toLowerCase();
    }
    const structured = correlationId.structured;
    if (!structured || typeof structured !== 'object') throw invalid();
    const bits = (value: unknown): string => {
      let n: bigint;
      if (value === undefined)
        n = 0n; // Omitted proto3 scalar defaults to zero.
      else if (typeof value === 'number' && Number.isSafeInteger(value))
        n = BigInt(value);
      else if (typeof value === 'bigint') n = value;
      else if (
        typeof value === 'string' &&
        /^-?\d+$/.test(value) &&
        value.length <= 20
      )
        n = BigInt(value);
      else if (
        value &&
        typeof value === 'object' &&
        'low' in value &&
        'high' in value &&
        'unsigned' in value
      ) {
        const { low, high, unsigned } = value;
        if (
          typeof low !== 'number' ||
          typeof high !== 'number' ||
          !Number.isInteger(low) ||
          !Number.isInteger(high) ||
          low < -2147483648 ||
          low > 2147483647 ||
          high < -2147483648 ||
          high > 2147483647 ||
          typeof unsigned !== 'boolean'
        )
          throw invalid();
        n = (BigInt(high >>> 0) << 32n) | BigInt(low >>> 0);
      } else throw invalid();
      if (n < -(1n << 63n) || n >= 1n << 64n) throw invalid();
      return BigInt.asUintN(64, n).toString(16).padStart(16, '0');
    };
    return (
      bits(structured.mostSignificantBits) +
      bits(structured.leastSignificantBits)
    );
  }

  private mapServiceError(error: unknown): Error {
    if (error instanceof StreamDeletedServiceError) {
      const metadata = new Metadata();
      metadata.set('exception', 'stream-deleted');
      metadata.set('stream-name', error.streamName);

      return Object.assign(
        new Error(`Stream "${error.streamName}" is deleted.`),
        {
          name: 'StreamDeletedError',
          code: status.UNKNOWN,
          details: 'Stream deleted.',
          metadata,
        },
      );
    }

    if (error instanceof InvalidArgumentServiceError) {
      return Object.assign(new Error(error.message), {
        name: 'InvalidArgumentError',
        code: status.INVALID_ARGUMENT,
        details: error.message,
      });
    }

    return error as Error;
  }
}
