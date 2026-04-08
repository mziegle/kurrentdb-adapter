import { Controller } from '@nestjs/common';
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
  InvalidArgumentServiceError,
  PostgresEventStoreService,
  StreamDeletedServiceError,
} from '../postgres-event-store.service';
import { AdapterStatsService } from '../adapter-stats.service';
import {
  Metadata,
  ServerReadableStream,
  ServerWritableStream,
  sendUnaryData,
  status,
} from '@grpc/grpc-js';
import { ServerDuplexStream } from '@grpc/grpc-js';
import { logHotPath, summarizeGrpcMetadata } from '../debug-log';
import { BatchAppendSession } from './batch-append-session';

@Controller()
export class StreamsController {
  constructor(
    private readonly eventStore: PostgresEventStoreService,
    private readonly stats: AdapterStatsService,
  ) {}

  @GrpcMethod('Streams', 'read')
  read(
    request: ReadReq,
    metadata?: Metadata,
    call?: ServerWritableStream<ReadReq, ReadResp>,
  ): Observable<ReadResp> {
    logHotPath('gRPC Streams.Read', {
      detail: [
        summarizeGrpcMetadata(metadata),
        this.summarizeReadRequest(request),
      ]
        .filter(Boolean)
        .join(' '),
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
          console.info(
            `[debug] gRPC Streams.Read resolved ${responses.length} response(s) ${this.summarizeReadResponses(
              responses,
            )}`,
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
      detail: summarizeGrpcMetadata(call.metadata),
    });
    const operation = this.stats.startOperation('append');
    let completed = false;
    const messages: AppendReq[] = [];

    call.on('data', (message: AppendReq) => {
      messages.push(message);
    });

    call.on('end', () => {
      this.eventStore
        .append(messages)
        .then((response) => {
          completed = true;
          operation.succeeded();
          callback(null, response);
        })
        .catch((error: unknown) => {
          completed = true;
          operation.failed();
          callback(this.mapServiceError(error), null);
        });
    });

    call.on('error', (error) => {
      if (!completed) {
        completed = true;
        operation.failed();
      }
      callback(error, null);
    });
  }

  @GrpcMethod('Streams', 'delete')
  delete(
    request: DeleteReq,
    metadata?: Metadata,
  ): Promise<DeleteResp> | Observable<DeleteResp> | DeleteResp {
    logHotPath('gRPC Streams.Delete', {
      detail: summarizeGrpcMetadata(metadata),
    });
    const operation = this.stats.startOperation('delete');
    return this.eventStore
      .delete(request)
      .then((response) => {
        operation.succeeded();
        return response;
      })
      .catch((error: unknown) => {
        operation.failed();
        throw new RpcException(this.mapServiceError(error));
      });
  }

  @GrpcMethod('Streams', 'tombstone')
  tombstone(
    request: TombstoneReq,
    metadata?: Metadata,
  ): Promise<TombstoneResp> | Observable<TombstoneResp> | TombstoneResp {
    logHotPath('gRPC Streams.Tombstone', {
      detail: summarizeGrpcMetadata(metadata),
    });
    const operation = this.stats.startOperation('tombstone');
    return this.eventStore
      .tombstone(request)
      .then((response) => {
        operation.succeeded();
        return response;
      })
      .catch((error: unknown) => {
        operation.failed();
        throw new RpcException(this.mapServiceError(error));
      });
  }

  @GrpcStreamCall('Streams', 'batchAppend')
  batchAppend(call: ServerDuplexStream<BatchAppendReq, BatchAppendResp>): void {
    logHotPath('gRPC Streams.BatchAppend', {
      detail: summarizeGrpcMetadata(call.metadata),
    });
    const session = new BatchAppendSession(
      call,
      (requestGroup, enqueueResponse) =>
        this.handleBatchAppendGroup(call, requestGroup, enqueueResponse),
      (correlationId) => this.getBatchAppendCorrelationKey(correlationId),
      (error) => this.mapServiceError(error),
    );

    call.on('data', (message: BatchAppendReq) => {
      session.onData(message);
    });
    call.on('end', () => {
      session.onEnd();
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
  ): Promise<void> {
    const operation = this.stats.startOperation('batchAppend');
    try {
      const response = await this.eventStore.batchAppend(requestGroup);
      if (call.destroyed) {
        operation.succeeded();
        return;
      }

      await enqueueResponse(response);
      operation.succeeded();
    } catch (error: unknown) {
      operation.failed();
      if (!call.destroyed) {
        call.destroy(this.mapServiceError(error));
      }
    }
  }

  private getBatchAppendCorrelationKey(
    correlationId: BatchAppendReq['correlationId'],
  ): string {
    if (correlationId?.string) {
      return correlationId.string;
    }

    if (correlationId?.structured) {
      return `${correlationId.structured.mostSignificantBits}:${correlationId.structured.leastSignificantBits}`;
    }

    throw new Error('Batch append request must include a correlation id.');
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
