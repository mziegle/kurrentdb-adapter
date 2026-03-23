import { Controller } from '@nestjs/common';
import {
  GrpcMethod,
  GrpcStreamCall,
  RpcException,
} from '@nestjs/microservices';
import {
  AppendReq,
  AppendResp,
  BatchAppendResp,
  DeleteReq,
  DeleteResp,
  ReadReq,
  ReadResp,
  TombstoneReq,
  TombstoneResp,
} from './interfaces/streams';
import { Observable } from 'rxjs';
import {
  PostgresEventStoreService,
  StreamDeletedServiceError,
} from './postgres-event-store.service';
import {
  Metadata,
  ServerReadableStream,
  ServerWritableStream,
  sendUnaryData,
  status,
} from '@grpc/grpc-js';

@Controller()
export class StreamsController {
  constructor(private readonly eventStore: PostgresEventStoreService) {}

  @GrpcMethod('Streams', 'read')
  read(
    request: ReadReq,
    _metadata?: Metadata,
    call?: ServerWritableStream<ReadReq, ReadResp>,
  ): Observable<ReadResp> {
    return new Observable<ReadResp>((subscriber) => {
      let cancelled = false;
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
              subscriber.complete();
            }

            return;
          }

          const responses = await this.eventStore.read(request);
          for (const response of responses) {
            subscriber.next(response);
          }

          subscriber.complete();
        } catch (error: unknown) {
          if (!cancelled) {
            subscriber.error(new RpcException(this.mapServiceError(error)));
          }
        }
      })();

      return () => {
        cancelled = true;
        call?.off('cancelled', cancelHandler);
      };
    });
  }

  @GrpcStreamCall('Streams', 'append')
  append(
    call: ServerReadableStream<AppendReq, AppendResp>,
    callback: sendUnaryData<AppendResp>,
  ): void {
    const messages: AppendReq[] = [];

    call.on('data', (message: AppendReq) => {
      messages.push(message);
    });

    call.on('end', () => {
      this.eventStore
        .append(messages)
        .then((response) => callback(null, response))
        .catch((error: unknown) => callback(this.mapServiceError(error), null));
    });

    call.on('error', (error) => {
      callback(error, null);
    });
  }

  @GrpcMethod('Streams', 'delete')
  delete(
    request: DeleteReq,
  ): Promise<DeleteResp> | Observable<DeleteResp> | DeleteResp {
    return this.eventStore.delete(request).catch((error: unknown) => {
      throw new RpcException(this.mapServiceError(error));
    });
  }

  @GrpcMethod('Streams', 'tombstone')
  tombstone(
    request: TombstoneReq,
  ): Promise<TombstoneResp> | Observable<TombstoneResp> | TombstoneResp {
    return this.eventStore.tombstone(request).catch((error: unknown) => {
      throw new RpcException(this.mapServiceError(error));
    });
  }

  @GrpcStreamCall('Streams', 'batchAppend')
  batchAppend(): Observable<BatchAppendResp> {
    throw new Error('Method not implemented.');
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

    return error as Error;
  }
}
