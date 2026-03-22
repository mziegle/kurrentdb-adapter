import { Controller } from '@nestjs/common';
import {
  GrpcMethod,
  GrpcStreamCall,
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
} from './interfaces/streams';
import { Observable } from 'rxjs';
import { PostgresEventStoreService } from './postgres-event-store.service';
import { ServerReadableStream, sendUnaryData } from '@grpc/grpc-js';

@Controller()
export class StreamsController {
  constructor(private readonly eventStore: PostgresEventStoreService) {}

  @GrpcMethod('Streams', 'read')
  read(request: ReadReq): Observable<ReadResp> {
    return new Observable<ReadResp>((subscriber) => {
      this.eventStore
        .read(request)
        .then((responses) => {
          for (const response of responses) {
            subscriber.next(response);
          }
          subscriber.complete();
        })
        .catch((error: unknown) => subscriber.error(error));
    });
  }

  @GrpcStreamCall('Streams', 'append')
  append(
    call: ServerReadableStream<AppendReq, AppendResp>,
    callback: sendUnaryData<AppendResp>,
  ): void {
    const messages: AppendReq[] = [];

    call.on('data', (message) => {
      messages.push(message);
    });

    call.on('end', () => {
      this.eventStore
        .append(messages)
        .then((response) => callback(null, response))
        .catch((error: unknown) => callback(error as Error, null));
    });

    call.on('error', (error) => {
      callback(error, null);
    });
  }

  @GrpcMethod('Streams', 'delete')
  delete(
    request: DeleteReq,
  ): Promise<DeleteResp> | Observable<DeleteResp> | DeleteResp {
    throw new Error('Method not implemented.');
  }

  @GrpcMethod('Streams', 'tombstone')
  tombstone(
    request: TombstoneReq,
  ): Promise<TombstoneResp> | Observable<TombstoneResp> | TombstoneResp {
    throw new Error('Method not implemented.');
  }

  @GrpcStreamCall('Streams', 'batchAppend')
  batchAppend(
    request: ServerReadableStream<BatchAppendReq, BatchAppendResp>,
  ): Observable<BatchAppendResp> {
    throw new Error('Method not implemented.');
  }
}
