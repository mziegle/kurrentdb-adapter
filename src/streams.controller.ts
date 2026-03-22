import { Controller } from '@nestjs/common';
import {
  AppendReq,
  AppendResp,
  BatchAppendReq,
  BatchAppendResp,
  DeleteReq,
  DeleteResp,
  ReadReq,
  ReadResp,
  StreamsControllerMethods,
  TombstoneReq,
  TombstoneResp,
} from './interfaces/streams';
import { StreamsController as IStreamController } from './interfaces/streams';
import { Observable } from 'rxjs';
import { randomUUID } from 'node:crypto';

@Controller()
@StreamsControllerMethods()
export class StreamsController implements IStreamController {
  read(request: ReadReq): Observable<ReadResp> {
    console.log(request);

    const o = new Observable<ReadResp>((subscriber) => {
      const response: ReadResp = {
        event: {
          event: {
            id: { string: randomUUID() },
            data: new Uint8Array(),
            commitPosition: 0,
            streamIdentifier: { streamName: Buffer.from('sample-stream') },
            streamRevision: 1,
            customMetadata: new Uint8Array(),
            metadata: {},
            preparePosition: 0,
          },
          link: {
            id: { string: randomUUID() },
            data: new Uint8Array(),
            commitPosition: 0,
            streamIdentifier: { streamName: Buffer.from('sample-stream') },
            streamRevision: 1,
            customMetadata: new Uint8Array(),
            metadata: {},
            preparePosition: 0,
          },
        },
      };
      subscriber.next(response);
      subscriber.complete();
    });

    return o;
  }

  append(
    request: Observable<AppendReq>,
  ): Promise<AppendResp> | Observable<AppendResp> | AppendResp {
    console.log(request);

    const o = new Observable<AppendResp>((subscriber) => {
      const response: AppendResp = {
        success: {
          currentRevision: 1,
        },
      };
      subscriber.next(response);
      subscriber.complete();
    });

    return o;
  }

  delete(
    request: DeleteReq,
  ): Promise<DeleteResp> | Observable<DeleteResp> | DeleteResp {
    throw new Error('Method not implemented.');
  }

  tombstone(
    request: TombstoneReq,
  ): Promise<TombstoneResp> | Observable<TombstoneResp> | TombstoneResp {
    throw new Error('Method not implemented.');
  }

  batchAppend(
    request: Observable<BatchAppendReq>,
  ): Observable<BatchAppendResp> {
    throw new Error('Method not implemented.');
  }
}
