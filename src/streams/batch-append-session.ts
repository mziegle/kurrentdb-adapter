import { ServerDuplexStream } from '@grpc/grpc-js';
import { BatchAppendReq, BatchAppendResp } from '../interfaces/streams';

export class BatchAppendSession {
  private readonly pendingMessages = new Map<string, BatchAppendReq[]>();
  private activeWrites = 0;
  private streamEnded = false;
  private streamClosed = false;
  private responseChain = Promise.resolve();

  constructor(
    private readonly call: ServerDuplexStream<BatchAppendReq, BatchAppendResp>,
    private readonly handleGroup: (
      requestGroup: BatchAppendReq[],
      enqueueResponse: (response: BatchAppendResp) => Promise<void>,
    ) => Promise<void>,
    private readonly getCorrelationKey: (
      correlationId: BatchAppendReq['correlationId'],
    ) => string,
    private readonly mapServiceError: (error: unknown) => Error,
  ) {}

  onData(message: BatchAppendReq): void {
    if (this.isClosed()) {
      return;
    }

    const correlationKey = this.getCorrelationKey(message.correlationId);
    const requestGroup = this.pendingMessages.get(correlationKey) ?? [];
    requestGroup.push(message);
    this.pendingMessages.set(correlationKey, requestGroup);

    if (!message.isFinal) {
      return;
    }

    this.pendingMessages.delete(correlationKey);
    this.activeWrites += 1;

    void this.handleGroup(requestGroup, (response) =>
      this.enqueueResponse(response),
    ).finally(() => {
      this.activeWrites -= 1;
      this.finishIfComplete();
    });
  }

  onEnd(): void {
    this.streamEnded = true;
    this.finishIfComplete();
  }

  onClose(): void {
    this.streamClosed = true;
    this.pendingMessages.clear();
  }

  onError(error: Error): void {
    if (!this.call.destroyed && !this.streamEnded) {
      this.call.destroy(error);
    }
  }

  private enqueueResponse(response: BatchAppendResp): Promise<void> {
    this.responseChain = this.responseChain
      .then(async () => {
        if (this.isClosed()) {
          return;
        }

        await this.writeResponse(response);
      })
      .catch((error: unknown) => {
        if (!this.isClosed()) {
          this.call.destroy(this.mapServiceError(error));
        }
      });

    return this.responseChain;
  }

  private finishIfComplete(): void {
    if (
      !this.streamEnded ||
      this.activeWrites > 0 ||
      this.call.destroyed ||
      this.streamClosed
    ) {
      return;
    }

    this.streamClosed = true;
    this.pendingMessages.clear();
    this.call.end();
  }

  private isClosed(): boolean {
    return this.streamClosed || this.call.destroyed;
  }

  private writeResponse(response: BatchAppendResp): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.call.write(response, (error) => {
        if (error) {
          reject(error instanceof Error ? error : new Error(String(error)));
          return;
        }

        resolve();
      });
    });
  }
}
