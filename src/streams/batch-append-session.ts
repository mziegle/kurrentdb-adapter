import { ServerDuplexStream, status } from '@grpc/grpc-js';
import { BatchAppendReq, BatchAppendResp } from '../interfaces/streams';
import { requestSize } from '../mongodb/request-size';
import { PostgresProtocolService } from '../postgres/postgres-protocol.service';

const MAX_SESSION_BYTES = 16 * 1024 * 1024;
const MAX_SESSION_EVENTS = 10_000;
const MAX_PROCESS_BYTES = 64 * 1024 * 1024;
const MAX_TOMBSTONES = 64;
const processBackings = new Map<ArrayBufferLike, number>();
let processBytes = 0;

export class BatchAppendSession {
  private readonly tombstones = new Set<string>();
  private readonly activeKeys = new Set<string>();
  private readonly pendingMessages = new Map<string, BatchAppendReq[]>();
  private readonly groupStartedAt = new Map<string, number>();
  private readonly groupTimers = new Map<string, NodeJS.Timeout>();
  private readonly groupBytes = new Map<string, number>();
  private readonly groupBackings = new Map<string, Set<ArrayBufferLike>>();
  private readonly retainedBackings = new Map<ArrayBufferLike, number>();
  private readonly groupEvents = new Map<string, number>();
  private readonly processGroupBytes = new Map<string, number>();
  private readonly processGroupBackings = new Map<
    string,
    Set<ArrayBufferLike>
  >();
  private retainedBytes = 0;
  private retainedEvents = 0;
  private activeWrites = 0;
  private pendingRejections = 0;
  private tombstoneBytes = 0;
  private readonly rejectionEntries = new Set<{
    response: BatchAppendResp | undefined;
    bytes: number;
    writing: boolean;
  }>();
  private streamEnded = false;
  private streamClosed = false;
  private responseChain = Promise.resolve();

  constructor(
    private readonly call: ServerDuplexStream<BatchAppendReq, BatchAppendResp>,
    private readonly handleGroup: (
      requestGroup: BatchAppendReq[],
      enqueueResponse: (response: BatchAppendResp) => Promise<void>,
      startedAt: number,
      retainUntil: (completion: Promise<void>) => void,
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

    let correlationKey: string;
    try {
      correlationKey = this.getCorrelationKey(message.correlationId);
    } catch (error) {
      this.onClose();
      this.call.destroy(this.mapServiceError(error));
      return;
    }
    if (this.tombstones.has(correlationKey)) return;
    if (this.activeKeys.has(correlationKey)) {
      this.failCapacity();
      return;
    }

    const events = message.proposedMessages?.length ?? 0;
    if (events > MAX_SESSION_EVENTS - this.retainedEvents) {
      this.rejectCapacity(message);
      return;
    }
    let bytes: number;
    const found = new Set<ArrayBufferLike>();
    try {
      bytes = requestSize(
        message,
        MAX_SESSION_BYTES - this.retainedBytes,
        true,
        { retained: this.retainedBackings, found },
      );
    } catch {
      this.rejectCapacity(message);
      return;
    }
    if (!this.pendingMessages.has(correlationKey)) {
      if (this.pendingMessages.size + this.activeWrites >= 64) {
        this.rejectCapacity(message);
        return;
      }
      this.groupStartedAt.set(correlationKey, Date.now());
      const timer = setTimeout(() => this.failCapacity(), 30_000);
      timer.unref();
      this.groupTimers.set(correlationKey, timer);
    }
    const processFound = new Set<ArrayBufferLike>();
    let processCharge: number;
    try {
      processCharge = requestSize(
        message,
        MAX_PROCESS_BYTES - processBytes,
        true,
        {
          retained: processBackings,
          found: processFound,
        },
      );
    } catch {
      this.rejectCapacity(message);
      return;
    }
    const processGroup =
      this.processGroupBackings.get(correlationKey) ??
      new Set<ArrayBufferLike>();
    processBytes += processCharge;
    for (const backing of processFound) {
      if (processGroup.has(backing)) continue;
      processGroup.add(backing);
      const count = processBackings.get(backing) ?? 0;
      processBackings.set(backing, count + 1);
      if (count === 0) processCharge -= backing.byteLength;
    }
    this.processGroupBackings.set(correlationKey, processGroup);
    this.processGroupBytes.set(
      correlationKey,
      (this.processGroupBytes.get(correlationKey) ?? 0) + processCharge,
    );
    const requestGroup = this.pendingMessages.get(correlationKey) ?? [];
    const groupBackings =
      this.groupBackings.get(correlationKey) ?? new Set<ArrayBufferLike>();
    this.retainedBytes += bytes;
    for (const backing of found) {
      if (groupBackings.has(backing)) continue;
      groupBackings.add(backing);
      const count = this.retainedBackings.get(backing) ?? 0;
      this.retainedBackings.set(backing, count + 1);
      if (count === 0) bytes -= backing.byteLength;
    }
    this.groupBackings.set(correlationKey, groupBackings);
    this.retainedEvents += events;
    this.groupBytes.set(
      correlationKey,
      (this.groupBytes.get(correlationKey) ?? 0) + bytes,
    );
    this.groupEvents.set(
      correlationKey,
      (this.groupEvents.get(correlationKey) ?? 0) + events,
    );
    requestGroup.push(message);
    this.pendingMessages.set(correlationKey, requestGroup);

    if (!message.isFinal) {
      return;
    }

    this.pendingMessages.delete(correlationKey);
    clearTimeout(this.groupTimers.get(correlationKey));
    this.groupTimers.delete(correlationKey);
    const retainedGroupBytes = this.groupBytes.get(correlationKey)!;
    const retainedGroupBackings = this.groupBackings.get(correlationKey)!;
    this.groupBackings.delete(correlationKey);
    const retainedGroupEvents = this.groupEvents.get(correlationKey)!;
    this.groupBytes.delete(correlationKey);
    this.groupEvents.delete(correlationKey);
    const startedAt = this.groupStartedAt.get(correlationKey)!;
    this.groupStartedAt.delete(correlationKey);
    const retainedProcessBytes = this.processGroupBytes.get(correlationKey)!;
    const retainedProcessBackings =
      this.processGroupBackings.get(correlationKey)!;
    this.processGroupBytes.delete(correlationKey);
    this.processGroupBackings.delete(correlationKey);
    this.activeWrites += 1;
    this.activeKeys.add(correlationKey);

    let owners = 1;
    const release = () => {
      if (--owners !== 0) return;
      this.retainedBytes -= retainedGroupBytes;
      this.releaseBackings(retainedGroupBackings);
      this.releaseProcess(retainedProcessBytes, retainedProcessBackings);
      this.retainedEvents -= retainedGroupEvents;
      this.activeWrites -= 1;
      this.activeKeys.delete(correlationKey);
      this.finishIfComplete();
    };
    const retainUntil = (completion: Promise<void>) => {
      owners += 1;
      void completion.then(release, () => undefined);
    };
    let handling: Promise<void>;
    try {
      handling = this.handleGroup(
        requestGroup,
        (response) => this.enqueueResponse(response),
        startedAt,
        retainUntil,
      );
    } catch (error) {
      release();
      this.onClose();
      this.call.destroy(this.mapServiceError(error));
      return;
    }
    void handling
      .catch((error: unknown) => {
        if (!this.isClosed()) {
          this.onClose();
          this.call.destroy(this.mapServiceError(error));
        }
      })
      .finally(release);
  }

  onEnd(): void {
    this.streamEnded = true;
    this.clearPending();
    this.finishIfComplete();
  }

  onClose(): void {
    this.streamClosed = true;
    this.clearPending();
    processBytes -= this.tombstoneBytes;
    this.tombstoneBytes = 0;
    this.tombstones.clear();
    for (const entry of this.rejectionEntries) {
      if (!entry.writing) this.releaseRejection(entry);
    }
  }

  private clearPending(): void {
    for (const timer of this.groupTimers.values()) clearTimeout(timer);
    this.groupTimers.clear();
    for (const [key, bytes] of this.processGroupBytes) {
      this.releaseProcess(bytes, this.processGroupBackings.get(key)!);
    }
    this.processGroupBytes.clear();
    this.processGroupBackings.clear();
    this.pendingMessages.clear();
    this.groupStartedAt.clear();
    for (const bytes of this.groupBytes.values()) this.retainedBytes -= bytes;
    this.groupBytes.clear();
    for (const backings of this.groupBackings.values())
      this.releaseBackings(backings);
    this.groupBackings.clear();
    for (const events of this.groupEvents.values())
      this.retainedEvents -= events;
    this.groupEvents.clear();
  }

  private releaseProcess(bytes: number, backings: Set<ArrayBufferLike>): void {
    processBytes -= bytes;
    for (const backing of backings) {
      const count = processBackings.get(backing)!;
      if (count === 1) {
        processBackings.delete(backing);
        processBytes -= backing.byteLength;
      } else {
        processBackings.set(backing, count - 1);
      }
    }
  }

  private releaseBackings(backings: Set<ArrayBufferLike>): void {
    for (const backing of backings) {
      const count = this.retainedBackings.get(backing)!;
      if (count === 1) {
        this.retainedBackings.delete(backing);
        this.retainedBytes -= backing.byteLength;
      } else {
        this.retainedBackings.set(backing, count - 1);
      }
    }
  }

  onError(error: Error): void {
    this.onClose();
    void error;
  }

  private rejectCapacity(message: BatchAppendReq): void {
    if (
      this.pendingRejections >= 64 ||
      this.tombstones.size >= MAX_TOMBSTONES
    ) {
      this.failCapacity();
      return;
    }
    let key: string;
    try {
      key = this.getCorrelationKey(message.correlationId);
    } catch {
      this.failCapacity();
      return;
    }
    const options =
      this.pendingMessages.get(key)?.find((part) => part.options)?.options ??
      message.options;
    const name = options?.streamIdentifier?.streamName;
    const id = message.correlationId;
    // Bound the independently queued response; do not retain a decoded pooled
    // backing or the rejected payload while waiting for transport backpressure.
    if (
      !id ||
      key.length > 256 ||
      !(name instanceof Uint8Array) ||
      name.byteLength > 4096
    ) {
      this.failCapacity();
      return;
    }
    const response: BatchAppendResp = {
      correlationId: {
        string: id.string,
        structured: id.structured && {
          mostSignificantBits: id.structured.mostSignificantBits,
          leastSignificantBits: id.structured.leastSignificantBits,
        },
      },
      streamIdentifier: { streamName: Uint8Array.from(name) },
      streamPosition: options?.streamPosition,
      noStream: options?.noStream && {},
      any: options?.any && {},
      streamExists: options?.streamExists && {},
      error: new PostgresProtocolService().createBatchAppendUnknownStatus(
        'Append was not dispatched.',
      ),
    };
    clearTimeout(this.groupTimers.get(key));
    this.groupTimers.delete(key);
    this.groupStartedAt.delete(key);
    this.pendingMessages.delete(key);
    const bytes = this.groupBytes.get(key);
    if (bytes !== undefined) this.retainedBytes -= bytes;
    this.groupBytes.delete(key);
    const backings = this.groupBackings.get(key);
    if (backings) this.releaseBackings(backings);
    this.groupBackings.delete(key);
    this.retainedEvents -= this.groupEvents.get(key) ?? 0;
    this.groupEvents.delete(key);
    const groupProcessBytes = this.processGroupBytes.get(key);
    const groupProcessBackings = this.processGroupBackings.get(key);
    if (groupProcessBytes !== undefined && groupProcessBackings)
      this.releaseProcess(groupProcessBytes, groupProcessBackings);
    this.processGroupBytes.delete(key);
    this.processGroupBackings.delete(key);
    let responseBytes: number;
    let keyBytes: number;
    try {
      keyBytes = requestSize({ key }, MAX_PROCESS_BYTES - processBytes);
      responseBytes = requestSize(
        response,
        MAX_PROCESS_BYTES - processBytes - keyBytes,
        true,
      );
    } catch {
      this.failCapacity();
      return;
    }
    processBytes += keyBytes + responseBytes;
    this.tombstoneBytes += keyBytes;
    this.tombstones.add(key);
    this.pendingRejections++;
    const entry = {
      response: response as BatchAppendResp | undefined,
      bytes: responseBytes,
      writing: false,
    };
    this.rejectionEntries.add(entry);
    this.enqueueRejection(entry);
  }

  private releaseRejection(entry: {
    response: BatchAppendResp | undefined;
    bytes: number;
    writing: boolean;
  }): void {
    if (!this.rejectionEntries.delete(entry)) return;
    entry.response = undefined;
    processBytes -= entry.bytes;
    this.pendingRejections--;
    this.finishIfComplete();
  }

  private enqueueRejection(entry: {
    response: BatchAppendResp | undefined;
    bytes: number;
    writing: boolean;
  }): void {
    this.responseChain = this.responseChain
      .then(async () => {
        if (!entry.response || this.isClosed()) return;
        entry.writing = true;
        await this.writeResponse(entry.response);
      })
      .catch((error: unknown) => {
        if (!this.isClosed()) {
          this.onClose();
          this.call.destroy(this.mapServiceError(error));
        }
      })
      .finally(() => this.releaseRejection(entry));
  }

  private failCapacity(): void {
    this.onClose();
    // grpc-js translates an emitted error into non-OK trailers. destroy(error)
    // tears down the duplex before those trailers reach the official client.
    this.call.emit(
      'error',
      Object.assign(
        new Error(
          'Batch append session capacity exceeded; in-flight outcomes may be unknown.',
        ),
        { code: status.RESOURCE_EXHAUSTED },
      ),
    );
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
      this.pendingRejections > 0 ||
      this.call.destroyed ||
      this.streamClosed
    ) {
      return;
    }

    this.onClose();
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
