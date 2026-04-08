export class StreamDeletedServiceError extends Error {
  constructor(readonly streamName: string) {
    super(`Stream "${streamName}" is deleted.`);
  }
}

export class InvalidArgumentServiceError extends Error {}
