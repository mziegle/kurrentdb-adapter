// The official Node client batches at 3 MiB by default; allow framing overhead.
export const GRPC_RECEIVE_HARD_MAX_BYTES = 4 * 1024 * 1024;

export function parseGrpcReceiveLimit(value: string | undefined): number {
  if (value === undefined) return GRPC_RECEIVE_HARD_MAX_BYTES;
  if (!/^[1-9][0-9]*$/.test(value) || value.length > 7) {
    throw new Error(
      'GRPC_MAX_RECEIVE_MESSAGE_BYTES must be an integer from 1 to 4194304',
    );
  }
  const bytes = Number(value);
  if (bytes > GRPC_RECEIVE_HARD_MAX_BYTES) {
    throw new Error(
      'GRPC_MAX_RECEIVE_MESSAGE_BYTES must be an integer from 1 to 4194304',
    );
  }
  return bytes;
}
