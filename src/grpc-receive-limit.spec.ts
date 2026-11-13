import { parseGrpcReceiveLimit } from './grpc-receive-limit';

describe('gRPC inbound message size configuration', () => {
  it('defaults to 4 MiB and permits positive decimal limits up to the hard maximum', () => {
    expect(parseGrpcReceiveLimit(undefined)).toBe(4 * 1024 * 1024);
    expect(parseGrpcReceiveLimit('1')).toBe(1);
    expect(parseGrpcReceiveLimit('4194304')).toBe(4194304);
  });

  it.each(['', '0', '-1', '4194305', '1.5', '1e3', ' 42', '9'.repeat(10000)])(
    'rejects unsafe configuration without echoing it',
    (value) => {
      try {
        parseGrpcReceiveLimit(value);
        throw new Error('expected rejection');
      } catch (error) {
        expect((error as Error).message).toBe(
          'GRPC_MAX_RECEIVE_MESSAGE_BYTES must be an integer from 1 to 4194304',
        );
      }
    },
  );
});
