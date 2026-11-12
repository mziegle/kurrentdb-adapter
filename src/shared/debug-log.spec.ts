import { Metadata } from '@grpc/grpc-js';
import { extractGrpcMetadata, logHotPath } from './debug-log';
import { appLogger } from './logger';

describe('request logging', () => {
  it('does not log binary event data previews', () => {
    const trace = jest.spyOn(appLogger, 'trace').mockImplementation(() => {});
    logHotPath('request', { trace: { data: Buffer.from('sensitive-event') } });
    const output = JSON.stringify(trace.mock.calls);
    expect(output).not.toContain('sensitive-event');
    expect(output).not.toContain(
      Buffer.from('sensitive-event').toString('hex'),
    );
    trace.mockRestore();
  });

  it('does not log gRPC credentials or password fields at trace level', () => {
    const metadata = new Metadata();
    metadata.set('authorization', 'Bearer secret-token');
    metadata.set('connection-name', 'example');
    const trace = jest.spyOn(appLogger, 'trace').mockImplementation(() => {});
    logHotPath('request', {
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request: {
          options: { currentPassword: 'old-secret', newPassword: 'new-secret' },
        },
      },
    });
    const output = JSON.stringify(trace.mock.calls);
    expect(output).not.toContain('secret-token');
    expect(output).not.toContain('old-secret');
    expect(output).not.toContain('new-secret');
    trace.mockRestore();
  });
});
