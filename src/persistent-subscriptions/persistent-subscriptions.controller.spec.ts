import { status } from '@grpc/grpc-js';
import { RpcException } from '@nestjs/microservices';
import { EMPTY } from 'rxjs';
import { PersistentSubscriptionsController } from './persistent-subscriptions.controller';

describe('persistent subscription stubs', () => {
  const originalBackend = process.env.EVENT_STORE_BACKEND;
  afterEach(() => {
    if (originalBackend === undefined) delete process.env.EVENT_STORE_BACKEND;
    else process.env.EVENT_STORE_BACKEND = originalBackend;
  });

  it.each([
    'read',
    'getInfo',
    'replayParked',
    'list',
    'restartSubsystem',
  ] as const)('rejects MongoDB %s with UNIMPLEMENTED', (method) => {
    process.env.EVENT_STORE_BACKEND = 'mongodb';
    const controller = new PersistentSubscriptionsController();
    try {
      Reflect.apply(controller[method], controller, [
        method === 'read' ? EMPTY : {},
      ]);
      throw new Error('expected RpcException');
    } catch (error) {
      expect(error).toBeInstanceOf(RpcException);
      expect((error as RpcException).getError()).toMatchObject({
        code: status.UNIMPLEMENTED,
      });
    }
  });

  it.each([
    'read',
    'getInfo',
    'replayParked',
    'list',
    'restartSubsystem',
  ] as const)('preserves PostgreSQL %s stub', (method) => {
    process.env.EVENT_STORE_BACKEND = 'postgres';
    const controller = new PersistentSubscriptionsController();
    expect(() =>
      Reflect.apply(controller[method], controller, [
        method === 'read' ? EMPTY : {},
      ]),
    ).not.toThrow();
  });
});
