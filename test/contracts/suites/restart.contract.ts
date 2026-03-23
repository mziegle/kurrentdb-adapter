import { jsonEvent } from '@kurrent/kurrentdb-client';
import { StreamsContractContext } from '../contract-test-context';

export function registerRestartContractSuite(
  context: StreamsContractContext,
  options?: {
    supportsRestart?: boolean;
  },
): void {
  describe('Restart', () => {
    const restartTest = (options?.supportsRestart ?? true) ? it : it.skip;

    restartTest(
      'keeps persisted events after restarting the backend',
      async () => {
        const streamName = context.createStreamName('restart-persistence');

        await context
          .backend()
          .getClient()
          .appendToStream(streamName, [
            jsonEvent({
              type: 'booking-created',
              data: { step: 1 },
            }),
            jsonEvent({
              type: 'booking-confirmed',
              data: { step: 2 },
            }),
          ]);

        await context.backend().restart();

        expect(await context.readStreamEvents(streamName)).toEqual([
          {
            type: 'booking-created',
            data: { step: 1 },
          },
          {
            type: 'booking-confirmed',
            data: { step: 2 },
          },
        ]);
      },
    );
  });
}
