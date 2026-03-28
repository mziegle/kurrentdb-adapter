import {
  createStreamsContractContext,
  StreamsContractBackend,
} from './contract-test-context';
import { registerAppendContractSuite } from './suites/append.contract';
import { registerBatchAppendContractSuite } from './suites/batch-append.contract';
import { registerDeleteContractSuite } from './suites/delete.contract';
import { registerReadContractSuite } from './suites/read.contract';
import { registerReadAllContractSuite } from './suites/read-all.contract';
import { registerRestartContractSuite } from './suites/restart.contract';
import { registerStreamMetadataContractSuite } from './suites/stream-metadata.contract';
import { registerSubscriptionContractSuite } from './suites/subscription.contract';
import { registerTombstoneContractSuite } from './suites/tombstone.contract';

export function registerStreamsContractSuite(
  backendName: string,
  setupBackend: () => Promise<StreamsContractBackend>,
  options?: {
    supportsRestart?: boolean;
  },
): void {
  describe(`Streams contract: ${backendName}`, () => {
    jest.setTimeout(120_000);

    let backend: StreamsContractBackend;

    beforeAll(async () => {
      backend = await setupBackend();
    });

    afterAll(async () => {
      await backend?.dispose();
    });

    const context = createStreamsContractContext(backendName, () => backend);

    registerAppendContractSuite(context);
    registerBatchAppendContractSuite(context);
    registerReadContractSuite(context);
    registerReadAllContractSuite(context);
    registerStreamMetadataContractSuite(context);
    registerSubscriptionContractSuite(context);
    registerRestartContractSuite(context, options);
    registerDeleteContractSuite(context);
    registerTombstoneContractSuite(context);
  });
}
