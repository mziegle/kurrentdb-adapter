import { setupAdapterBackend } from './contracts/adapter-backend';
import { registerStreamsContractSuite } from './contracts/streams-contract-suite';

registerStreamsContractSuite('Adapter', setupAdapterBackend, {
  supportsRestart: true,
});
