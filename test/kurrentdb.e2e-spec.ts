import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { registerStreamsContractSuite } from './contracts/streams-contract-suite';

registerStreamsContractSuite('KurrentDB', setupKurrentDbBackend, {
  supportsRestart: false,
});
