import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { registerStreamsContractSuite } from './contracts/streams-contract-suite';

registerStreamsContractSuite('KurrentDB', setupKurrentDbBackend, {
  supportsRestart: !process.env.KURRENTDB_TEST_CONNECTION_STRING,
});
