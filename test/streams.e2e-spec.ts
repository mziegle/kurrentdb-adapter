import { setupAdapterBackend } from './contracts/adapter-backend';
import { setupContainerBackend } from './contracts/container-backend';
import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { registerStreamsContractSuite } from './contracts/streams-contract-suite';

const backend = process.env.E2E_BACKEND;

registerStreamsContractSuite(
  backend === 'container'
    ? 'Adapter Container'
    : backend === 'kurrentdb'
      ? 'KurrentDB'
      : 'Adapter',
  backend === 'container'
    ? setupContainerBackend
    : backend === 'kurrentdb'
      ? setupKurrentDbBackend
      : setupAdapterBackend,
  {
    supportsRestart:
      backend !== 'kurrentdb' || !process.env.KURRENTDB_TEST_CONNECTION_STRING,
  },
);
