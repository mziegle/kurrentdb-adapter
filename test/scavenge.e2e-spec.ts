import { setupAdapterBackend } from './contracts/adapter-backend';
import { setupContainerBackend } from './contracts/container-backend';
import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { registerScavengeContractSuite } from './contracts/scavenge-contract-suite';

const backend = process.env.E2E_BACKEND;

registerScavengeContractSuite(
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
);
