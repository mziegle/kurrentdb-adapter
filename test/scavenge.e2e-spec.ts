import { setupAdapterBackend } from './contracts/adapter-backend';
import { registerScavengeContractSuite } from './contracts/scavenge-contract-suite';

registerScavengeContractSuite('Adapter', setupAdapterBackend);
