import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { registerScavengeContractSuite } from './contracts/scavenge-contract-suite';

registerScavengeContractSuite('KurrentDB', setupKurrentDbBackend);
