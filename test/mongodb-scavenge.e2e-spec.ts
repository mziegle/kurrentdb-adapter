import { setupMongoDBBackend } from './contracts/mongodb-backend';
import { registerScavengeContractSuite } from './contracts/scavenge-contract-suite';

registerScavengeContractSuite('MongoDB Adapter', () =>
  setupMongoDBBackend({ unsafeScavenge: true }),
);
