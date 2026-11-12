import { setupMongoDBBackend } from './contracts/mongodb-backend';
import { registerStreamsContractSuite } from './contracts/streams-contract-suite';

registerStreamsContractSuite('MongoDB Adapter', setupMongoDBBackend);
