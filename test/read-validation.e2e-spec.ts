import { setupAdapterBackend } from './contracts/adapter-backend';
import { registerReadValidationContractSuite } from './contracts/read-validation-contract-suite';

registerReadValidationContractSuite('Adapter', setupAdapterBackend);
