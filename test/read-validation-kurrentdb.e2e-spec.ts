import { setupKurrentDbBackend } from './contracts/kurrentdb-backend';
import { registerReadValidationContractSuite } from './contracts/read-validation-contract-suite';

registerReadValidationContractSuite('KurrentDB', setupKurrentDbBackend);
