import test from 'node:test';
import assert from 'node:assert/strict';
import { inspectConfig, loadConfig } from '../dist/config/config.js';

test('uses the default KurrentDB endpoint when no config is provided', async () => {
  const previousConnection = process.env.KDB_CONNECTION;
  const previousAdapter = process.env.KDB_ADAPTER_CONNECTION;
  const previousReference = process.env.KDB_REFERENCE_CONNECTION;
  const previousCompare = process.env.KDB_COMPARE_CONNECTION;
  const previousConfigPath = process.env.KDB_CLI_CONFIG_PATH;

  delete process.env.KDB_CONNECTION;
  delete process.env.KDB_ADAPTER_CONNECTION;
  delete process.env.KDB_REFERENCE_CONNECTION;
  delete process.env.KDB_COMPARE_CONNECTION;
  process.env.KDB_CLI_CONFIG_PATH = 'missing-test-config.json';

  try {
    const config = await loadConfig();

    assert.equal(
      config.backends.adapter?.connectionString,
      'kurrentdb://127.0.0.1:2113?tls=false',
    );
    assert.equal(config.backends.reference, undefined);
  } finally {
    restoreEnv('KDB_CONNECTION', previousConnection);
    restoreEnv('KDB_ADAPTER_CONNECTION', previousAdapter);
    restoreEnv('KDB_REFERENCE_CONNECTION', previousReference);
    restoreEnv('KDB_COMPARE_CONNECTION', previousCompare);
    restoreEnv('KDB_CLI_CONFIG_PATH', previousConfigPath);
  }
});

test('shows config sources for the default endpoint', async () => {
  const previousConnection = process.env.KDB_CONNECTION;
  const previousConfigPath = process.env.KDB_CLI_CONFIG_PATH;

  process.env.KDB_CONNECTION = 'kurrentdb://127.0.0.1:9999?tls=false';
  process.env.KDB_CLI_CONFIG_PATH = 'missing-test-config.json';

  try {
    const inspection = await inspectConfig();

    assert.equal(
      inspection.defaultEndpoint.connectionString,
      'kurrentdb://127.0.0.1:9999?tls=false',
    );
    assert.equal(inspection.defaultEndpoint.source, 'env:KDB_CONNECTION');
    assert.equal(inspection.compareEndpoint, undefined);
  } finally {
    restoreEnv('KDB_CONNECTION', previousConnection);
    restoreEnv('KDB_CLI_CONFIG_PATH', previousConfigPath);
  }
});

function restoreEnv(name, value) {
  if (value === undefined) {
    delete process.env[name];
    return;
  }

  process.env[name] = value;
}
