const { spawnSync } = require('node:child_process');

const mode = process.argv[2];
const npmCommand =
  process.platform === 'win32'
    ? {
        command: process.env.ComSpec || 'cmd.exe',
        args: ['/d', '/s', '/c', 'npm run container:build'],
      }
    : {
        command: 'npm',
        args: ['run', 'container:build'],
      };

if (!mode) {
  console.error('Usage: node scripts/run-e2e.js <e2e|dev> [jest args...]');
  process.exit(1);
}

const jestArgs = process.argv.slice(3);

const backends = mode === 'e2e' ? ['container', 'kurrentdb'] : ['dev'];

for (const backend of backends) {
  if (backend === 'container') {
    const buildResult = spawnSync(npmCommand.command, npmCommand.args, {
      stdio: 'inherit',
      env: process.env,
    });

    if ((buildResult.status ?? 1) !== 0) {
      process.exit(buildResult.status ?? 1);
    }
  }

  const result = spawnSync(
    process.execPath,
    [
      'node_modules/jest/bin/jest.js',
      '--config',
      './test/jest-e2e.json',
      ...jestArgs,
    ],
    {
      stdio: 'inherit',
      env: {
        ...process.env,
        E2E_BACKEND: backend,
      },
    },
  );

  if ((result.status ?? 1) !== 0) {
    process.exit(result.status ?? 1);
  }
}
