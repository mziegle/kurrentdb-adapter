import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';

type TraceOptions = {
  proxyHost?: string;
  proxyPort?: string;
  suppressHttp1Bodies?: boolean;
  suppressHttp1Headers?: boolean;
  suppressHttp2FrameTypes?: string;
  suppressHttpPaths?: string;
  upstreamHost?: string;
  upstreamPort?: string;
  useDefaultSuppressions?: boolean;
  verbosity?: string;
};

export async function runTraceProxy(options: TraceOptions): Promise<void> {
  const repoRoot = resolveRepoRoot();
  const traceScriptPath = path.join(repoRoot, 'scripts', 'trace-kurrentdb.js');

  if (!existsSync(traceScriptPath)) {
    throw new Error(`Could not find trace script at ${traceScriptPath}`);
  }

  const child = spawn(process.execPath, [traceScriptPath], {
    cwd: repoRoot,
    stdio: 'inherit',
    env: {
      ...process.env,
      ...(options.proxyHost
        ? { TRACE_PROXY_HOST: options.proxyHost }
        : {}),
      ...(options.proxyPort
        ? { TRACE_PROXY_PORT: options.proxyPort }
        : {}),
      ...(options.upstreamHost
        ? { TRACE_UPSTREAM_HOST: options.upstreamHost }
        : {}),
      ...(options.upstreamPort
        ? { TRACE_UPSTREAM_PORT: options.upstreamPort }
        : {}),
      ...(options.verbosity
        ? { TRACE_VERBOSITY: options.verbosity }
        : {}),
      ...(options.useDefaultSuppressions !== undefined
        ? {
            TRACE_USE_DEFAULT_SUPPRESSIONS: options.useDefaultSuppressions
              ? '1'
              : '0',
          }
        : {}),
      ...(options.suppressHttpPaths
        ? { TRACE_SUPPRESS_HTTP_PATHS: options.suppressHttpPaths }
        : {}),
      ...(options.suppressHttp2FrameTypes
        ? {
            TRACE_SUPPRESS_HTTP2_FRAME_TYPES:
              options.suppressHttp2FrameTypes,
          }
        : {}),
      ...(options.suppressHttp1Headers
        ? { TRACE_SUPPRESS_HTTP1_HEADERS: '1' }
        : {}),
      ...(options.suppressHttp1Bodies
        ? { TRACE_SUPPRESS_HTTP1_BODIES: '1' }
        : {}),
    },
  });

  await new Promise<void>((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (signal) {
        resolve();
        return;
      }

      if (code === 0) {
        resolve();
        return;
      }

      reject(
        new Error(`Trace proxy exited with code ${code ?? 'unknown'}`),
      );
    });
  });
}

function resolveRepoRoot(): string {
  const candidates = [
    process.cwd(),
    path.resolve(process.cwd(), '..'),
    path.resolve(process.cwd(), '../..'),
  ];

  for (const candidate of candidates) {
    if (existsSync(path.join(candidate, 'scripts', 'trace-kurrentdb.js'))) {
      return candidate;
    }
  }

  throw new Error(
    'Could not locate the repo root containing scripts/trace-kurrentdb.js.',
  );
}
