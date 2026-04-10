import pino from 'pino';

function shouldUsePrettyTransport(): boolean {
  if (process.env.LOG_PRETTY === 'false') {
    return false;
  }

  if (process.env.LOG_PRETTY === 'true') {
    return true;
  }

  if (process.env.NODE_ENV === 'production') {
    return false;
  }

  try {
    require.resolve('pino-pretty');
    return true;
  } catch {
    return false;
  }
}

export const appLogger = pino({
  level: process.env.LOG_LEVEL ?? 'info',
  base: undefined,
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: {
    level: (label) => ({ level: label }),
  },
  transport: shouldUsePrettyTransport()
    ? {
        target: 'pino-pretty',
        options: {
          colorize: false,
          translateTime: 'SYS:standard',
          ignore: 'pid,hostname',
          singleLine: true,
        },
      }
    : undefined,
});
