jest.mock('@nestjs/core', () => ({
  NestFactory: { createMicroservice: () => new Promise(() => {}) },
}));

import { appLogger } from './shared/logger';
import * as main from './main';
import { Socket } from 'node:net';

describe('probe request logging', () => {
  it('does not log query credentials or raw wire bytes at any level', () => {
    const logs: string[] = [];
    const spies = (['info', 'trace'] as const).map((level) =>
      jest.spyOn(appLogger, level).mockImplementation((...args: unknown[]) => {
        logs.push(JSON.stringify(args));
      }),
    );
    const log = (
      main as unknown as {
        logIncomingProbe: (socket: Socket, chunk: Buffer) => void;
      }
    ).logIncomingProbe;
    const socket = {} as Socket;
    const secret = 'probe-secret-892';
    log(socket, Buffer.from(`GET /info HTTP/1.1?token=${secret}\r\n\r\n`));
    log(
      socket,
      Buffer.from(
        `GET /other?token=${secret} HTTP/1.1\r\nAuthorization: ${secret}\r\n\r\n`,
      ),
    );
    log(socket, Buffer.from(`unknown-${secret}`));
    const output = logs.join('\n');
    expect(output).toContain('HTTP unhandled');
    expect(output).not.toContain(secret);
    expect(output).not.toContain(Buffer.from(secret).toString('hex'));
    spies.forEach((spy) => spy.mockRestore());
  });
});
