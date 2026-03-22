import { createServer, Server } from 'node:net';

export async function getFreePort(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const server: Server = createServer();

    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        server.close(() => reject(new Error('Unable to acquire a free port.')));
        return;
      }

      const { port } = address;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve(port);
      });
    });

    server.on('error', reject);
  });
}
