import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { existsSync } from 'node:fs';
import { join } from 'node:path';

async function bootstrap() {
  const grpcUrl = process.env.GRPC_URL ?? '0.0.0.0:2113';
  const protoDir = resolveProtoDir();

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      transport: Transport.GRPC,
      options: {
        url: grpcUrl,
        package: [
          'event_store.client.streams',
          'event_store.client.server_features',
        ],
        protoPath: [
          join(protoDir, 'streams.proto'),
          join(protoDir, 'serverfeatures.proto'),
        ],
        loader: {
          includeDirs: [protoDir],
        },
      },
    },
  );

  await app.listen();
}

bootstrap().catch((err) => {
  console.error('Error during application bootstrap:', err);
  process.exit(1);
});

function resolveProtoDir(): string {
  const candidates = [
    join(process.cwd(), 'proto/Grpc'),
    join(__dirname, '../proto/Grpc'),
    join(__dirname, 'proto/Grpc'),
  ];

  for (const candidate of candidates) {
    if (existsSync(join(candidate, 'streams.proto'))) {
      return candidate;
    }
  }

  throw new Error('Could not locate proto/Grpc/streams.proto.');
}
