import { Injectable } from '@nestjs/common';
import { Pool, PoolClient } from 'pg';

@Injectable()
export class PostgresInfrastructureService {
  createPool(): Pool {
    return new Pool(this.getPoolConfig());
  }

  async initializeSchema(client: PoolClient): Promise<void> {
    await client.query(`
      CREATE TABLE IF NOT EXISTS stream_events (
        global_position BIGSERIAL PRIMARY KEY,
        stream_name TEXT NOT NULL,
        stream_generation BIGINT NOT NULL DEFAULT 0,
        stream_revision BIGINT NOT NULL,
        event_id UUID NOT NULL UNIQUE,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        custom_metadata BYTEA NOT NULL DEFAULT '\\x',
        data BYTEA NOT NULL DEFAULT '\\x',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (stream_name, stream_generation, stream_revision)
      )
    `);

    await client.query(`
      ALTER TABLE stream_events
      ADD COLUMN IF NOT EXISTS stream_generation BIGINT NOT NULL DEFAULT 0
    `);

    await client.query(`
      ALTER TABLE stream_events
      DROP CONSTRAINT IF EXISTS stream_events_stream_name_stream_revision_key
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_stream_events_stream_revision
      ON stream_events (stream_name, stream_generation, stream_revision)
    `);

    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_events_stream_generation_revision_unique
      ON stream_events (stream_name, stream_generation, stream_revision)
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS tombstoned_streams (
        stream_name TEXT PRIMARY KEY,
        tombstoned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_position BIGINT NOT NULL
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS stream_retention_policies (
        stream_name TEXT PRIMARY KEY,
        current_generation BIGINT NOT NULL DEFAULT 0,
        current_revision BIGINT NULL,
        deleted_at TIMESTAMPTZ NULL,
        max_age BIGINT NULL,
        max_count BIGINT NULL,
        truncate_before BIGINT NULL
      )
    `);

    await client.query(`
      ALTER TABLE stream_retention_policies
      ADD COLUMN IF NOT EXISTS current_generation BIGINT NOT NULL DEFAULT 0
    `);

    await client.query(`
      ALTER TABLE stream_retention_policies
      ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ NULL
    `);

    await client.query(`
      ALTER TABLE stream_retention_policies
      ADD COLUMN IF NOT EXISTS max_age BIGINT NULL
    `);
  }

  private getPoolConfig(): ConstructorParameters<typeof Pool>[0] {
    if (process.env.POSTGRES_URL) {
      return { connectionString: process.env.POSTGRES_URL };
    }

    return {
      host: process.env.POSTGRES_HOST ?? 'localhost',
      port: Number(process.env.POSTGRES_PORT ?? 5432),
      database: process.env.POSTGRES_DB ?? 'kurrentdb_adapter',
      user: process.env.POSTGRES_USER ?? 'postgres',
      password: process.env.POSTGRES_PASSWORD ?? 'postgres',
    };
  }
}
