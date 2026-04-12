import { Injectable } from '@nestjs/common';
import { Pool, PoolClient } from 'pg';

@Injectable()
export class PostgresSubscriptionService {
  private listenClient: PoolClient | null = null;
  private readonly streamVersions = new Map<string, number>();
  private readonly streamListeners = new Map<string, Set<() => void>>();

  async initialize(
    pool: Pool,
    updatesChannel: string,
    allStreamKey: string,
  ): Promise<void> {
    this.listenClient = await pool.connect();
    this.listenClient.on('notification', (notification) => {
      if (notification.channel !== updatesChannel) {
        return;
      }

      const streamName = notification.payload;
      if (!streamName) {
        return;
      }

      this.bumpStreamVersion(streamName);
      this.bumpStreamVersion(allStreamKey);
    });
    await this.listenClient.query(`LISTEN ${updatesChannel}`);
  }

  async destroy(updatesChannel: string): Promise<void> {
    if (!this.listenClient) {
      return;
    }

    try {
      await this.listenClient.query(`UNLISTEN ${updatesChannel}`);
    } finally {
      this.listenClient.release();
      this.listenClient = null;
    }
  }

  async notifyStreamUpdated(
    streamName: string,
    updatesChannel: string,
    client: PoolClient | Pool,
  ): Promise<void> {
    await client.query('SELECT pg_notify($1, $2)', [
      updatesChannel,
      streamName,
    ]);
  }

  getStreamVersion(streamName: string): number {
    return this.streamVersions.get(streamName) ?? 0;
  }

  waitForStreamUpdate(
    streamName: string,
    observedVersion: number,
    isCancelled: () => boolean,
  ): Promise<void> {
    if (
      isCancelled() ||
      this.getStreamVersion(streamName) !== observedVersion
    ) {
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      const listeners =
        this.streamListeners.get(streamName) ?? new Set<() => void>();
      let settled = false;

      const finish = () => {
        if (settled) {
          return;
        }

        settled = true;
        clearInterval(interval);

        listeners.delete(finish);
        if (listeners.size === 0) {
          this.streamListeners.delete(streamName);
        }

        resolve();
      };

      listeners.add(finish);
      this.streamListeners.set(streamName, listeners);

      const interval = setInterval(() => {
        if (
          isCancelled() ||
          this.getStreamVersion(streamName) !== observedVersion
        ) {
          finish();
        }
      }, 100);
    });
  }

  private bumpStreamVersion(streamName: string): void {
    const nextVersion = this.getStreamVersion(streamName) + 1;
    this.streamVersions.set(streamName, nextVersion);

    const listeners = this.streamListeners.get(streamName);
    if (!listeners) {
      return;
    }

    this.streamListeners.delete(streamName);
    for (const listener of listeners) {
      listener();
    }
  }
}
