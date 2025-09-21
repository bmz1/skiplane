import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { Pool } from "pg";

import { MigrationController } from "./internal/migration-controller.js";
import { PgQueueStore } from "./internal/pg-store.js";
import { assertValidIdentifier } from "./internal/sql.js";
import { WorkerPool, type WorkerController } from "./internal/worker-pool.js";
import type {
  Job,
  JobHandler,
  QueueEventListener,
  QueueEventName,
  QueueMetrics,
} from "./types.js";

export interface JobQueueOptions {
  connectionString?: string;
  pool?: Pool;
  tableName?: string;
  queueName?: string;
  pollIntervalMs?: number;
  defaultMaxAttempts?: number;
  retryBackoff?: (attempt: number) => number;
  concurrency?: number;
  autoMigrate?: boolean;
  migrationsTableName?: string;
}

export interface EnqueueOptions {
  id?: string;
  queueName?: string;
  runAt?: Date;
  maxAttempts?: number;
}

export interface WorkerOptions {
  concurrency?: number;
  signal?: AbortSignal;
  pollIntervalMs?: number;
  queueName?: string;
}

export type { Job, JobHandler, JobStatus, QueueMetrics } from "./types.js";
export type {
  QueueEvents,
  QueueEventListener,
  QueueEventName,
} from "./types.js";
export type { WorkerController } from "./internal/worker-pool.js";

interface QueueConfig {
  tableName: string;
  queueName: string;
  pollIntervalMs: number;
  defaultMaxAttempts: number;
  concurrency: number;
  autoMigrate: boolean;
  retryBackoff: (attempt: number) => number;
  migrationsTableName: string;
}

export class JobQueue<TPayload = unknown> {
  private readonly pool: Pool;
  private readonly ownsPool: boolean;
  private readonly config: QueueConfig;
  private readonly store: PgQueueStore<TPayload>;
  private readonly migrations: MigrationController;
  private readonly events = new EventEmitter();
  private workerPool?: WorkerPool<TPayload>;

  public constructor(options: JobQueueOptions = {}) {
    const pool = options.pool;
    const connectionString =
      options.connectionString ??
      process.env.POSTGRES_URL ??
      process.env.DATABASE_URL;

    if (!pool && !connectionString) {
      throw new Error(
        "A connection string is required when no Pool instance is provided.",
      );
    }

    this.pool = pool ?? new Pool({ connectionString });
    this.ownsPool = pool === undefined;

    const tableName = options.tableName ?? "skiplane_jobs";
    const migrationsTableName =
      options.migrationsTableName ?? `${tableName}_migrations`;

    assertValidIdentifier(tableName);
    assertValidIdentifier(migrationsTableName);

    this.config = {
      tableName,
      queueName: options.queueName ?? "default",
      pollIntervalMs: Math.max(10, options.pollIntervalMs ?? 200),
      defaultMaxAttempts: Math.max(1, options.defaultMaxAttempts ?? 5),
      concurrency: Math.max(1, options.concurrency ?? 1),
      autoMigrate: options.autoMigrate ?? true,
      retryBackoff:
        options.retryBackoff ??
        ((attempt) => Math.min(60000, 2 ** (attempt - 1) * 1000)),
      migrationsTableName,
    };

    this.store = new PgQueueStore<TPayload>(this.pool, {
      tableName: this.config.tableName,
      defaultMaxAttempts: this.config.defaultMaxAttempts,
    });

    this.migrations = new MigrationController(
      this.pool,
      this.store as PgQueueStore<unknown>,
      {
        migrationsTableName: this.config.migrationsTableName,
        defaultMaxAttempts: this.config.defaultMaxAttempts,
      },
    );
  }

  public on<K extends QueueEventName<TPayload>>(
    event: K,
    listener: QueueEventListener<TPayload, K>,
  ): this {
    this.events.on(event as string, listener as (...args: unknown[]) => void);
    return this;
  }

  public off<K extends QueueEventName<TPayload>>(
    event: K,
    listener: QueueEventListener<TPayload, K>,
  ): this {
    this.events.off(event as string, listener as (...args: unknown[]) => void);
    return this;
  }

  public once<K extends QueueEventName<TPayload>>(
    event: K,
    listener: QueueEventListener<TPayload, K>,
  ): this {
    this.events.once(event as string, listener as (...args: unknown[]) => void);
    return this;
  }

  public async runMigrations(): Promise<void> {
    await this.migrations.ensure({ allowAuto: true, force: true });
  }

  public async destroy(): Promise<void> {
    await this.stop();
    if (this.ownsPool) {
      await this.pool.end();
    }
  }

  public async enqueue(
    payload: TPayload,
    options: EnqueueOptions = {},
  ): Promise<string> {
    await this.ensureMigrations({ allowAuto: this.config.autoMigrate });

    const id = options.id ?? randomUUID();
    const queueName = options.queueName ?? this.config.queueName;
    const availableAt = options.runAt ?? new Date();
    const maxAttempts = Math.max(
      1,
      options.maxAttempts ?? this.config.defaultMaxAttempts,
    );

    return this.store.enqueue(payload, {
      id,
      queueName,
      availableAt,
      maxAttempts,
    });
  }

  public start(
    handler: JobHandler<TPayload>,
    options: WorkerOptions = {},
  ): WorkerController {
    if (this.workerPool) {
      throw new Error(
        "Workers already running. Call stop() before starting again.",
      );
    }

    const queueName = options.queueName ?? this.config.queueName;
    const pollIntervalMs = Math.max(
      10,
      options.pollIntervalMs ?? this.config.pollIntervalMs,
    );
    const concurrency = Math.max(
      1,
      options.concurrency ?? this.config.concurrency,
    );

    const ensureReady = async (): Promise<void> => {
      await this.ensureMigrations({ allowAuto: this.config.autoMigrate });
    };

    this.workerPool = new WorkerPool<TPayload>(this.store, {
      pollIntervalMs,
      concurrency,
      queueName,
      ensureReady,
      retryBackoff: this.config.retryBackoff,
      serializeError: this.serializeError,
      events: this.events,
    });

    const controller = this.workerPool.start(handler);

    if (options.signal) {
      if (options.signal.aborted) {
        void controller.stop();
      } else {
        options.signal.addEventListener(
          "abort",
          () => {
            void controller.stop();
          },
          { once: true },
        );
      }
    }

    return controller;
  }

  public async stop(): Promise<void> {
    await this.workerPool?.stop();
    this.workerPool = undefined;
  }

  public async getJob(id: string): Promise<Job<TPayload> | null> {
    await this.ensureMigrations({ allowAuto: this.config.autoMigrate });
    return this.store.getJob(id);
  }

  public async getMetrics(
    queueName = this.config.queueName,
  ): Promise<QueueMetrics> {
    await this.ensureMigrations({ allowAuto: this.config.autoMigrate });
    return this.store.getMetrics(queueName);
  }

  private async ensureMigrations({
    allowAuto,
  }: {
    allowAuto: boolean;
  }): Promise<void> {
    if (this.migrations.isComplete()) {
      return;
    }

    if (this.config.autoMigrate || allowAuto) {
      await this.migrations.ensure({ allowAuto: true });
      return;
    }

    throw new Error(
      "Migrations have not been run. Call runMigrations() before using the queue or enable autoMigrate.",
    );
  }

  private serializeError(error: unknown): string {
    if (error instanceof Error) {
      return error.stack ?? error.message;
    }
    if (typeof error === "string") {
      return error;
    }
    try {
      return JSON.stringify(error);
    } catch (serializationError) {
      return String(serializationError);
    }
  }
}
