import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { describe, it, expect, vi, afterEach } from "vitest";

import { JobQueue, type JobQueueOptions, type QueueEventListener } from "../src/index.js";
import { WorkerPool } from "../src/internal/worker-pool.js";
import type { Job } from "../src/types.js";
import type { PgQueueStore } from "../src/internal/pg-store.js";
import { testPool, testConnectionString } from "./setup.js";

interface WaitForOptions {
  timeout?: number;
  interval?: number;
}

const createdTables = new Set<string>();

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor<T>(
  predicate: () => Promise<T | undefined | null | false> | T | undefined | null | false,
  options: WaitForOptions = {}
): Promise<T> {
  const timeout = options.timeout ?? 5000;
  const interval = options.interval ?? 25;
  const start = Date.now();

  while (true) {
    const result = await predicate();
    if (result) {
      return result as T;
    }
    if (Date.now() - start > timeout) {
      throw new Error("waitFor timed out");
    }
    await wait(interval);
  }
}

function randomName(prefix: string): string {
  return `${prefix}_${randomUUID().replace(/-/g, "")}`;
}

function assertValidIdentifier(name: string): void {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(`Invalid identifier ${name}`);
  }
}

async function dropTables(): Promise<void> {
  for (const table of createdTables) {
    assertValidIdentifier(table);
    await testPool.query(`DROP TABLE IF EXISTS "${table}"`);
  }
  createdTables.clear();
}

function createQueue<TPayload>(options: Partial<JobQueueOptions> = {}) {
  const tableName = options.tableName ?? randomName("jobs");
  const queueName = options.queueName ?? randomName("queue");
  const migrationsTableName = options.migrationsTableName ?? `${tableName}_migrations`;

  const queue = new JobQueue<TPayload>({
    ...options,
    pool: testPool,
    tableName,
    queueName,
    migrationsTableName,
    pollIntervalMs: options.pollIntervalMs ?? 25,
    retryBackoff: options.retryBackoff ?? (() => 10)
  });

  createdTables.add(tableName);
  createdTables.add(migrationsTableName);

  return { queue, tableName, queueName };
}

function createOwnedQueue<TPayload>(options: Partial<JobQueueOptions> = {}) {
  const tableName = options.tableName ?? randomName("jobs");
  const queueName = options.queueName ?? randomName("queue");
  const migrationsTableName = options.migrationsTableName ?? `${tableName}_migrations`;

  const queue = new JobQueue<TPayload>({
    ...options,
    connectionString: testConnectionString,
    tableName,
    queueName,
    migrationsTableName,
    pollIntervalMs: options.pollIntervalMs ?? 25,
    retryBackoff: options.retryBackoff ?? (() => 10)
  });

  createdTables.add(tableName);
  createdTables.add(migrationsTableName);

  return { queue, tableName, queueName };
}

afterEach(async () => {
  await dropTables();
});

describe("JobQueue", () => {
  it("processes a job successfully", async () => {
    const { queue } = createQueue<{ value: number }>({ defaultMaxAttempts: 3 });

    const jobId = await queue.enqueue({ value: 42 });
    const handler = vi.fn(async () => {});

    const worker = queue.start(handler, { pollIntervalMs: 10 });

    try {
      const completedJob = await waitFor(async () => {
        const job = await queue.getJob(jobId);
        return job?.status === "completed" ? job : null;
      });

      expect(completedJob?.attempts).toBe(1);
      expect(handler).toHaveBeenCalledTimes(1);
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("retries a job until it succeeds", async () => {
    const { queue } = createQueue({ defaultMaxAttempts: 5, retryBackoff: () => 25 });
    await queue.runMigrations();

    const jobId = await queue.enqueue({ task: "retry" }, { maxAttempts: 4 });

    let attempts = 0;
    const worker = queue.start(async () => {
      attempts += 1;
      if (attempts < 3) {
        throw new Error("boom");
      }
    }, { pollIntervalMs: 10 });

    try {
      const completedJob = await waitFor(async () => {
        const job = await queue.getJob(jobId);
        return job?.status === "completed" ? job : null;
      }, { timeout: 8000 });

      expect(completedJob?.attempts).toBe(3);
      expect(attempts).toBe(3);
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("marks a job as failed after exhausting retries", async () => {
    const { queue } = createQueue({ defaultMaxAttempts: 2, retryBackoff: () => 10 });
    await queue.runMigrations();

    const jobId = await queue.enqueue({ task: "fail" }, { maxAttempts: 2 });

    const worker = queue.start(async () => {
      throw new Error("not gonna happen");
    }, { pollIntervalMs: 10 });

    try {
      const failedJob = await waitFor(async () => {
        const job = await queue.getJob(jobId);
        return job?.status === "failed" ? job : null;
      }, { timeout: 8000 });

      expect(failedJob?.attempts).toBe(2);
      expect(failedJob?.lastError).toContain("not gonna happen");
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("emits observability events", async () => {
    const { queue } = createQueue({ retryBackoff: () => 10 });
    await queue.runMigrations();

    const jobId = await queue.enqueue({ task: "observe" });

    const events: string[] = [];
    queue.on("jobStarted", ({ job }) => {
      events.push(`started:${job.id}`);
    });
    queue.on("jobCompleted", ({ job }) => {
      events.push(`completed:${job.id}`);
    });

    const worker = queue.start(async () => {}, { pollIntervalMs: 10 });

    try {
      await waitFor(() => events.includes(`completed:${jobId}`));

      expect(events).toContain(`started:${jobId}`);
      expect(events).toContain(`completed:${jobId}`);
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("supports manual migrations when autoMigrate is disabled", async () => {
    const { queue } = createQueue({ autoMigrate: false });

    await expect(queue.enqueue({ task: "manual" })).rejects.toThrow(
      /Migrations have not been run/
    );

    await queue.runMigrations();

    const jobId = await queue.enqueue({ task: "manual" });
    const worker = queue.start(async () => {}, { pollIntervalMs: 10 });

    try {
      await waitFor(async () => {
        const job = await queue.getJob(jobId);
        return job?.status === "completed" ? job : null;
      });
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("rejects invalid SQL identifiers", () => {
    expect(() => {
      // Hyphen makes the identifier invalid.
      // eslint-disable-next-line no-new
      new JobQueue({ pool: testPool, tableName: "invalid-name" });
    }).toThrow(/Invalid SQL identifier/);
  });

  it("enforces unique job identifiers", async () => {
    const { queue } = createQueue();
    await queue.runMigrations();

    const jobId = randomUUID();
    await queue.enqueue({ step: 1 }, { id: jobId });

    await expect(queue.enqueue({ step: 2 }, { id: jobId })).rejects.toThrow(
      /already exists/
    );

    await queue.destroy();
  });

  it("reports queue metrics across statuses", async () => {
    const { queue, queueName } = createQueue({ defaultMaxAttempts: 2 });
    await queue.runMigrations();

    const jobId = await queue.enqueue({ task: "metrics" }, { maxAttempts: 1 });
    const worker = queue.start(async () => {
      throw new Error("metrics failure");
    }, { pollIntervalMs: 10 });

    try {
      await waitFor(async () => {
        const job = await queue.getJob(jobId);
        return job?.status === "failed" ? job : null;
      });

      const metrics = await queue.getMetrics(queueName);
      expect(metrics.statusCounts.failed).toBe(1);
      expect(metrics.statusCounts.queued).toBe(0);
      expect(metrics.oldestQueuedAt).toBeNull();
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("serializes circular error payloads", async () => {
    const { queue } = createQueue({ defaultMaxAttempts: 1 });
    await queue.runMigrations();

    const jobId = await queue.enqueue({ task: "circular" });
    const worker = queue.start(async () => {
      const error: Record<string, unknown> = {};
      error.self = error;
      throw error;
    }, { pollIntervalMs: 10 });

    try {
      const failedJob = await waitFor(async () => {
        const job = await queue.getJob(jobId);
        return job?.status === "failed" ? job : null;
      });

      expect(failedJob?.lastError).toContain("Converting circular structure to JSON");
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("allows rerunning migrations without reapplying statements", async () => {
    const { queue } = createQueue();

    await queue.runMigrations();
    await expect(queue.runMigrations()).resolves.toBeUndefined();

    await queue.destroy();
  });

  it("deduplicates concurrent migration requests", async () => {
    const { queue } = createQueue();

    await Promise.all([queue.runMigrations(), queue.runMigrations()]);

    await queue.destroy();
  });

  it("emits errors when storage interactions fail", async () => {
    const { queue } = createQueue({ pollIntervalMs: 0 });
    await queue.runMigrations();

    const originalClaim = (queue as unknown as { store: { claimJob: () => Promise<unknown> } }).store
      .claimJob.bind((queue as unknown as { store: { claimJob: () => Promise<unknown> } }).store);

    let invocation = 0;
    const store = (queue as unknown as { store: { claimJob: (queueName: string) => Promise<unknown> } })
      .store;
    store.claimJob = async (queueName: string) => {
      invocation += 1;
      if (invocation === 1) {
        throw new Error("claim failed");
      }
      return originalClaim(queueName);
    };

    const errors: unknown[] = [];
    queue.on("error", ({ error }) => {
      errors.push(error);
    });

    const controller = queue.start(async () => {}, { pollIntervalMs: 0 });

    try {
      await waitFor(() => (errors.length > 0));

      await wait(25);

      expect(errors[0]).toBeInstanceOf(Error);
    } finally {
      await controller.stop();
      await queue.destroy();
    }
  });

  it("destroys owned pools when tearing down", async () => {
    const { queue, tableName } = createOwnedQueue();

    await queue.runMigrations();

    const pool = (queue as unknown as { pool: { end: () => Promise<void> } }).pool;
    const endSpy = vi.spyOn(pool, "end");

    await queue.destroy();

    expect(endSpy).toHaveBeenCalled();

    createdTables.add(tableName); // ensure cleanup in afterEach
  });

  it("prevents starting workers twice", async () => {
    const { queue } = createQueue();
    await queue.runMigrations();

    const worker = queue.start(async () => {}, { pollIntervalMs: 10 });

    await expect(() => queue.start(async () => {})).toThrow(/Workers already running/);

    await worker.stop();
    await queue.destroy();
  });

  it("links abort signals to worker shutdown", async () => {
    const { queue } = createQueue();
    await queue.runMigrations();

    const abortController = new AbortController();
    const worker = queue.start(async () => {}, {
      pollIntervalMs: 10,
      signal: abortController.signal
    });

    abortController.abort();
    await wait(50);

    await worker.stop();
    await queue.destroy();
  });

  it("stops workers immediately when signal is already aborted", async () => {
    const { queue } = createQueue();
    await queue.runMigrations();

    const abortController = new AbortController();
    abortController.abort();

    const worker = queue.start(async () => {}, {
      pollIntervalMs: 10,
      signal: abortController.signal
    });

    await worker.stop();
    await queue.destroy();
  });

  it("preserves string errors when serializing failures", async () => {
    const { queue } = createQueue({ defaultMaxAttempts: 1 });
    await queue.runMigrations();

    const jobId = await queue.enqueue({ task: "string-error" });
    const worker = queue.start(async () => {
      throw "plain-string";
    }, { pollIntervalMs: 10 });

    try {
      const failedJob = await waitFor(async () => {
        const job = await queue.getJob(jobId);
        return job?.status === "failed" ? job : null;
      });

      expect(failedJob?.lastError).toBe("plain-string");
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("exposes store metadata via getters", async () => {
    const { queue, tableName } = createQueue();
    await queue.runMigrations();

    const store = (queue as unknown as { store: { tableName: string; jobsIndexName: string } }).store;

    expect(store.tableName).toBe(tableName);
    expect(store.jobsIndexName).toContain(tableName);

    await queue.destroy();
  });

  it("requires allowAuto when migrations are not forced", async () => {
    const { queue } = createQueue();
    const migrations = (queue as unknown as { migrations: { ensure: (input: { allowAuto: boolean }) => Promise<void> } })
      .migrations;

    await expect(migrations.ensure({ allowAuto: false })).rejects.toThrow(
      /Migrations have not been run/
    );

    await queue.destroy();
  });

  it("emits an error when migrations are required but disabled", async () => {
    const { queue } = createQueue({ autoMigrate: false });

    const errors: unknown[] = [];
    queue.on("error", ({ error }) => {
      errors.push(error);
    });

    const worker = queue.start(async () => {}, { pollIntervalMs: 10 });

    try {
      await waitFor(() => (errors.length > 0));

      expect(errors[0]).toBeInstanceOf(Error);
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("processes jobs concurrently when configured", async () => {
    const { queue } = createQueue();
    await queue.runMigrations();

    const processed: string[] = [];

    const worker = queue.start(async (job) => {
      processed.push(job.id);
      await wait(50);
    }, { concurrency: 2, pollIntervalMs: 1 });

    const jobIds = await Promise.all([
      queue.enqueue({ order: 1 }),
      queue.enqueue({ order: 2 })
    ]);

    try {
      await waitFor(() => (processed.length === 2 ? processed : null));
      expect(new Set(processed)).toEqual(new Set(jobIds));
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("supports removing event listeners", async () => {
    const { queue } = createQueue<{ task: string }>();
    await queue.runMigrations();

    const events: string[] = [];
    const handler: QueueEventListener<{ task: string }, "jobStarted"> = ({ job }) => {
      events.push(job.id);
    };

    queue.on("jobStarted", handler);
    queue.off("jobStarted", handler);

    const worker = queue.start(async () => {}, { pollIntervalMs: 10 });

    try {
      await queue.enqueue({ task: "ignored" });
      await wait(50);
      expect(events).toHaveLength(0);
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("fires once listeners a single time", async () => {
    const { queue } = createQueue();
    await queue.runMigrations();

    const triggered: string[] = [];
    queue.once("jobCompleted", ({ job }) => {
      triggered.push(job.id);
    });

    const worker = queue.start(async () => {}, { pollIntervalMs: 10 });

    try {
      await queue.enqueue({ order: 1 });
      await queue.enqueue({ order: 2 });

      await waitFor(() => (triggered.length > 0 ? triggered : null));

      expect(triggered).toHaveLength(1);
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });

  it("requires a connection string when no pool is provided", () => {
    const originalPostgresUrl = process.env.POSTGRES_URL;
    const originalDatabaseUrl = process.env.DATABASE_URL;

    // Ensure the environment does not satisfy the constructor.
    delete process.env.POSTGRES_URL;
    delete process.env.DATABASE_URL;

    expect(() => {
      // eslint-disable-next-line no-new
      new JobQueue();
    }).toThrow(/connection string is required/);

    if (originalPostgresUrl !== undefined) {
      process.env.POSTGRES_URL = originalPostgresUrl;
    } else {
      delete process.env.POSTGRES_URL;
    }
    if (originalDatabaseUrl !== undefined) {
      process.env.DATABASE_URL = originalDatabaseUrl;
    } else {
      delete process.env.DATABASE_URL;
    }
  });

  it("returns null when requesting an unknown job", async () => {
    const { queue } = createQueue();
    await queue.runMigrations();

    const missing = await queue.getJob(randomUUID());
    expect(missing).toBeNull();

    await queue.destroy();
  });

  it("skips redundant migration runs once complete", async () => {
    const { queue } = createQueue();

    await queue.runMigrations();

    const migrations = (queue as unknown as {
      migrations: {
        ensure: (input: { allowAuto: boolean }) => Promise<void>;
      };
    }).migrations;

    await migrations.ensure({ allowAuto: true });
    await migrations.ensure({ allowAuto: true });

    await queue.destroy();
  });

  it("shares a single migration run across concurrent callers", async () => {
    const { queue } = createQueue();

    const migrations = (queue as unknown as {
      migrations: {
        ensure: (input: { allowAuto: boolean }) => Promise<void>;
      };
    }).migrations;

    await Promise.all([migrations.ensure({ allowAuto: true }), migrations.ensure({ allowAuto: true })]);

    await queue.destroy();
  });

  it("rolls back migrations when a statement fails", async () => {
    const { queue } = createQueue();

    const pool = (queue as unknown as { pool: { connect: () => Promise<unknown> } }).pool;
    const store = (queue as unknown as { store: { tableName: string } }).store;

    let createJobTableAttempted = false;
    const rollbackCalls: string[] = [];

    const mockClient = {
      query: vi.fn(async (sql: string) => {
        if (sql.startsWith("CREATE TABLE IF NOT EXISTS") && sql.includes("_migrations")) {
          return;
        }
        if (sql === "BEGIN") {
          return;
        }
        if (sql.startsWith("LOCK TABLE")) {
          return;
        }
        if (sql.startsWith("SELECT 1 FROM")) {
          return { rowCount: 0 };
        }
        if (sql.startsWith("CREATE TABLE IF NOT EXISTS") && sql.includes(store.tableName)) {
          createJobTableAttempted = true;
          throw new Error("migration boom");
        }
        if (sql === "ROLLBACK") {
          rollbackCalls.push(sql);
          return;
        }
        throw new Error(`Unexpected query: ${sql}`);
      }),
      release: vi.fn()
    };

    const connectSpy = vi.spyOn(pool, "connect").mockResolvedValue(mockClient as never);

    const migrations = (queue as unknown as {
      migrations: {
        ensure: (input: { allowAuto: boolean; force?: boolean }) => Promise<void>;
      };
    }).migrations;

    await expect(migrations.ensure({ allowAuto: true, force: true })).rejects.toThrow(/migration boom/);

    expect(createJobTableAttempted).toBe(true);
    expect(rollbackCalls).toContain("ROLLBACK");

    connectSpy.mockRestore();

    await queue.destroy();
  });

  it("skips already applied migrations", async () => {
    const { queue } = createQueue();

    const pool = (queue as unknown as { pool: { connect: () => Promise<unknown> } }).pool;
    const store = (queue as unknown as { store: { quotedTableName: string } }).store;

    const executedStatements: string[] = [];

    const mockClient = {
      query: vi.fn(async (sql: string) => {
        executedStatements.push(sql);

        if (sql.startsWith("CREATE TABLE IF NOT EXISTS") && sql.includes("_migrations")) {
          return;
        }
        if (sql === "BEGIN") {
          return;
        }
        if (sql.startsWith("LOCK TABLE")) {
          return;
        }
        if (sql.startsWith("SELECT 1 FROM")) {
          return { rowCount: 1 };
        }
        if (sql === "ROLLBACK") {
          return;
        }

        throw new Error(`Unexpected query ${sql}`);
      }),
      release: vi.fn()
    };

    const connectSpy = vi.spyOn(pool, "connect").mockResolvedValue(mockClient as never);

    const migrations = (queue as unknown as {
      migrations: {
        ensure: (input: { allowAuto: boolean; force?: boolean }) => Promise<void>;
      };
    }).migrations;

    await migrations.ensure({ allowAuto: true, force: true });

    connectSpy.mockRestore();

    expect(executedStatements).toContain("ROLLBACK");
    expect(executedStatements.some((sql) => sql.includes(store.quotedTableName))).toBe(false);

    await queue.destroy();
  });

  it("short-circuits re-applied migrations", async () => {
    const { queue } = createQueue();

    await queue.runMigrations();

    const migrations = (queue as unknown as {
      migrations: {
        ensure: (input: { allowAuto: boolean }) => Promise<void>;
      };
    }).migrations as unknown as { ensure: (input: { allowAuto: boolean }) => Promise<void>; migrationsComplete: boolean };

    migrations.migrationsComplete = false;

    await migrations.ensure({ allowAuto: true });

    await queue.destroy();
  });

  it("clamps configuration values to safe minimums", async () => {
    const tableName = randomName("jobs");
    const queueName = randomName("queue");
    const migrationsTableName = `${tableName}_migrations`;

    const queue = new JobQueue({
      pool: testPool,
      tableName,
      queueName,
      migrationsTableName,
      pollIntervalMs: 5,
      defaultMaxAttempts: 0,
      concurrency: 0
    });

    createdTables.add(tableName);
    createdTables.add(migrationsTableName);

    const config = (queue as unknown as {
      config: {
        pollIntervalMs: number;
        defaultMaxAttempts: number;
        concurrency: number;
      };
    }).config;

    expect(config.pollIntervalMs).toBe(10);
    expect(config.defaultMaxAttempts).toBe(1);
    expect(config.concurrency).toBe(1);

    await queue.destroy();
  });

  it("falls back to default identifiers and queue name", async () => {
    const queue = new JobQueue({ pool: testPool });
    createdTables.add("skiplane_jobs");
    createdTables.add("skiplane_jobs_migrations");

    await queue.runMigrations();

    const config = (queue as unknown as {
      config: {
        tableName: string;
        queueName: string;
        retryBackoff: (attempt: number) => number;
      };
    }).config;

    expect(config.tableName).toBe("skiplane_jobs");
    expect(config.queueName).toBe("default");
    expect(config.retryBackoff(3)).toBe(Math.min(60000, 2 ** (3 - 1) * 1000));

    await queue.destroy();
  });

  it("serializes errors without stack traces", async () => {
    const { queue } = createQueue();
    await queue.runMigrations();

    const serialize = (queue as unknown as {
      serializeError: (error: unknown) => string;
    }).serializeError.bind(queue);

    const err = new Error("no stack");
    Object.defineProperty(err, "stack", { value: undefined });

    const result = serialize(err);
    expect(result).toBe("no stack");

    await queue.destroy();
  });

  it("uses configured defaults when start options are omitted", async () => {
    const { queue } = createQueue({ pollIntervalMs: 25, concurrency: 3 });
    await queue.runMigrations();

    const worker = queue.start(async () => {});

    try {
      const workerPool = (queue as unknown as {
        workerPool: {
          options: { pollIntervalMs: number; concurrency: number };
        };
      }).workerPool;

      expect(workerPool.options.pollIntervalMs).toBe(25);
      expect(workerPool.options.concurrency).toBe(3);
    } finally {
      await worker.stop();
      await queue.destroy();
    }
  });
});

describe("WorkerPool", () => {
  function createStubStore(): PgQueueStore<Job<unknown>> {
    const row = {
      id: randomUUID(),
      queue_name: "stub",
      payload: { value: 1 },
      status: "queued" as const,
      attempts: 0,
      max_attempts: 1,
      available_at: new Date(),
      created_at: new Date(),
      updated_at: new Date(),
      started_at: null,
      finished_at: null,
      last_error: null
    };

    const store = {
      claimJob: vi.fn().mockResolvedValue(null),
      completeJob: vi.fn().mockResolvedValue(undefined),
      scheduleRetry: vi.fn().mockResolvedValue(undefined),
      failJob: vi.fn().mockResolvedValue(undefined),
      mapRowToJob: vi.fn().mockImplementation(() => ({
        id: row.id,
        queue: row.queue_name,
        payload: row.payload,
        status: "queued" as const,
        attempts: 1,
        maxAttempts: 1,
        availableAt: row.available_at,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        startedAt: row.started_at,
        finishedAt: row.finished_at,
        lastError: row.last_error
      }))
    } as Partial<PgQueueStore<Job<unknown>>>;

    return store as PgQueueStore<Job<unknown>>;
  }

  it("throws when started twice", async () => {
    const store = createStubStore();
    const events = new EventEmitter();
    const pool = new WorkerPool(store, {
      pollIntervalMs: 5,
      concurrency: 1,
      queueName: "stub",
      ensureReady: async () => {},
      retryBackoff: () => 0,
      serializeError: (error) => String(error),
      events
    });

    const controller = pool.start(async () => {});

    expect(() => pool.start(async () => {})).toThrow(/Workers already running/);

    await controller.stop();
  });

  it("resolves immediately when delay receives zero", async () => {
    const store = createStubStore();
    const events = new EventEmitter();
    const pool = new WorkerPool(store, {
      pollIntervalMs: 5,
      concurrency: 1,
      queueName: "stub",
      ensureReady: async () => {},
      retryBackoff: () => 0,
      serializeError: (error) => String(error),
      events
    });

    const delay = (pool as unknown as {
      delay: (ms: number, signal: AbortSignal) => Promise<void>;
    }).delay.bind(pool);

    await delay(0, new AbortController().signal);
  });
});
