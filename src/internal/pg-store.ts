import type { Pool } from "pg";

import { quoteIdentifier } from "./sql.js";
import type { Job, JobStatus, QueueMetrics } from "../types.js";

interface PgStoreOptions {
  tableName: string;
  defaultMaxAttempts: number;
}

interface JobRow<TPayload> {
  id: string;
  queue_name: string;
  payload: TPayload;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  available_at: Date;
  created_at: Date;
  updated_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
  last_error: string | null;
}

export class PgQueueStore<TPayload> {
  private readonly table: string;
  private readonly quotedTable: string;
  private readonly jobsIndex: string;
  private readonly quotedJobsIndex: string;

  public constructor(
    private readonly pool: Pool,
    options: PgStoreOptions,
  ) {
    this.table = options.tableName;
    this.quotedTable = quoteIdentifier(options.tableName);
    this.jobsIndex = `${options.tableName}_queue_status_available_idx`;
    this.quotedJobsIndex = quoteIdentifier(this.jobsIndex);
  }

  public get tableName(): string {
    return this.table;
  }

  public get quotedTableName(): string {
    return this.quotedTable;
  }

  public get jobsIndexName(): string {
    return this.jobsIndex;
  }

  public get quotedJobsIndexName(): string {
    return this.quotedJobsIndex;
  }

  public async enqueue(
    payload: TPayload,
    options: {
      id: string;
      queueName: string;
      availableAt: Date;
      maxAttempts: number;
    },
  ): Promise<string> {
    const text = `
      INSERT INTO ${this.quotedTable}
        (id, queue_name, payload, status, attempts, max_attempts, available_at, created_at, updated_at)
      VALUES ($1, $2, $3::jsonb, 'queued', 0, $4, $5, NOW(), NOW())
      ON CONFLICT (id) DO NOTHING
    `;

    const values = [
      options.id,
      options.queueName,
      JSON.stringify(payload),
      options.maxAttempts,
      options.availableAt,
    ];

    const result = await this.pool.query(text, values);
    if (result.rowCount === 0) {
      throw new Error(`Job with id ${options.id} already exists.`);
    }

    return options.id;
  }

  public async getJob(id: string): Promise<Job<TPayload> | null> {
    const result = await this.pool.query<JobRow<TPayload>>(
      `SELECT * FROM ${this.quotedTable} WHERE id = $1`,
      [id],
    );
    const row = result.rows[0];
    return row ? this.mapRowToJob(row) : null;
  }

  public async claimJob(queueName: string): Promise<JobRow<TPayload> | null> {
    const result = await this.pool.query<JobRow<TPayload>>(
      `WITH job AS (
        SELECT id
        FROM ${this.quotedTable}
        WHERE status = 'queued'
          AND queue_name = $1
          AND available_at <= NOW()
        ORDER BY available_at ASC, created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      UPDATE ${this.quotedTable} j
      SET status = 'processing', attempts = j.attempts + 1, started_at = NOW(), updated_at = NOW()
      FROM job
      WHERE j.id = job.id
      RETURNING j.*`,
      [queueName],
    );
    return result.rows[0] ?? null;
  }

  public async completeJob(id: string): Promise<void> {
    await this.pool.query(
      `UPDATE ${this.quotedTable}
       SET status = 'completed', finished_at = NOW(), updated_at = NOW(), last_error = NULL
       WHERE id = $1`,
      [id],
    );
  }

  public async scheduleRetry(
    id: string,
    delayMs: number,
    lastError: string,
  ): Promise<void> {
    await this.pool.query(
      `UPDATE ${this.quotedTable}
       SET status = 'queued', available_at = NOW() + ($2 * INTERVAL '1 millisecond'),
           updated_at = NOW(), last_error = $3
       WHERE id = $1`,
      [id, delayMs, lastError],
    );
  }

  public async failJob(id: string, lastError: string): Promise<void> {
    await this.pool.query(
      `UPDATE ${this.quotedTable}
       SET status = 'failed', finished_at = NOW(), updated_at = NOW(), last_error = $2
       WHERE id = $1`,
      [id, lastError],
    );
  }

  public async getMetrics(queueName: string): Promise<QueueMetrics> {
    const statusCounts: Record<JobStatus, number> = {
      queued: 0,
      processing: 0,
      completed: 0,
      failed: 0,
    };

    const counts = await this.pool.query<{ status: JobStatus; count: string }>(
      `SELECT status, COUNT(*)::text as count
       FROM ${this.quotedTable}
       WHERE queue_name = $1
       GROUP BY status`,
      [queueName],
    );

    for (const row of counts.rows) {
      statusCounts[row.status] = Number.parseInt(row.count, 10);
    }

    const oldest = await this.pool.query<{ oldest: Date }>(
      `SELECT MIN(available_at) as oldest
       FROM ${this.quotedTable}
       WHERE queue_name = $1 AND status = 'queued'`,
      [queueName],
    );

    return {
      queueName,
      statusCounts,
      oldestQueuedAt: oldest.rows[0]?.oldest ?? null,
    };
  }

  public mapRowToJob(row: JobRow<TPayload>): Job<TPayload> {
    return {
      id: row.id,
      queue: row.queue_name,
      payload: row.payload,
      status: row.status,
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      availableAt: row.available_at,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      startedAt: row.started_at,
      finishedAt: row.finished_at,
      lastError: row.last_error,
    };
  }
}

export type { JobRow };
