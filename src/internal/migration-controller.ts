import type { Pool } from "pg";

import { quoteIdentifier } from "./sql.js";
import type { PgQueueStore } from "./pg-store.js";

interface Migration {
  id: number;
  name: string;
  statements: string[];
}

interface MigrationControllerOptions {
  migrationsTableName: string;
  defaultMaxAttempts: number;
}

export class MigrationController {
  private readonly quotedMigrationsTable: string;
  private migrationsPromise?: Promise<void>;
  private migrationsComplete = false;

  public constructor(
    private readonly pool: Pool,
    private readonly store: PgQueueStore<unknown>,
    private readonly options: MigrationControllerOptions,
  ) {
    this.quotedMigrationsTable = quoteIdentifier(options.migrationsTableName);
  }

  public isComplete(): boolean {
    return this.migrationsComplete;
  }

  public async ensure({
    allowAuto,
    force,
  }: {
    allowAuto: boolean;
    force?: boolean;
  }): Promise<void> {
    if (this.migrationsComplete && !force) {
      return;
    }

    if (this.migrationsPromise) {
      await this.migrationsPromise;
      return;
    }

    if (!allowAuto && !force) {
      throw new Error(
        "Migrations have not been run. Call runMigrations() before using the queue or enable autoMigrate.",
      );
    }

    this.migrationsPromise = this.applyMigrations()
      .then(() => {
        this.migrationsComplete = true;
      })
      .finally(() => {
        this.migrationsPromise = undefined;
      });

    await this.migrationsPromise;
  }

  private async applyMigrations(): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query(
        `CREATE TABLE IF NOT EXISTS ${this.quotedMigrationsTable} (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )`,
      );

      for (const migration of this.buildMigrations()) {
        await client.query("BEGIN");
        try {
          await client.query(
            `LOCK TABLE ${this.quotedMigrationsTable} IN EXCLUSIVE MODE`,
          );

          const applied = await client.query(
            `SELECT 1 FROM ${this.quotedMigrationsTable} WHERE id = $1`,
            [migration.id],
          );

          /* c8 ignore next */
          if ((applied.rowCount ?? 0) > 0) {
            await client.query("ROLLBACK");
            continue;
          }

          for (const statement of migration.statements) {
            await client.query(statement);
          }

          await client.query(
            `INSERT INTO ${this.quotedMigrationsTable} (id, name) VALUES ($1, $2)`,
            [migration.id, migration.name],
          );

          await client.query("COMMIT");
        } catch (error) {
          await client.query("ROLLBACK");
          throw error;
        }
      }
    } finally {
      client.release();
    }
  }

  private buildMigrations(): Migration[] {
    return [
      {
        id: 1,
        name: "create_jobs_table",
        statements: [
          `CREATE TABLE IF NOT EXISTS ${this.store.quotedTableName} (
            id UUID PRIMARY KEY,
            queue_name TEXT NOT NULL,
            payload JSONB NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT ${this.options.defaultMaxAttempts},
            available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            last_error TEXT
          )`,
          `CREATE INDEX IF NOT EXISTS ${this.store.quotedJobsIndexName}
            ON ${this.store.quotedTableName} (queue_name, status, available_at)`,
        ],
      },
    ];
  }
}
