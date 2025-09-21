import { Pool } from "pg";
import { beforeAll, afterAll } from "vitest";

declare global {
  // eslint-disable-next-line no-var
  var __TEST_POOL__: Pool | undefined;
}

export const testConnectionString =
  process.env.POSTGRES_URL ??
  process.env.DATABASE_URL ??
  "postgres://postgres:postgres@localhost:5432/skiplane";

process.env.POSTGRES_URL = testConnectionString;

const pool = new Pool({ connectionString: testConnectionString });

globalThis.__TEST_POOL__ = pool;

beforeAll(async () => {
  await pool.query("SELECT 1");
});

afterAll(async () => {
  await pool.end();
  globalThis.__TEST_POOL__ = undefined;
});

export const testPool = pool;
