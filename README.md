# Skiplane

Skiplane is a lightweight, production-grade job queue for Node.js that is backed
by PostgreSQL and relies on `FOR UPDATE SKIP LOCKED` to coordinate work safely
across many workers.

## Why Skiplane?

- **PostgreSQL-first**: no extra services to operate—just point at an existing
  database.
- **Deterministic concurrency**: workers reserve jobs with `SKIP LOCKED`, so
  throughput scales linearly without double-processing.
- **Predictable retries**: configure attempt caps and exponential backoff
  (customizable per queue) without sprinkling retry logic throughout handlers.
- **Observability hooks**: subscribe to lifecycle events (`jobReserved`,
  `jobCompleted`, `jobFailed`, etc.) to drive metrics, logging, and alerts.
- **Auto or manual migrations**: ship with zero-downtime migrations that can run
  automatically or be controlled explicitly in strict environments.
- **Modern toolchain**: TypeScript, ESM-only.

## Installation

```bash
npm install skiplane
```

Skiplane targets Node.js `>= 18.17` and compiles to pure ESM. If you are using
CommonJS you will need to load it via dynamic `import()`.

## Quick start

```ts
import { JobQueue } from "skiplane";

const queue = new JobQueue({
  connectionString: process.env.POSTGRES_URL,
  queueName: "email",
  defaultMaxAttempts: 5,
  retryBackoff: (attempt) => Math.min(30_000, 2 ** (attempt - 1) * 500)
});

await queue.runMigrations(); // optional if already run

const worker = queue.start(async (job) => {
  // Do the work.
  console.log("sending email", job.payload);
});

await queue.enqueue({ to: "friend@example.com", subject: "Hey" });
```

See `examples/README.md` for a runnable script (`npm run example:basic`) that
wires everything together, including event listeners and metrics.

## Configuration

### `JobQueueOptions`

| Option               | Type                | Default             | Notes |
| -------------------- | ------------------- | ------------------- | ----- |
| `connectionString`   | `string`            | env `POSTGRES_URL`  | Optional if `pool` provided. |
| `pool`               | `pg.Pool`           | `undefined`         | Supply your own pool to reuse connections. |
| `tableName`          | `string`            | `skiplane_jobs`     | Must be a valid SQL identifier. |
| `queueName`          | `string`            | `default`           | Logical queue label; can be overridden per job. |
| `pollIntervalMs`     | `number`            | `200`               | Worker idle wait when no jobs are ready. |
| `defaultMaxAttempts` | `number`            | `5`                 | Floor of `1`. |
| `retryBackoff`       | `(attempt) => ms`   | exponential (1s cap) | Attempt starts at `1`. Return delay in milliseconds. |
| `concurrency`        | `number`            | `1`                 | Global default worker count. |
| `autoMigrate`        | `boolean`           | `true`              | Disable to require `runMigrations()` manually. |
| `migrationsTableName`| `string`            | `${tableName}_migrations` | Records applied migrations. |

### Enqueue options

```ts
await queue.enqueue(payload, {
  id: "optional-uuid",     // default random UUID
  queueName: "high-priority",
  runAt: new Date(Date.now() + 30_000),
  maxAttempts: 10
});
```

Jobs scheduled in the future stay hidden until `runAt` is reached.

### Worker control

```ts
const controller = queue.start(handler, {
  concurrency: 5,
  pollIntervalMs: 100,
  queueName: "email-high",
  signal: abortController.signal
});

await controller.stop(); // Graceful shutdown
```

All workers share a single `WorkerPool` per queue instance. Stopping waits for
in-flight jobs to finish before returning.

## Observability

Skiplane emits events via Node's `EventEmitter`. Subscribe with `queue.on()`.

| Event           | Payload                                               | Purpose |
| --------------- | ------------------------------------------------------ | ------- |
| `jobReserved`   | `{ job }`                                              | Inspect reservation latency. |
| `jobStarted`    | `{ job }`                                              | Start of handler execution. |
| `jobCompleted`  | `{ job, durationMs }`                                  | Measure run time and throughput. |
| `jobFailed`     | `{ job, error, willRetry }`                            | Alert on failures or retries. |
| `idle`          | `{ durationMs }`                                       | Detect empty queues or adjust polling. |
| `error`         | `{ error }`                                            | Surface unexpected worker errors. |

Metrics are available via `queue.getMetrics(queueName?)`, returning counts by
status and the oldest queued job timestamp.

## Migrations

- **Automatic**: default behavior. The first queue interaction runs migrations
  (schema creation, index management) guarded by advisory locks.
- **Manual**: set `autoMigrate: false` and invoke `await queue.runMigrations()`
  during deploys or inside your migration tooling. All other operations will
  throw until migrations run.

The migration controller records applied steps in
`<tableName>_migrations`, making each deployment idempotent.

## Architecture (deep modules)

- **`JobQueue` (facade)** — the only exported class. It coordinates the store,
  migration controller, and worker pool while exposing a narrow public API.
- **`PgQueueStore`** — encapsulates all SQL. The rest of the system never
  constructs raw queries, keeping database knowledge isolated.
- **`MigrationController`** — responsible for schema evolution, locking, and
  bookkeeping so callers treat migrations as a single action.
- **`WorkerPool`** — manages concurrency, retries, and event emission; the queue
  only observes its start/stop hook.
- **`types.ts`** — shared contracts (jobs, handlers, events) so modules stay in
  sync without tight coupling.

This structure follows Ousterhout’s principle of deep modules: complex internals
(`for update` queries, transactions, delay logic) stay hidden, while users only
handle a minimal surface (`enqueue`, `start`, `stop`, `getJob`).

## Development

1. Install dependencies
   ```bash
   npm install
   ```
2. Run the TypeScript build
   ```bash
   npm run build
   ```
3. Lint the source and tests
   ```bash
   npm run lint
   ```
4. Execute the Vitest suite (requires Postgres)
   ```bash
   docker compose up -d db
   npm test
   docker compose down
   ```
5. Try the example app
   ```bash
   docker compose up -d db
   POSTGRES_URL="postgres://postgres:postgres@localhost:5432/skiplane" npm run example:basic
   docker compose down
   ```

## Testing philosophy

Tests live in `tests/` and connect to a real database to cover migrations,
retries, and error flows. Helpers generate per-test tables to avoid cross-test
interference. The integration focus keeps modules deep—the testers do not peek
into internals and instead exercise public behavior.

## Contributing

Pull requests are welcome. Please:

- Run `npm run lint`, `npm run build`, and `npm test` before submitting.
- Add or update tests for new behavior.
- Keep modules deep and interfaces minimal in line with the design philosophy.

## License

MIT © 2024-present. See `LICENSE`.
