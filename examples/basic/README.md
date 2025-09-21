# Basic example

This example is intentionally small: it bootstraps a `JobQueue`, starts a
worker, enqueues a single job, and waits for the job to finish. It mirrors the
library defaults so you can see the happy path without configuration noise.

## Run

```bash
# from the project root
docker compose up -d db
POSTGRES_URL="postgres://postgres:postgres@localhost:5432/skiplane" npm run example:basic
```

You will see:

- Worker life-cycle events as the job transitions through reservation,
  execution, and completion.
- Structured logging that links the worker’s work to the job payload.

Stop the worker with `Ctrl+C` once the script finishes and bring the database
back down with:

```bash
docker compose down
```
