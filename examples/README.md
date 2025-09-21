# Examples

The examples in this directory show how to stitch the job queue together with
PostgreSQL and run end-to-end scenarios. Each example assumes that a PostgreSQL
instance is reachable via the `POSTGRES_URL` (or `DATABASE_URL`) environment
variable. The provided `docker-compose.yml` starts a local database with the
expected credentials.

## Prerequisites

```bash
# from the project root
npm install
npm run build
```

## Running the examples

```bash
# 1. Start PostgreSQL (optional if you already have a database available)
docker compose up -d db

# 2. Run an example (see the subsections below)
POSTGRES_URL="postgres://postgres:postgres@localhost:5432/skiplane" npm run example:basic

# 3. Stop the database when finished
docker compose down
```

### Available examples

- `basic`: demonstrates enqueuing a job, processing it with a worker, and
  reacting to observability events.

Each example directory contains its own `README.md` with additional context.
