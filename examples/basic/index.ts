import { setTimeout as sleep } from "node:timers/promises";

import { JobQueue, type Job } from "../../src/index.js";

type ExamplePayload = {
  greeting: string;
  attempt?: number;
};

const connectionString =
  process.env.POSTGRES_URL ??
  process.env.DATABASE_URL ??
  "postgres://postgres:postgres@localhost:5432/skiplane";

const queue = new JobQueue<ExamplePayload>({
  connectionString,
  queueName: "example_basic",
  pollIntervalMs: 50,
  retryBackoff: (attempt) => Math.min(1000, 2 ** (attempt - 1) * 100)
});

queue.on("jobReserved", ({ job }) => {
  console.log(`[queue] reserved job ${job.id}`);
});

queue.on("jobStarted", ({ job }) => {
  console.log(`[queue] started job ${job.id}`);
});

queue.on("jobCompleted", ({ job, durationMs }) => {
  console.log(`[queue] completed job ${job.id} in ${durationMs.toFixed(1)}ms`);
});

queue.on("jobFailed", ({ job, error, willRetry }) => {
  console.warn(
    `[queue] job ${job.id} failed${willRetry ? " (retrying)" : ""}:`,
    error
  );
});

queue.on("error", ({ error }) => {
  console.error(`[queue] worker error:`, error);
});

async function waitForFinalState(jobId: string): Promise<Job<ExamplePayload>> {
  return new Promise<Job<ExamplePayload>>((resolve, reject) => {
    const onCompleted = ({ job }: { job: Job<ExamplePayload> }) => {
      if (job.id !== jobId) {
        return;
      }
      cleanup();
      resolve(job);
    };

    const onFailed = ({
      job,
      error,
      willRetry
    }: {
      job: Job<ExamplePayload>;
      error: unknown;
      willRetry: boolean;
    }) => {
      if (job.id !== jobId) {
        return;
      }
      if (willRetry) {
        return;
      }
      cleanup();
      reject(error instanceof Error ? error : new Error(String(error)));
    };

    const onError = ({ error }: { error: unknown }) => {
      cleanup();
      reject(error instanceof Error ? error : new Error(String(error)));
    };

    function cleanup(): void {
      queue.off("jobCompleted", onCompleted);
      queue.off("jobFailed", onFailed);
      queue.off("error", onError);
    }

    queue.on("jobCompleted", onCompleted);
    queue.on("jobFailed", onFailed);
    queue.on("error", onError);
  });
}

async function main(): Promise<void> {
  console.log(`[main] using database ${connectionString}`);
  await queue.runMigrations();

  const worker = queue.start(async (job) => {
    console.log(`[worker] processing ${job.id}`, job.payload);
    await sleep(250);
  });

  try {
    const jobId = await queue.enqueue({ greeting: "Hello from Skiplane!" });
    console.log(`[main] enqueued job ${jobId}`);

    const completed = await waitForFinalState(jobId);
    console.log(`[main] job ${completed.id} finished with status ${completed.status}`);

    const metrics = await queue.getMetrics();
    console.log(`[main] queue metrics`, metrics.statusCounts);
  } finally {
    await worker.stop();
    await queue.destroy();
  }
}

await main().catch((error) => {
  console.error(`[main] example failed:`, error);
  process.exitCode = 1;
});
