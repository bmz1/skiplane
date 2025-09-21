import type { EventEmitter } from "node:events";
import { performance } from "node:perf_hooks";

import type { JobHandler, Job } from "../types.js";
import type { PgQueueStore } from "./pg-store.js";

interface WorkerPoolOptions {
  pollIntervalMs: number;
  concurrency: number;
  queueName: string;
  ensureReady: () => Promise<void>;
  retryBackoff: (attempt: number) => number;
  serializeError: (error: unknown) => string;
  events: EventEmitter;
}

export interface WorkerController {
  stop: () => Promise<void>;
}

export class WorkerPool<TPayload> {
  private workerState?: {
    abortController: AbortController;
    promises: Promise<void>[];
  };

  public constructor(
    private readonly store: PgQueueStore<TPayload>,
    private readonly options: WorkerPoolOptions,
  ) {}

  public start(handler: JobHandler<TPayload>): WorkerController {
    if (this.workerState) {
      throw new Error(
        "Workers already running. Call stop() before starting again.",
      );
    }

    const abortController = new AbortController();
    const promises: Promise<void>[] = [];

    this.workerState = {
      abortController,
      promises,
    };

    for (let i = 0; i < this.options.concurrency; i += 1) {
      promises.push(this.runWorker(handler, abortController.signal));
    }

    const stop = async () => {
      await this.stop();
    };

    return { stop };
  }

  public async stop(): Promise<void> {
    if (!this.workerState) {
      return;
    }

    this.workerState.abortController.abort();
    await Promise.allSettled(this.workerState.promises);
    this.workerState = undefined;
  }

  private async runWorker(
    handler: JobHandler<TPayload>,
    signal: AbortSignal,
  ): Promise<void> {
    try {
      await this.options.ensureReady();
    } catch (error) {
      this.options.events.emit("error", { error });
      return;
    }

    while (!signal.aborted) {
      try {
        const jobRow = await this.store.claimJob(this.options.queueName);
        if (!jobRow) {
          this.options.events.emit("idle", {
            durationMs: this.options.pollIntervalMs,
          });
          await this.delay(this.options.pollIntervalMs, signal);
          continue;
        }

        const job = this.store.mapRowToJob(jobRow);
        this.options.events.emit("jobReserved", { job });
        this.options.events.emit("jobStarted", { job });

        const startedAt = performance.now();

        try {
          await handler(job);
          await this.store.completeJob(jobRow.id);
          const durationMs = performance.now() - startedAt;
          const completedJob: Job<TPayload> = {
            ...job,
            status: "completed",
            finishedAt: new Date(),
            lastError: null,
          };
          this.options.events.emit("jobCompleted", {
            job: completedJob,
            durationMs,
          });
        } catch (error) {
          const attempts = jobRow.attempts;
          const serializedError = this.options.serializeError(error);
          const willRetry = attempts < jobRow.max_attempts;

          if (willRetry) {
            const delayMs = Math.max(0, this.options.retryBackoff(attempts));
            await this.store.scheduleRetry(jobRow.id, delayMs, serializedError);
          } else {
            await this.store.failJob(jobRow.id, serializedError);
          }

          const failedJob: Job<TPayload> = {
            ...job,
            status: willRetry ? "queued" : "failed",
            lastError: serializedError,
          };
          this.options.events.emit("jobFailed", {
            job: failedJob,
            error,
            willRetry,
          });
        }
      } catch (error) {
        this.options.events.emit("error", { error });
        await this.delay(this.options.pollIntervalMs, signal);
      }
    }
  }

  private async delay(ms: number, signal: AbortSignal): Promise<void> {
    if (ms <= 0) {
      return;
    }

    await new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        cleanup();
        resolve();
      }, ms);

      const cleanup = () => {
        clearTimeout(timeout);
        signal.removeEventListener("abort", abortHandler);
      };

      const abortHandler = () => {
        cleanup();
        resolve();
      };

      signal.addEventListener("abort", abortHandler, { once: true });
    });
  }
}
