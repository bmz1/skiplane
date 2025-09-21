export type JobStatus = "queued" | "processing" | "completed" | "failed";

export interface Job<TPayload> {
  id: string;
  queue: string;
  payload: TPayload;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  availableAt: Date;
  createdAt: Date;
  updatedAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
  lastError: string | null;
}

export type JobHandler<TPayload> = (job: Job<TPayload>) => Promise<void>;

export interface QueueMetrics {
  queueName: string;
  statusCounts: Record<JobStatus, number>;
  oldestQueuedAt: Date | null;
}

export interface QueueEvents<TPayload> {
  jobReserved: { job: Job<TPayload> };
  jobStarted: { job: Job<TPayload> };
  jobCompleted: { job: Job<TPayload>; durationMs: number };
  jobFailed: { job: Job<TPayload>; error: unknown; willRetry: boolean };
  idle: { durationMs: number };
  error: { error: unknown };
}

export type QueueEventName<TPayload> = keyof QueueEvents<TPayload>;

export type QueueEventListener<TPayload, K extends QueueEventName<TPayload>> = (
  payload: QueueEvents<TPayload>[K],
) => void;
