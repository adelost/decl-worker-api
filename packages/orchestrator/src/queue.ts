/**
 * BullMQ queue setup for task management.
 */

import { Queue, Worker, Job, type JobsOptions } from "bullmq";
import { Redis } from "ioredis";
import type { Task, TaskResult } from "@dwa/core";
import { processTask } from "./engine/dispatcher.js";
import { runEffects } from "./engine/effects.js";

// Lazy initialization for Redis and queues
let _connection: Redis | null = null;
let _queues: Record<string, Queue> | null = null;

export function getConnection(): Redis {
  if (!_connection) {
    _connection = new Redis(process.env.REDIS_URL || "redis://localhost:6379", {
      maxRetriesPerRequest: null,
    });
  }
  return _connection;
}

export function getQueues(): Record<string, Queue> {
  if (!_queues) {
    const connection = getConnection();
    _queues = {
      default: new Queue("declarative-worker-api-default", { connection }),
      gpu: new Queue("declarative-worker-api-gpu", { connection }),
      cpu: new Queue("declarative-worker-api-cpu", { connection }),
    };
  }
  return _queues;
}

// Legacy export for backwards compatibility
export const queues = new Proxy({} as Record<string, Queue>, {
  get(_, prop: string) {
    return getQueues()[prop];
  },
});

/**
 * Add a task to the appropriate queue.
 */
export async function enqueueTask(task: Task): Promise<string> {
  const allQueues = getQueues();
  const queueName = task.queue || "default";
  const queue = allQueues[queueName] || allQueues.default;

  const jobOptions: JobsOptions = {
    priority: task.priority,
    attempts: task.retry?.attempts || 3,
    backoff: task.retry?.backoff === "exponential"
      ? { type: "exponential", delay: task.retry.delay || 1000 }
      : { type: "fixed", delay: task.retry?.delay || 1000 },
  };

  if (task.delay) {
    jobOptions.delay = task.delay;
  }

  if (task.cron) {
    jobOptions.repeat = { pattern: task.cron };
  }

  const job = await queue.add(task.type, task, jobOptions);

  // Run onPending effects
  if (task.onPending?.length) {
    await runEffects(task.onPending, { task, jobId: job.id });
  }

  return job.id!;
}

/**
 * Get task status by job ID.
 */
export async function getTaskStatus(
  jobId: string,
  queueName: string = "default"
): Promise<TaskResult | null> {
  const allQueues = getQueues();
  const queue = allQueues[queueName] || allQueues.default;
  const job = await queue.getJob(jobId);

  if (!job) return null;

  const state = await job.getState();

  return {
    id: job.id!,
    status: mapState(state),
    result: job.returnvalue,
    error: job.failedReason,
    progress: job.progress as number | undefined,
    startedAt: job.processedOn ? new Date(job.processedOn) : undefined,
    completedAt: job.finishedOn ? new Date(job.finishedOn) : undefined,
  };
}

function mapState(state: string): TaskResult["status"] {
  switch (state) {
    case "completed":
      return "completed";
    case "failed":
    case "stuck":
      return "failed";
    case "active":
      return "running";
    case "paused":
    default:
      return "pending";
  }
}

/**
 * List tasks from a queue with optional filtering.
 */
export async function listTasks(
  queueName: string = "default",
  status?: string,
  limit: number = 50
): Promise<TaskResult[]> {
  const allQueues = getQueues();
  const queue = allQueues[queueName] || allQueues.default;

  // Get jobs by state
  type JobState = "completed" | "failed" | "active" | "waiting" | "delayed";
  const states: JobState[] = status
    ? [status as JobState]
    : ["completed", "failed", "active", "waiting", "delayed"];

  const jobs = await queue.getJobs(states, 0, limit);

  return Promise.all(
    jobs.map(async (job) => {
      const state = await job.getState();
      return {
        id: job.id!,
        status: mapState(state),
        result: job.returnvalue,
        error: job.failedReason,
        progress: job.progress as number | undefined,
        startedAt: job.processedOn ? new Date(job.processedOn) : undefined,
        completedAt: job.finishedOn ? new Date(job.finishedOn) : undefined,
        // Include task type for easier identification
        type: job.data?.type,
        queue: queueName,
      };
    })
  );
}

/**
 * Create workers for processing tasks.
 */
export function createWorkers() {
  const connection = getConnection();
  const workerOptions = {
    connection,
    concurrency: parseInt(process.env.WORKER_CONCURRENCY || "5"),
  };

  const processor = async (job: Job<Task>) => {
    const task = job.data;

    try {
      // Report progress
      await job.updateProgress(0);

      // Process the task
      const result = await processTask(task, (progress) => {
        job.updateProgress(progress);
        if (task.onProgress?.length) {
          runEffects(task.onProgress, { task, progress, jobId: job.id });
        }
      });

      await job.updateProgress(100);

      // Run onSuccess effects
      if (task.onSuccess?.length) {
        await runEffects(task.onSuccess, { task, result, jobId: job.id });
      }

      return result;
    } catch (error) {
      // Run onError effects
      if (task.onError?.length) {
        await runEffects(task.onError, {
          task,
          error: error instanceof Error ? error.message : String(error),
          jobId: job.id,
        });
      }
      throw error;
    }
  };

  return {
    default: new Worker("declarative-worker-api-default", processor, workerOptions),
    gpu: new Worker("declarative-worker-api-gpu", processor, {
      ...workerOptions,
      concurrency: parseInt(process.env.GPU_WORKER_CONCURRENCY || "2"),
    }),
    cpu: new Worker("declarative-worker-api-cpu", processor, workerOptions),
  };
}

/**
 * Graceful shutdown.
 */
export async function shutdown() {
  if (_queues) {
    await Promise.all([
      _queues.default.close(),
      _queues.gpu.close(),
      _queues.cpu.close(),
    ]);
    _queues = null;
  }
  if (_connection) {
    await _connection.quit();
    _connection = null;
  }
}
