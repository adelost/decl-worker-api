/**
 * Local backend client - bridges DWA to a local Python FastAPI worker.
 *
 * Unlike Modal/Ray (fire-and-forget HTTP), the local backend creates jobs
 * on the Python worker, polls for completion, and returns results.
 *
 * Two execution modes:
 * - Worker endpoints (POST /api/worker/{name}): Direct sync response
 * - Job endpoints (POST /api/jobs): Async polling until complete
 */

import type {
  Backend,
  Task,
  TaskResult,
  ResourcePool,
} from "@dwa/core";

export interface LocalConfig {
  /** Python worker base URL, e.g. "http://localhost:8080" */
  url: string;
  /** Poll interval in ms (default: 500) */
  pollInterval?: number;
  /** Default timeout in seconds (default: 600) */
  timeout?: number;
}

/** Python worker job status values */
type WorkerJobStatus = "pending" | "running" | "complete" | "failed" | "cancelled";

interface WorkerJob {
  id: string;
  type: string;
  status: WorkerJobStatus;
  progress: number;
  message: string;
  error: string | null;
  result?: unknown;
}

interface WorkerPollResponse {
  job: WorkerJob;
  events: Array<{ seq: number; type: string; [key: string]: unknown }>;
  event_seq: number;
}

export class LocalBackend implements Backend {
  name = "local";
  private url: string;
  private pollInterval: number;
  private timeout: number;

  constructor(config: LocalConfig) {
    this.url = config.url.replace(/\/+$/, ""); // strip trailing slashes
    this.pollInterval = config.pollInterval ?? 500;
    this.timeout = (config.timeout ?? 600) * 1000;
  }

  /**
   * Execute a task on the Python worker.
   *
   * Routes by task.type prefix:
   * - "worker.*" → POST /api/worker/{name} (sync, direct result)
   * - everything else → POST /api/jobs (async, poll until complete)
   */
  async execute(task: Task): Promise<unknown> {
    if (task.type.startsWith("worker.")) {
      return this.executeWorker(task);
    }
    return this.executeJob(task);
  }

  /**
   * Direct worker execution - calls POST /api/worker/{name} which returns
   * the result synchronously (the Python endpoint blocks until done).
   */
  private async executeWorker(task: Task): Promise<unknown> {
    const workerName = task.type.replace("worker.", "");
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const res = await fetch(`${this.url}/api/worker/${workerName}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(task.payload),
        signal: controller.signal,
      });

      if (!res.ok) {
        const text = await res.text();
        throw new Error(`Worker ${workerName} failed: ${res.status} - ${text}`);
      }

      return res.json();
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Job-based execution - creates a job on the Python worker, then polls
   * /api/jobs/{id}/poll until complete or failed.
   */
  private async executeJob(task: Task): Promise<unknown> {
    // Create job on Python worker
    const createRes = await fetch(`${this.url}/api/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type: task.type,
        source: task.payload.source ?? task.payload.filePath ?? "",
        params: task.payload,
      }),
    });

    if (!createRes.ok) {
      const text = await createRes.text();
      throw new Error(`Failed to create job: ${createRes.status} - ${text}`);
    }

    const { job_id } = (await createRes.json()) as { job_id: string };

    // Poll until complete
    return this.pollUntilDone(job_id);
  }

  /**
   * Poll Python worker's /api/jobs/{id}/poll endpoint until the job
   * reaches a terminal state (complete, failed, cancelled).
   */
  private async pollUntilDone(jobId: string): Promise<unknown> {
    const deadline = Date.now() + this.timeout;
    let sinceSeq = 0;

    while (Date.now() < deadline) {
      const res = await fetch(
        `${this.url}/api/jobs/${jobId}/poll?since_seq=${sinceSeq}`
      );

      if (!res.ok) {
        throw new Error(`Poll failed: ${res.status}`);
      }

      const data = (await res.json()) as WorkerPollResponse;
      sinceSeq = data.event_seq;

      const { job } = data;

      if (job.status === "complete") {
        // Fetch full result if available
        if (job.result !== undefined) {
          return job.result;
        }
        const resultRes = await fetch(`${this.url}/api/jobs/${jobId}/result`);
        if (resultRes.ok) {
          return resultRes.json();
        }
        return { status: "complete" };
      }

      if (job.status === "failed") {
        throw new Error(job.error ?? "Job failed without error message");
      }

      if (job.status === "cancelled") {
        throw new Error("Job was cancelled");
      }

      // Wait before next poll
      await sleep(this.pollInterval);
    }

    throw new Error(`Job ${jobId} timed out after ${this.timeout / 1000}s`);
  }

  async getStatus(taskId: string): Promise<TaskResult> {
    const res = await fetch(`${this.url}/api/jobs/${taskId}`);

    if (!res.ok) {
      throw new Error(`Failed to get status: ${res.status}`);
    }

    const job = (await res.json()) as WorkerJob;

    return {
      id: job.id,
      status: mapWorkerStatus(job.status),
      progress: Math.round(job.progress * 100), // Python uses 0-1, DWA uses 0-100
      error: job.error ?? undefined,
      result: job.result,
    };
  }

  async cancel(taskId: string): Promise<boolean> {
    const res = await fetch(`${this.url}/api/jobs/${taskId}`, {
      method: "DELETE",
    });
    return res.ok;
  }

  async isHealthy(): Promise<boolean> {
    try {
      const res = await fetch(`${this.url}/health`, {
        signal: AbortSignal.timeout(5000),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  async getResources(): Promise<ResourcePool> {
    try {
      const res = await fetch(`${this.url}/api/backends/status`, {
        signal: AbortSignal.timeout(5000),
      });

      if (!res.ok) {
        return emptyResources();
      }

      const data = (await res.json()) as Record<string, unknown>;

      // Python worker reports GPU info via backends/status
      // For now, report a single local GPU as available
      return {
        gpus: [{ name: "local", vram: 11000, available: true }],
        ram: { total: 0, available: 0 },
        vram: {
          total: (data.vram_total as number) ?? 0,
          available: (data.vram_free as number) ?? 0,
        },
      };
    } catch {
      return emptyResources();
    }
  }
}

function mapWorkerStatus(status: WorkerJobStatus): TaskResult["status"] {
  switch (status) {
    case "complete":
      return "completed";
    case "failed":
      return "failed";
    case "cancelled":
      return "cancelled";
    case "running":
      return "running";
    case "pending":
    default:
      return "pending";
  }
}

function emptyResources(): ResourcePool {
  return {
    gpus: [],
    ram: { total: 0, available: 0 },
    vram: { total: 0, available: 0 },
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
