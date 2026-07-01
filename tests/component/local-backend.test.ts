/**
 * Tests for the LocalBackend that bridges DWA to Python FastAPI worker.
 * Uses Node's http.createServer to simulate the Python worker.
 */

import { createServer, type IncomingMessage, type ServerResponse, type Server } from "http";
import { afterAll, beforeAll } from "vitest";
import { feature, rule, component, expect } from "bdd-vitest";
import { LocalBackend } from "../../packages/orchestrator/src/backends/local.js";
import type { Task } from "@dwa/core";

// --- Mock Python Worker ---

interface MockWorkerConfig {
  /** Number of polls before job completes */
  pollsToComplete?: number;
  /** Result to return on completion */
  result?: unknown;
  /** Error message (makes job fail instead of complete) */
  error?: string;
  /** Whether /api/health returns OK */
  healthy?: boolean;
  /** Worker results keyed by worker name */
  workerResults?: Record<string, unknown>;
}

function createMockWorker(config: MockWorkerConfig = {}) {
  const {
    pollsToComplete = 2,
    result = { status: "done" },
    error,
    healthy = true,
    workerResults = {},
  } = config;

  let pollCount = 0;
  const jobId = "job_mock_1";

  const handler = (req: IncomingMessage, res: ServerResponse) => {
    const url = new URL(req.url!, `http://${req.headers.host}`);
    const path = url.pathname;
    const method = req.method;

    res.setHeader("Content-Type", "application/json");

    // Health check
    if (path === "/health" && method === "GET") {
      if (healthy) {
        res.end(JSON.stringify({ status: "ok" }));
      } else {
        res.writeHead(503);
        res.end(JSON.stringify({ error: "unhealthy" }));
      }
      return;
    }

    // Create job
    if (path === "/api/jobs" && method === "POST") {
      pollCount = 0;
      res.end(JSON.stringify({ job_id: jobId, status: "pending" }));
      return;
    }

    // Poll job
    if (path === `/api/jobs/${jobId}/poll` && method === "GET") {
      pollCount++;

      if (error && pollCount >= pollsToComplete) {
        res.end(JSON.stringify({
          job: { id: jobId, status: "failed", progress: 0, error, message: "", result: null },
          events: [],
          event_seq: pollCount,
        }));
        return;
      }

      if (pollCount >= pollsToComplete) {
        res.end(JSON.stringify({
          job: { id: jobId, status: "complete", progress: 1.0, message: "", error: null, result },
          events: [],
          event_seq: pollCount,
        }));
        return;
      }

      res.end(JSON.stringify({
        job: { id: jobId, status: "running", progress: pollCount / pollsToComplete, message: `Step ${pollCount}`, error: null },
        events: [{ seq: pollCount, type: "progress" }],
        event_seq: pollCount,
      }));
      return;
    }

    // Job result
    if (path === `/api/jobs/${jobId}/result` && method === "GET") {
      res.end(JSON.stringify(result));
      return;
    }

    // Job status
    if (path === `/api/jobs/${jobId}` && method === "GET") {
      res.end(JSON.stringify({
        id: jobId,
        type: "analyze_full",
        status: pollCount >= pollsToComplete ? "complete" : "running",
        progress: Math.min(pollCount / pollsToComplete, 1),
        message: "",
        error: null,
      }));
      return;
    }

    // Cancel job
    if (path === `/api/jobs/${jobId}` && method === "DELETE") {
      res.end(JSON.stringify({ cancelled: true }));
      return;
    }

    // Worker endpoints
    const workerMatch = path.match(/^\/api\/worker\/(.+)$/);
    if (workerMatch && method === "POST") {
      const workerName = workerMatch[1];
      const workerResult = workerResults[workerName];
      if (workerResult) {
        res.end(JSON.stringify(workerResult));
      } else {
        res.writeHead(404);
        res.end(JSON.stringify({ error: `Unknown worker: ${workerName}` }));
      }
      return;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: "Not found" }));
  };

  return { handler, getJobId: () => jobId };
}

// --- Server lifecycle ---

let mockServer: Server;
let serverPort: number;
let serverUrl: string;

function startServer(config: MockWorkerConfig = {}): Promise<void> {
  return new Promise((resolve) => {
    const { handler } = createMockWorker(config);
    mockServer = createServer(handler);
    mockServer.listen(0, () => {
      const addr = mockServer.address();
      if (typeof addr === "object" && addr) {
        serverPort = addr.port;
        serverUrl = `http://localhost:${serverPort}`;
      }
      resolve();
    });
  });
}

function stopServer(): Promise<void> {
  return new Promise((resolve) => {
    if (mockServer) {
      mockServer.close(() => resolve());
    } else {
      resolve();
    }
  });
}

// --- Tests ---

feature("LocalBackend", () => {
  rule("Health Checks", () => {
    component("reports healthy when Python worker responds OK", {
      given: ["a healthy Python worker", async () => {
        await startServer({ healthy: true });
        return new LocalBackend({ url: serverUrl });
      }],
      when: ["checking health", (backend) => backend.isHealthy()],
      then: ["returns true", (result) => expect(result).toBe(true)],
      cleanup: () => stopServer(),
    });

    component("reports unhealthy when Python worker is down", {
      given: ["an unreachable Python worker", () =>
        new LocalBackend({ url: "http://localhost:1" })
      ],
      when: ["checking health", (backend) => backend.isHealthy()],
      then: ["returns false", (result) => expect(result).toBe(false)],
    });
  });

  rule("Job-Based Execution", () => {
    component("creates job and polls until complete", {
      given: ["a backend with a fast-completing job", async () => {
        await startServer({ pollsToComplete: 2, result: { embeddings: [1, 2, 3] } });
        return new LocalBackend({ url: serverUrl, pollInterval: 10 });
      }],
      when: ["executing a task", (backend) =>
        backend.execute({
          type: "analyze_full",
          payload: { source: "/path/to/video.mp4", workspaceId: "ws_1" },
        })
      ],
      then: ["returns the job result", (result) => {
        expect(result).toEqual({ embeddings: [1, 2, 3] });
      }],
      cleanup: () => stopServer(),
    });

    component("throws on job failure", {
      given: ["a backend with a failing job", async () => {
        await startServer({ pollsToComplete: 1, error: "CUDA out of memory" });
        return new LocalBackend({ url: serverUrl, pollInterval: 10 });
      }],
      then: ["rejects with error message", async (backend) => {
        await expect(
          backend.execute({
            type: "analyze_full",
            payload: { source: "/path/to/video.mp4" },
          })
        ).rejects.toThrow("CUDA out of memory");
      }],
      cleanup: () => stopServer(),
    });
  });

  rule("Worker Execution", () => {
    component("calls worker endpoint directly for worker.* tasks", {
      given: ["a backend with siglip worker endpoint", async () => {
        await startServer({
          workerResults: { siglip: { embeddings: [[0.1, 0.2]], count: 30 } },
        });
        return new LocalBackend({ url: serverUrl });
      }],
      when: ["executing a worker task", (backend) =>
        backend.execute({
          type: "worker.siglip",
          payload: { filePath: "/path/to/video.mp4", workspaceId: "ws_1" },
        })
      ],
      then: ["returns worker result directly", (result) => {
        expect(result).toEqual({ embeddings: [[0.1, 0.2]], count: 30 });
      }],
      cleanup: () => stopServer(),
    });
  });

  rule("Status & Cancel", () => {
    component("maps Python worker status to DWA format", {
      given: ["a backend with a running job", async () => {
        // pollsToComplete=2 so the mock is in "running" state after 1st poll
        await startServer({ pollsToComplete: 2 });
        return new LocalBackend({ url: serverUrl });
      }],
      when: ["getting job status", (backend) => backend.getStatus("job_mock_1")],
      then: ["returns mapped status with 0-100 progress", (result) => {
        expect(result.id).toBe("job_mock_1");
        expect(typeof result.progress).toBe("number");
        expect(result.progress).toBeGreaterThanOrEqual(0);
        expect(result.progress).toBeLessThanOrEqual(100);
      }],
      cleanup: () => stopServer(),
    });

    component("cancels a running job", {
      given: ["a backend with a cancellable job", async () => {
        await startServer({ pollsToComplete: 100 });
        return new LocalBackend({ url: serverUrl });
      }],
      when: ["cancelling the job", (backend) => backend.cancel!("job_mock_1")],
      then: ["returns true", (result) => expect(result).toBe(true)],
      cleanup: () => stopServer(),
    });
  });
});
