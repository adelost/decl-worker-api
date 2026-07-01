/**
 * Fastify server for the orchestrator API.
 */

import Fastify from "fastify";
import cors from "@fastify/cors";
import type { Task, TaskResult } from "@dwa/core";
import { registerBackend } from "@dwa/core";
import { enqueueTask, getTaskStatus, createWorkers, shutdown, getConnection } from "./queue.js";
import { TimingStore } from "./timing.js";
import { ModalBackend } from "./backends/modal.js";
import { RayBackend } from "./backends/ray.js";
import { LocalBackend } from "./backends/local.js";

const fastify = Fastify({
  logger: true,
});

/**
 * API response type for task status.
 * Maps internal TaskResult to public API contract.
 *
 * This is the DTO pattern: internal types stay internal,
 * public API has its own stable contract.
 */
interface ApiTaskResult<T = unknown> {
  taskId: string;
  status: "queued" | "running" | "completed" | "failed";
  result?: T;
  error?: string;
  progress?: number;
}

/** Extended API response for list endpoint (includes type and queue) */
interface ApiTaskListItem extends ApiTaskResult {
  type?: string;
  queue?: string;
}

type ApiStatus = ApiTaskResult["status"];

function mapStatus(internal: string): ApiStatus {
  return internal === "pending" ? "queued" : internal as ApiStatus;
}

/**
 * Maps internal TaskResult to public API response.
 * - id → taskId (semantic naming for SDK users)
 * - "pending" → "queued" (user-friendly status)
 */
function toApiResponse<T = unknown>(internal: TaskResult): ApiTaskResult<T> {
  return {
    taskId: internal.id,
    status: mapStatus(internal.status),
    result: internal.result as T,
    error: internal.error,
    progress: internal.progress,
  };
}

/** Maps list item (has extra fields like type, queue) */
function toApiListItem(internal: TaskResult & { type?: string; queue?: string }): ApiTaskListItem {
  return {
    taskId: internal.id,
    status: mapStatus(internal.status),
    result: internal.result,
    error: internal.error,
    progress: internal.progress,
    type: internal.type,
    queue: internal.queue,
  };
}

// Enable CORS
await fastify.register(cors, {
  origin: process.env.CORS_ORIGIN || true,
});

// Register backends
const modalUrl = process.env.MODAL_URL;
if (modalUrl) {
  registerBackend(
    new ModalBackend({
      url: modalUrl,
      token: process.env.MODAL_TOKEN,
    })
  );
  fastify.log.info(`Registered Modal backend: ${modalUrl}`);
}

const rayUrl = process.env.RAY_URL;
if (rayUrl) {
  registerBackend(
    new RayBackend({
      url: rayUrl,
      dashboardUrl: process.env.RAY_DASHBOARD_URL,
    })
  );
  fastify.log.info(`Registered Ray backend: ${rayUrl}`);
}

const localUrl = process.env.LOCAL_WORKER_URL;
if (localUrl) {
  registerBackend(
    new LocalBackend({
      url: localUrl,
      pollInterval: parseInt(process.env.LOCAL_POLL_INTERVAL || "500"),
      timeout: parseInt(process.env.LOCAL_TIMEOUT || "600"),
    })
  );
  fastify.log.info(`Registered Local backend: ${localUrl}`);
}

// Health check
fastify.get("/health", async () => {
  return { status: "healthy", timestamp: new Date().toISOString() };
});

// Create task
fastify.post<{ Body: Task }>("/api/tasks", async (request, reply) => {
  const task = request.body;

  if (!task.type) {
    return reply.status(400).send({ error: "task.type is required" });
  }

  if (!task.payload) {
    return reply.status(400).send({ error: "task.payload is required" });
  }

  try {
    const jobId = await enqueueTask(task);
    return {
      taskId: jobId,
      status: "queued" as const,
      queue: task.queue || "default",
    };
  } catch (error) {
    fastify.log.error(error);
    return reply.status(500).send({
      error: error instanceof Error ? error.message : "Failed to enqueue task",
    });
  }
});

// Get task status
fastify.get<{
  Params: { id: string };
  Querystring: { queue?: string };
}>("/api/tasks/:id", async (request, reply) => {
  const { id } = request.params;
  const { queue } = request.query;

  const internal = await getTaskStatus(id, queue);

  if (!internal) {
    return reply.status(404).send({ error: "Task not found" });
  }

  return toApiResponse(internal);
});

// List tasks with actual job data
fastify.get<{
  Querystring: { queue?: string; status?: string; limit?: number };
}>("/api/tasks", async (request) => {
  const { queue: queueName = "default", status, limit = 50 } = request.query;

  const { listTasks } = await import("./queue.js");
  const internalTasks = await listTasks(queueName, status, limit);

  return {
    queue: queueName,
    count: internalTasks.length,
    tasks: internalTasks.map(toApiListItem),
  };
});

// Visualize pipeline DAG (returns Mermaid diagram)
fastify.post<{ Body: Task }>("/api/visualize", async (request, reply) => {
  const task = request.body;

  if (!task.steps?.length) {
    return reply.status(400).send({ error: "No steps in task" });
  }

  const { visualizePipeline } = await import("./engine/visualize.js");
  const diagram = visualizePipeline(task);

  return {
    mermaid: diagram,
    url: `https://mermaid.live/edit#pako:${Buffer.from(JSON.stringify({ code: diagram })).toString("base64")}`,
  };
});

// Timing predictions
fastify.get<{
  Querystring: { inputDuration: number; steps: string };
}>("/api/timing/predictions", async (request) => {
  const { inputDuration, steps: stepsStr } = request.query;
  const stepList = stepsStr.split(",").map((s) => s.trim()).filter(Boolean);
  const store = new TimingStore(getConnection());
  return store.predictAll(stepList, inputDuration);
});

// Cancel task
fastify.delete<{
  Params: { id: string };
  Querystring: { queue?: string };
}>("/api/tasks/:id", async (request, reply) => {
  const { id } = request.params;
  const { queue = "default" } = request.query;

  const internal = await getTaskStatus(id, queue);

  if (!internal) {
    return reply.status(404).send({ error: "Task not found" });
  }

  if (internal.status === "completed" || internal.status === "failed") {
    return reply.status(400).send({ error: "Task already finished" });
  }

  // Note: Actual cancellation requires worker cooperation
  return { taskId: id, cancelled: true };
});

// Start workers and server
async function start() {
  // Create workers
  const workers = createWorkers();
  fastify.log.info("Workers started");

  // Graceful shutdown
  const signals = ["SIGINT", "SIGTERM"];
  for (const signal of signals) {
    process.on(signal, async () => {
      fastify.log.info(`Received ${signal}, shutting down...`);

      await Promise.all([
        workers.default.close(),
        workers.gpu.close(),
        workers.cpu.close(),
      ]);

      await shutdown();
      await fastify.close();
      process.exit(0);
    });
  }

  // Start server
  const port = parseInt(process.env.PORT || "3000");
  const host = process.env.HOST || "0.0.0.0";

  try {
    await fastify.listen({ port, host });
    fastify.log.info(`Server listening on ${host}:${port}`);
  } catch (error) {
    fastify.log.error(error);
    process.exit(1);
  }
}

start();
