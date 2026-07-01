/**
 * Task dispatcher - routes tasks to appropriate backends.
 * Supports parallel execution of independent steps via DAG scheduling.
 * Supports chunking for long-running tasks.
 */

import type { Task, Step } from "@dwa/core";
import { selectBackend } from "@dwa/core";
import {
  type ChunkConfig,
  shouldChunk,
  processWithChunking,
} from "./chunking.js";
import { sleep, withTimeout, executeWithRetry } from "./execution.js";
import { resolveTemplate, resolveTemplates } from "./templates.js";
import { processPipelineSequential } from "./sequential.js";

export type ProgressCallback = (progress: number) => void;

/** Sub-step progress detail (e.g. "frame 30/60", warmup timing) */
export interface StepProgress {
  /** 0-1 progress within this step */
  progress: number;
  /** Human-readable message, e.g. "Frame 30/60" */
  message?: string;
  /** Timing breakdown, e.g. { warmup: 2.1, inference: 15.3 } */
  subTimings?: Record<string, number>;
}

/** Step execution status for observability */
export interface StepStatus {
  id: string;
  task: string;
  status: "pending" | "running" | "completed" | "failed" | "skipped";
  startedAt?: Date;
  completedAt?: Date;
  duration?: number;
  error?: string;
  result?: unknown;
  retryAttempt?: number;
  /** Sub-step progress (updated during execution) */
  stepProgress?: StepProgress;
}

/** Pipeline execution result with full observability */
export interface PipelineResult {
  steps: unknown[];
  stepResults: Record<string, unknown>;
  stepStatus: StepStatus[];
  finalResult: unknown;
  totalDuration: number;
  parallelGroups: string[][];  // Which steps ran together
}

/** Event emitter for pipeline observability */
export type PipelineEventType = "step:start" | "step:complete" | "step:error" | "step:progress" | "pipeline:complete";
export interface PipelineEvent {
  type: PipelineEventType;
  stepId?: string;
  stepTask?: string;
  timestamp: Date;
  data?: unknown;
}
export type PipelineEventCallback = (event: PipelineEvent) => void;

/**
 * Process a task through the selected backend.
 * Supports automatic chunking for long-running tasks.
 */
export async function processTask(
  task: Task,
  onProgress?: ProgressCallback,
  onEvent?: PipelineEventCallback,
  chunkConfig?: ChunkConfig
): Promise<unknown> {
  // Handle pipeline tasks
  if (task.steps?.length) {
    // Check if any step has dependencies - use DAG scheduler
    const hasDependencies = task.steps.some((s) => s.dependsOn?.length || s.id);
    if (hasDependencies) {
      return processPipelineDAG(task, onProgress, onEvent);
    }
    // Legacy sequential processing
    return processPipelineSequential(task, onProgress, onEvent);
  }

  // Check if task should be chunked
  if (chunkConfig && await shouldChunk(task, chunkConfig)) {
    return processWithChunking(task, chunkConfig, async (chunkTask) => {
      const backend = await selectBackend(chunkTask);
      return executeWithRetry(chunkTask, () => backend.execute(chunkTask));
    });
  }

  // Single task execution with retry
  const backend = await selectBackend(task);
  return executeWithRetry(task, () => backend.execute(task));
}

/**
 * Process pipeline with DAG-based parallel execution.
 * Steps run in parallel when their dependencies are satisfied.
 */
async function processPipelineDAG(
  task: Task,
  onProgress?: ProgressCallback,
  onEvent?: PipelineEventCallback
): Promise<PipelineResult> {
  const pipelineStart = Date.now();
  const steps = task.steps!;

  // Build step map with auto-generated IDs if needed
  const stepMap = new Map<string, Step>();
  const stepIndexToId = new Map<number, string>();

  steps.forEach((step, index) => {
    const id = step.id || `step_${index}`;
    stepMap.set(id, step);
    stepIndexToId.set(index, id);
  });

  // Results keyed by step ID
  const results: Record<string, unknown> = {};
  const completed = new Set<string>();
  const failed = new Set<string>();
  const running = new Set<string>();

  // Step status tracking for observability
  const stepStatuses: Map<string, StepStatus> = new Map();
  const parallelGroups: string[][] = [];

  // Initialize step statuses
  for (const [id, step] of stepMap) {
    stepStatuses.set(id, {
      id,
      task: step.task,
      status: "pending",
    });
  }

  // Context for template resolution
  const context = {
    payload: task.payload,
    steps: results,
  };

  // Helper to emit events
  const emit = (type: PipelineEventType, stepId?: string, data?: unknown) => {
    if (onEvent) {
      const step = stepId ? stepMap.get(stepId) : undefined;
      onEvent({
        type,
        stepId,
        stepTask: step?.task,
        timestamp: new Date(),
        data,
      });
    }
  };

  /**
   * Check if a step's dependencies are satisfied.
   */
  function canRun(step: Step, stepId: string): boolean {
    if (running.has(stepId) || completed.has(stepId) || failed.has(stepId)) {
      return false;
    }

    const deps = step.dependsOn || [];
    return deps.every((depId) => completed.has(depId));
  }

  /**
   * Get all steps that can run now.
   */
  function getRunnableSteps(): Array<{ id: string; step: Step }> {
    const runnable: Array<{ id: string; step: Step }> = [];

    for (const [id, step] of stepMap) {
      if (canRun(step, id)) {
        runnable.push({ id, step });
      }
    }

    return runnable;
  }

  /**
   * Execute a single step (with optional forEach).
   */
  async function executeStep(
    stepId: string,
    step: Step
  ): Promise<{ id: string; result?: unknown; error?: Error }> {
    running.add(stepId);

    // Get status reference
    const status = stepStatuses.get(stepId)!;

    // Check runWhen condition BEFORE marking as running
    if (step.runWhen && step.runWhen !== "always") {
      if (step.runWhen === "on-demand") {
        // on-demand steps are skipped unless explicitly needed
        status.status = "skipped";
        running.delete(stepId);
        completed.add(stepId);
        results[stepId] = { skipped: true, reason: "on-demand" };
        emit("step:complete", stepId, { skipped: true });
        return { id: stepId, result: results[stepId] };
      }

      // Template condition - evaluate
      const conditionResult = resolveTemplate(step.runWhen, context);
      if (!conditionResult) {
        status.status = "skipped";
        running.delete(stepId);
        completed.add(stepId);
        results[stepId] = { skipped: true, reason: "condition-false", condition: step.runWhen };
        emit("step:complete", stepId, { skipped: true, condition: step.runWhen });
        return { id: stepId, result: results[stepId] };
      }
    }

    // Update status to running
    status.status = "running";
    status.startedAt = new Date();
    emit("step:start", stepId);

    try {
      // Handle forEach - run step for each item in array
      if (step.forEach) {
        const items = resolveTemplate(step.forEach, context);

        if (!Array.isArray(items)) {
          throw new Error(
            `forEach template "${step.forEach}" did not resolve to array, got: ${typeof items}`
          );
        }

        const concurrency = step.forEachConcurrency || items.length;
        const itemResults: unknown[] = [];

        // Process items in batches based on concurrency
        for (let i = 0; i < items.length; i += concurrency) {
          const batch = items.slice(i, i + concurrency);
          const batchPromises = batch.map(async (item, batchIndex) => {
            const index = i + batchIndex;
            const itemContext = {
              ...context,
              item,
              index,
            };

            const resolvedInput = resolveTemplates(step.input || {}, itemContext);

            const stepTask: Task = {
              type: step.task,
              backend: task.backend,
              payload: resolvedInput,
              resources: step.resources || task.resources,
              retry: step.retry || task.retry,
            };

            const executeItem = async () => {
              const backend = await selectBackend(stepTask);
              return executeWithRetry(stepTask, () => backend.execute(stepTask), (attempt) => {
                status.retryAttempt = attempt;
              });
            };

            // Get timeout from step or task resources
            const timeoutSeconds = step.timeout || task.resources?.timeout;
            return timeoutSeconds
              ? withTimeout(executeItem(), timeoutSeconds * 1000, `${stepId}[${index}]`)
              : executeItem();
          });

          const batchResults = await Promise.all(batchPromises);
          itemResults.push(...batchResults);
        }

        running.delete(stepId);
        completed.add(stepId);
        results[stepId] = itemResults;

        // Update status
        status.status = "completed";
        status.completedAt = new Date();
        status.duration = status.completedAt.getTime() - status.startedAt!.getTime();
        status.result = itemResults;
        emit("step:complete", stepId, { itemCount: itemResults.length });

        return { id: stepId, result: itemResults };
      }

      // Regular single execution
      const resolvedInput = resolveTemplates(step.input || {}, context);

      const stepTask: Task = {
        type: step.task,
        backend: task.backend,
        payload: resolvedInput,
        resources: step.resources || task.resources,
        retry: step.retry || task.retry,
      };

      const executeBackend = async () => {
        const backend = await selectBackend(stepTask);
        return executeWithRetry(stepTask, () => backend.execute(stepTask), (attempt) => {
          status.retryAttempt = attempt;
        });
      };

      // Get timeout from step or task resources
      const timeoutSeconds = step.timeout || task.resources?.timeout;
      const result = timeoutSeconds
        ? await withTimeout(executeBackend(), timeoutSeconds * 1000, stepId)
        : await executeBackend();

      running.delete(stepId);
      completed.add(stepId);
      results[stepId] = result;

      // Update status
      status.status = "completed";
      status.completedAt = new Date();
      status.duration = status.completedAt.getTime() - status.startedAt!.getTime();
      status.result = result;
      emit("step:complete", stepId, result);

      return { id: stepId, result };
    } catch (error) {
      running.delete(stepId);
      const errorMsg = error instanceof Error ? error.message : String(error);

      if (step.optional) {
        completed.add(stepId); // Mark as completed so dependents can run
        results[stepId] = { error: errorMsg, skipped: true };

        // Update status
        status.status = "skipped";
        status.completedAt = new Date();
        status.duration = status.completedAt.getTime() - status.startedAt!.getTime();
        status.error = errorMsg;
        emit("step:error", stepId, { error: errorMsg, optional: true });

        return { id: stepId, result: results[stepId] };
      }

      failed.add(stepId);

      // Update status
      status.status = "failed";
      status.completedAt = new Date();
      status.duration = status.completedAt.getTime() - status.startedAt!.getTime();
      status.error = errorMsg;
      emit("step:error", stepId, { error: errorMsg });

      return {
        id: stepId,
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }

  // Main execution loop
  const totalSteps = steps.length;
  let processedSteps = 0;

  while (completed.size + failed.size < totalSteps) {
    const runnable = getRunnableSteps();

    if (runnable.length === 0) {
      // Check for deadlock (no runnable steps but not all completed)
      if (running.size === 0) {
        const remaining = steps
          .filter((_, i) => {
            const id = stepIndexToId.get(i)!;
            return !completed.has(id) && !failed.has(id);
          })
          .map((s) => s.task);

        throw new Error(
          `Pipeline deadlock: cannot run remaining steps [${remaining.join(", ")}]. ` +
            `Check for circular dependencies or missing dependency IDs.`
        );
      }
      // Wait for running steps to complete
      await sleep(10);
      continue;
    }

    // Track which steps ran in parallel
    if (runnable.length > 1) {
      parallelGroups.push(runnable.map(r => r.id));
    }

    // Run all runnable steps in parallel
    const executions = runnable.map(({ id, step }) => executeStep(id, step));
    const outcomes = await Promise.all(executions);

    // Check for failures
    for (const outcome of outcomes) {
      if (outcome.error) {
        throw outcome.error;
      }
    }

    // Update progress
    processedSteps = completed.size;
    const progress = Math.round((processedSteps / totalSteps) * 100);
    onProgress?.(progress);
  }

  // Build ordered results array for backward compatibility
  const orderedResults: unknown[] = [];
  steps.forEach((_, index) => {
    const id = stepIndexToId.get(index)!;
    orderedResults.push(results[id]);
  });

  const totalDuration = Date.now() - pipelineStart;

  // Emit pipeline complete
  emit("pipeline:complete", undefined, { totalDuration, stepCount: steps.length });

  return {
    steps: orderedResults,
    stepResults: results,
    stepStatus: Array.from(stepStatuses.values()),
    finalResult: orderedResults[orderedResults.length - 1],
    totalDuration,
    parallelGroups,
  };
}
