/**
 * Task and pipeline types for the worker DSL.
 */

import type { Effect } from "./effect.js";
import type { ResourceRequirements } from "./resource.js";

export type BackendType = "modal" | "ray" | "local" | "auto";
export type QueueType = "default" | "gpu" | "cpu";

export interface Task {
  /** Unique task ID (auto-generated if not provided) */
  id?: string;

  /** Task type identifier, e.g. "ai.chat", "process.transcode" */
  type: string;

  /** Target backend - defaults to "auto" */
  backend?: BackendType;

  /** Queue assignment - defaults to "default" */
  queue?: QueueType;

  /** Priority (higher = more important) */
  priority?: number;

  /** Task payload/arguments */
  payload: Record<string, unknown>;

  /** Pipeline steps for multi-step tasks */
  steps?: Step[];

  /** Resource requirements */
  resources?: ResourceRequirements;

  /** Retry configuration */
  retry?: RetryConfig;

  /** Delay execution by milliseconds */
  delay?: number;

  /** Cron schedule expression */
  cron?: string;

  /** Effects triggered when task is pending */
  onPending?: Effect[];

  /** Effects triggered on progress updates */
  onProgress?: Effect[];

  /** Effects triggered on successful completion */
  onSuccess?: Effect[];

  /** Effects triggered on error */
  onError?: Effect[];
}

export interface Step {
  /** Step ID for referencing in dependencies and results */
  id?: string;

  /** Task type to execute */
  task: string;

  /** Input mapping with template support: "{{payload.url}}", "{{steps.download.path}}" */
  input?: Record<string, string>;

  /**
   * Steps this step depends on (by ID).
   * Step won't run until all dependencies complete.
   * Steps without dependencies or with all deps resolved run in parallel.
   */
  dependsOn?: string[];

  /**
   * Run this step for each item in an array.
   * Template that resolves to an array, e.g., "{{steps.scenes.keyframes}}"
   * Inside input, use "{{item}}" for current item and "{{index}}" for index.
   * Results are collected into an array.
   */
  forEach?: string;

  /**
   * Maximum parallel executions for forEach.
   * Default: unlimited (all items in parallel)
   */
  forEachConcurrency?: number;

  /**
   * Condition for running this step.
   * - "always" (default): Run when dependencies complete
   * - "on-demand": Only run if explicitly requested or results are referenced
   * - Template string: Run if template evaluates to truthy value
   */
  runWhen?: "always" | "on-demand" | string;

  /** Step timeout in seconds */
  timeout?: number;

  /** If true, pipeline continues even if this step fails */
  optional?: boolean;

  /** Step-specific resource requirements */
  resources?: ResourceRequirements;

  /** Step-specific retry configuration (overrides task-level) */
  retry?: RetryConfig;
}

export interface RetryConfig {
  /** Maximum retry attempts */
  attempts?: number;

  /** Backoff strategy */
  backoff?: "fixed" | "exponential";

  /** Initial delay in milliseconds */
  delay?: number;

  /** Maximum delay for exponential backoff */
  maxDelay?: number;
}

export type TaskStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

export interface TaskResult {
  id: string;
  status: TaskStatus;
  result?: unknown;
  error?: string;
  progress?: number;
  startedAt?: Date;
  completedAt?: Date;
  stepResults?: StepResult[];
}

export interface StepResult {
  step: number;
  task: string;
  status: TaskStatus;
  result?: unknown;
  error?: string;
}
