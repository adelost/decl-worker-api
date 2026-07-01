/**
 * Backend interface and configuration types.
 */

import type { Task, TaskResult, TaskStatus } from "./task.js";
import type { ResourcePool } from "./resource.js";

export interface Backend {
  /** Backend name identifier */
  name: string;

  /** Execute a task and return the result */
  execute(task: Task): Promise<unknown>;

  /** Get status of a running task */
  getStatus(taskId: string): Promise<TaskResult>;

  /** Cancel a running task */
  cancel?(taskId: string): Promise<boolean>;

  /** Check if backend is healthy and available */
  isHealthy(): Promise<boolean>;

  /** Get available resources (optional) */
  getResources?(): Promise<ResourcePool>;
}

export interface BackendConfig {
  modal?: ModalConfig;
  ray?: RayConfig;
  local?: LocalConfig;
}

export interface ModalConfig {
  /** Modal API URL */
  url: string;
  /** Authentication token */
  token?: string;
  /** Default timeout in seconds */
  timeout?: number;
}

export interface RayConfig {
  /** Ray Serve API URL */
  url: string;
  /** Ray dashboard URL (optional) */
  dashboardUrl?: string;
}

export interface LocalConfig {
  /** Python worker base URL, e.g. "http://localhost:8080" */
  url: string;
  /** Poll interval in ms (default: 500) */
  pollInterval?: number;
  /** Default timeout in seconds (default: 600) */
  timeout?: number;
}
