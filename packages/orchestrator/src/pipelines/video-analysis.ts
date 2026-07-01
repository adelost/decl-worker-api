/**
 * Video analysis pipeline expressed as a DWA DAG.
 *
 * Dependency graph:
 *
 *   siglip ──┬── scenes ──┬── objects
 *             │            ├── faces
 *             │            ├── depth
 *             │            ├── descriptions
 *             │            ├── events
 *             │            └── quality-poses ── poses-3d
 *             ├── tags
 *             ├── ocr
 *             └── poses (lightweight alternative)
 *   whisper (independent, parallel with siglip)
 *
 * VRAM constraint: GPU queue concurrency=1 ensures only one GPU model loaded
 * at a time. CPU steps (scenes) run on a separate queue for parallelism.
 */

import type { Task, Step } from "@dwa/core";

/** Worker configuration for the video analysis pipeline */
export interface VideoAnalysisConfig {
  filePath: string;
  workspaceId: string;
  /** Which optional workers to include */
  workers?: string[];
  /** Override analysis config (sample_fps, etc.) */
  config?: Record<string, unknown>;
}

/**
 * All available worker steps with their dependencies and queue assignments.
 * The DWA orchestrator schedules these respecting dependsOn + queue concurrency.
 */
const WORKER_STEPS: Record<string, Omit<Step, "id">> = {
  // --- Mandatory, no dependencies ---
  siglip: {
    task: "worker.siglip",
    dependsOn: [],
    resources: { gpu: true },
  },

  // --- Independent audio track (parallel with siglip) ---
  whisper: {
    task: "worker.whisper",
    dependsOn: [],
    optional: true,
    resources: { gpu: true },
  },

  // --- Depends on siglip ---
  scenes: {
    task: "worker.scenes",
    dependsOn: ["siglip"],
    // CPU-only: uses existing embeddings, no GPU model needed
  },
  tags: {
    task: "worker.tags",
    dependsOn: ["siglip"],
    resources: { gpu: true },
  },
  ocr: {
    task: "worker.ocr",
    dependsOn: ["siglip"],
    optional: true,
    resources: { gpu: true },
  },
  poses: {
    task: "worker.poses",
    dependsOn: ["siglip"],
    optional: true,
    resources: { gpu: true },
  },

  // --- Depends on scenes (needs keyframe timestamps) ---
  objects: {
    task: "worker.objects",
    dependsOn: ["scenes"],
    optional: true,
    resources: { gpu: true },
  },
  faces: {
    task: "worker.faces",
    dependsOn: ["scenes"],
    optional: true,
    resources: { gpu: true },
  },
  depth: {
    task: "worker.depth",
    dependsOn: ["scenes"],
    optional: true,
    runWhen: "on-demand",
    resources: { gpu: true },
  },
  descriptions: {
    task: "worker.descriptions",
    dependsOn: ["scenes"],
    optional: true,
    resources: { gpu: true },
  },
  events: {
    task: "worker.events",
    dependsOn: ["scenes"],
    optional: true,
    resources: { gpu: true },
  },
  "quality-poses": {
    task: "worker.quality-poses",
    dependsOn: ["siglip"],
    optional: true,
    resources: { gpu: true },
  },

  // --- Deep dependencies ---
  "poses-3d": {
    task: "worker.poses-3d",
    dependsOn: ["quality-poses"],
    optional: true,
    resources: { gpu: true },
  },
};

/** Mandatory workers that always run */
const MANDATORY_WORKERS = ["siglip", "scenes", "tags"];

/**
 * Build a video analysis Task with DAG steps.
 *
 * Only includes mandatory workers + requested optional workers.
 * The DWA orchestrator handles parallel execution respecting dependencies.
 */
export function buildVideoAnalysisTask(config: VideoAnalysisConfig): Task {
  const requestedWorkers = new Set([
    ...MANDATORY_WORKERS,
    ...(config.workers ?? []),
  ]);

  // Resolve transitive dependencies
  const resolved = resolveDependencies(requestedWorkers);

  // Build step list from resolved workers
  const steps: Step[] = [];
  for (const workerName of resolved) {
    const stepDef = WORKER_STEPS[workerName];
    if (!stepDef) continue;

    steps.push({
      id: workerName,
      ...stepDef,
      input: {
        filePath: "{{payload.filePath}}",
        workspaceId: "{{payload.workspaceId}}",
        ...(config.config ? { config: JSON.stringify(config.config) } : {}),
      },
    });
  }

  return {
    type: "video.analyze",
    queue: "gpu", // Primary queue (individual steps may override)
    payload: {
      filePath: config.filePath,
      workspaceId: config.workspaceId,
      config: config.config ?? {},
    },
    steps,
    retry: { attempts: 2, backoff: "exponential", delay: 5000 },
    onSuccess: [
      { $event: "invalidate", path: `/api/workspace/${config.workspaceId}` },
      { $event: "toast", text: "Analysis complete", variant: "success" },
    ],
    onError: [
      { $event: "toast", text: "Analysis failed: {{error}}", variant: "error" },
    ],
  };
}

/**
 * Resolve transitive dependencies for a set of workers.
 * Returns all workers needed, including implicit dependencies.
 */
function resolveDependencies(workers: Set<string>): string[] {
  const resolved = new Set(workers);
  let changed = true;

  while (changed) {
    changed = false;
    for (const worker of [...resolved]) {
      const stepDef = WORKER_STEPS[worker];
      if (!stepDef?.dependsOn) continue;

      for (const dep of stepDef.dependsOn) {
        if (!resolved.has(dep)) {
          resolved.add(dep);
          changed = true;
        }
      }
    }
  }

  return [...resolved];
}

/** Get the Mermaid DAG visualization for the default pipeline */
export function getVideoAnalysisMermaid(workers?: string[]): string {
  const requestedWorkers = new Set([
    ...MANDATORY_WORKERS,
    ...(workers ?? Object.keys(WORKER_STEPS)),
  ]);
  const resolved = resolveDependencies(requestedWorkers);

  const lines = ["graph TD"];
  for (const worker of resolved) {
    const stepDef = WORKER_STEPS[worker];
    if (!stepDef) continue;

    // Node label
    const label = worker.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    const gpu = stepDef.resources?.gpu ? " [GPU]" : "";
    lines.push(`  ${worker}["${label}${gpu}"]`);

    // Edges from dependencies
    for (const dep of stepDef.dependsOn ?? []) {
      if (resolved.includes(dep)) {
        lines.push(`  ${dep} --> ${worker}`);
      }
    }
  }

  return lines.join("\n");
}
