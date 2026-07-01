/**
 * TimingStore: persistent timing predictions for progress bar weighting.
 *
 * Records warmup time and processing rate per task type after each run.
 * Uses a rolling average (max 5 samples) to smooth variability.
 * Stores in Redis (dwa:timing:{taskType}) for persistence across restarts.
 *
 * Ported from ai-dsl's JobTimingStore (Python, JSON file) to DWA (TypeScript, Redis).
 *
 * Usage:
 *   const store = new TimingStore(redis);
 *   await store.record("gpu.siglip", { duration: 10.0, warmup: 2.0, inputDuration: 30.0 });
 *   const prediction = await store.predictAll(["gpu.siglip", "gpu.whisper"], 60.0);
 */

import type { Redis } from "ioredis";

const MAX_SAMPLES = 5;
const KEY_PREFIX = "dwa:timing:";

export interface TimingDetail {
  warmup: number;
  processing: number;
  total: number;
}

export interface TimingPrediction {
  steps: Record<string, TimingDetail | null>;
  total: number;
}

interface TimingEntry {
  warmup: number[];
  rate: number[];
}

export class TimingStore {
  constructor(private redis: Redis) {}

  /**
   * Record timing from a completed task.
   * Skips cached tasks (duration=0) or tasks without meaningful input (inputDuration=0).
   */
  async record(
    taskType: string,
    opts: { duration: number; warmup: number; inputDuration: number }
  ): Promise<void> {
    const { duration, warmup, inputDuration } = opts;

    // Skip cached/instant tasks and tasks without measurable input
    if (duration <= 0 || inputDuration <= 0) return;

    const processing = Math.max(duration - warmup, 0);
    const rate = processing / inputDuration;

    const key = KEY_PREFIX + taskType;
    const entry = await this.getEntry(key);

    entry.warmup.push(warmup);
    entry.rate.push(rate);

    // Rolling window
    entry.warmup = entry.warmup.slice(-MAX_SAMPLES);
    entry.rate = entry.rate.slice(-MAX_SAMPLES);

    await this.redis.set(key, JSON.stringify(entry));
  }

  /**
   * Predict timing for a single task type.
   * Returns null if no historical data exists.
   */
  async predict(taskType: string, inputDuration: number): Promise<TimingDetail | null> {
    const entry = await this.getEntry(KEY_PREFIX + taskType);

    if (entry.warmup.length === 0) return null;

    const avgWarmup = average(entry.warmup);
    const avgRate = average(entry.rate);
    const processing = avgRate * inputDuration;

    return {
      warmup: avgWarmup,
      processing,
      total: avgWarmup + processing,
    };
  }

  /**
   * Predict total time for multiple tasks with per-task breakdown.
   */
  async predictAll(taskTypes: string[], inputDuration: number): Promise<TimingPrediction> {
    const steps: Record<string, TimingDetail | null> = {};
    let total = 0;

    for (const taskType of taskTypes) {
      const detail = await this.predict(taskType, inputDuration);
      steps[taskType] = detail;
      if (detail) {
        total += detail.total;
      }
    }

    return { steps, total };
  }

  private async getEntry(key: string): Promise<TimingEntry> {
    const raw = await this.redis.get(key);
    if (!raw) return { warmup: [], rate: [] };

    try {
      const parsed = JSON.parse(raw) as TimingEntry;
      return {
        warmup: Array.isArray(parsed.warmup) ? parsed.warmup : [],
        rate: Array.isArray(parsed.rate) ? parsed.rate : [],
      };
    } catch {
      return { warmup: [], rate: [] };
    }
  }
}

function average(arr: number[]): number {
  if (arr.length === 0) return 0;
  return arr.reduce((sum, v) => sum + v, 0) / arr.length;
}
