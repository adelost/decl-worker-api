/**
 * Tests for the TimingStore (warmup/rate predictions).
 * Uses a minimal in-memory Redis mock.
 */

import { feature, rule, unit, expect } from "bdd-vitest";
import { TimingStore, type TimingDetail } from "../../packages/orchestrator/src/timing.js";

// Minimal Redis mock: only get/set needed by TimingStore
function createMockRedis() {
  const store = new Map<string, string>();
  return {
    get: async (key: string) => store.get(key) ?? null,
    set: async (key: string, value: string) => { store.set(key, value); return "OK"; },
    _store: store,
  } as any; // eslint-disable-line @typescript-eslint/no-explicit-any
}

feature("TimingStore", () => {
  rule("Recording", () => {
    unit("records warmup and rate from a completed task", {
      given: ["an empty timing store", () =>
        new TimingStore(createMockRedis())
      ],
      when: ["recording timing for siglip (10s total, 2s warmup, 30s video)", async (store) => {
        await store.record("gpu.siglip", { duration: 10, warmup: 2, inputDuration: 30 });
        return store;
      }],
      then: ["prediction uses recorded data", async (store) => {
        const detail = await store.predict("gpu.siglip", 60);
        expect(detail).not.toBeNull();
        expect(detail!.warmup).toBeCloseTo(2.0);
        // rate = (10-2)/30 = 0.267, processing = 0.267 * 60 = 16.0
        expect(detail!.processing).toBeCloseTo(16.0);
        expect(detail!.total).toBeCloseTo(18.0);
      }],
    });

    unit("skips zero-duration tasks (cached results)", {
      given: ["a timing store", () => new TimingStore(createMockRedis())],
      when: ["recording a cached task (duration=0)", async (store) => {
        await store.record("gpu.siglip", { duration: 0, warmup: 0, inputDuration: 30 });
        return store;
      }],
      then: ["no data recorded", async (store) => {
        const detail = await store.predict("gpu.siglip", 30);
        expect(detail).toBeNull();
      }],
    });

    unit("skips tasks without input duration (images)", {
      given: ["a timing store", () => new TimingStore(createMockRedis())],
      when: ["recording with zero input duration", async (store) => {
        await store.record("gpu.siglip", { duration: 5, warmup: 1, inputDuration: 0 });
        return store;
      }],
      then: ["no data recorded", async (store) => {
        const detail = await store.predict("gpu.siglip", 30);
        expect(detail).toBeNull();
      }],
    });
  });

  rule("Rolling Window", () => {
    unit("averages multiple samples for smoother predictions", {
      given: ["a store with 3 samples", async () => {
        const store = new TimingStore(createMockRedis());
        // Warmups: 2, 3, 4 → avg 3. Rates: 0.2, 0.3, 0.4 → avg 0.3
        await store.record("gpu.whisper", { duration: 8,  warmup: 2, inputDuration: 30 });
        await store.record("gpu.whisper", { duration: 12, warmup: 3, inputDuration: 30 });
        await store.record("gpu.whisper", { duration: 16, warmup: 4, inputDuration: 30 });
        return store;
      }],
      then: ["prediction averages all samples", async (store) => {
        const detail = await store.predict("gpu.whisper", 60);
        expect(detail!.warmup).toBeCloseTo(3.0);
        // avg rate = (6/30 + 9/30 + 12/30) / 3 = 0.3, processing = 0.3 * 60 = 18
        expect(detail!.processing).toBeCloseTo(18.0);
      }],
    });

    unit("keeps only last 5 samples", {
      given: ["a store with 7 samples", async () => {
        const store = new TimingStore(createMockRedis());
        // Record 7 samples with increasing warmup: 1, 2, 3, 4, 5, 6, 7
        for (let i = 1; i <= 7; i++) {
          await store.record("gpu.depth", { duration: i + 5, warmup: i, inputDuration: 10 });
        }
        return store;
      }],
      then: ["only last 5 samples used (warmup 3,4,5,6,7 → avg 5)", async (store) => {
        const detail = await store.predict("gpu.depth", 10);
        expect(detail!.warmup).toBeCloseTo(5.0);
      }],
    });
  });

  rule("Prediction", () => {
    unit("returns null for unknown task types", {
      given: ["an empty store", () => new TimingStore(createMockRedis())],
      then: ["predict returns null", async (store) => {
        const detail = await store.predict("unknown.task", 60);
        expect(detail).toBeNull();
      }],
    });

    unit("predictAll returns per-step breakdown and total", {
      given: ["a store with data for siglip and whisper", async () => {
        const store = new TimingStore(createMockRedis());
        await store.record("gpu.siglip", { duration: 10, warmup: 2, inputDuration: 30 });
        await store.record("gpu.whisper", { duration: 17, warmup: 3, inputDuration: 30 });
        return store;
      }],
      then: ["predictAll includes both + total", async (store) => {
        const prediction = await store.predictAll(
          ["gpu.siglip", "gpu.whisper", "cpu.unknown"],
          60
        );

        expect(prediction.steps["gpu.siglip"]).not.toBeNull();
        expect(prediction.steps["gpu.whisper"]).not.toBeNull();
        expect(prediction.steps["cpu.unknown"]).toBeNull();

        // Total = siglip.total + whisper.total (unknown excluded)
        expect(prediction.total).toBeGreaterThan(0);
        expect(prediction.total).toBe(
          prediction.steps["gpu.siglip"]!.total +
          prediction.steps["gpu.whisper"]!.total
        );
      }],
    });
  });
});
