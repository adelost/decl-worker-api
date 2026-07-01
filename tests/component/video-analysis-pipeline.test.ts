/**
 * Tests for the video analysis pipeline DAG definition.
 */

import { feature, rule, unit, expect } from "bdd-vitest";
import {
  buildVideoAnalysisTask,
  getVideoAnalysisMermaid,
} from "../../packages/orchestrator/src/pipelines/video-analysis.js";

feature("Video Analysis Pipeline", () => {
  rule("Pipeline Construction", () => {
    unit("includes mandatory workers when no optional workers requested", {
      given: ["a minimal config", () => ({
        filePath: "/path/to/video.mp4",
        workspaceId: "ws_123",
      })],
      when: ["building the task", (config) => buildVideoAnalysisTask(config)],
      then: ["includes siglip, scenes, and tags", (task) => {
        const stepIds = task.steps!.map((s) => s.id);
        expect(stepIds).toContain("siglip");
        expect(stepIds).toContain("scenes");
        expect(stepIds).toContain("tags");
        // Should not include optional workers
        expect(stepIds).not.toContain("objects");
        expect(stepIds).not.toContain("events");
      }],
    });

    unit("includes requested optional workers with dependencies", {
      given: ["a config with objects and events", () => ({
        filePath: "/path/to/video.mp4",
        workspaceId: "ws_123",
        workers: ["objects", "events"],
      })],
      when: ["building the task", (config) => buildVideoAnalysisTask(config)],
      then: ["includes workers + their transitive dependencies", (task) => {
        const stepIds = task.steps!.map((s) => s.id);
        // objects depends on scenes, events depends on scenes
        expect(stepIds).toContain("objects");
        expect(stepIds).toContain("events");
        expect(stepIds).toContain("scenes");
        // scenes depends on siglip
        expect(stepIds).toContain("siglip");
      }],
    });

    unit("resolves deep transitive dependencies (poses-3d)", {
      given: ["a config requesting poses-3d", () => ({
        filePath: "/video.mp4",
        workspaceId: "ws_1",
        workers: ["poses-3d"],
      })],
      when: ["building the task", (config) => buildVideoAnalysisTask(config)],
      then: ["includes quality-poses and siglip", (task) => {
        const stepIds = task.steps!.map((s) => s.id);
        expect(stepIds).toContain("poses-3d");
        expect(stepIds).toContain("quality-poses");
        expect(stepIds).toContain("siglip");
      }],
    });
  });

  rule("DAG Dependencies", () => {
    unit("siglip and whisper have no dependencies (can run in parallel)", {
      given: ["a config with whisper", () => ({
        filePath: "/video.mp4",
        workspaceId: "ws_1",
        workers: ["whisper"],
      })],
      when: ["building the task", (config) => buildVideoAnalysisTask(config)],
      then: ["both have empty dependsOn", (task) => {
        const siglip = task.steps!.find((s) => s.id === "siglip");
        const whisper = task.steps!.find((s) => s.id === "whisper");
        expect(siglip!.dependsOn).toEqual([]);
        expect(whisper!.dependsOn).toEqual([]);
      }],
    });

    unit("scenes depends on siglip", {
      given: ["a default config", () => ({
        filePath: "/video.mp4",
        workspaceId: "ws_1",
      })],
      when: ["building the task", (config) => buildVideoAnalysisTask(config)],
      then: ["scenes step has siglip dependency", (task) => {
        const scenes = task.steps!.find((s) => s.id === "scenes");
        expect(scenes!.dependsOn).toEqual(["siglip"]);
      }],
    });
  });

  rule("Task Metadata", () => {
    unit("sets retry config with exponential backoff", {
      given: ["any config", () => ({
        filePath: "/video.mp4",
        workspaceId: "ws_1",
      })],
      when: ["building the task", (config) => buildVideoAnalysisTask(config)],
      then: ["has retry config", (task) => {
        expect(task.retry!.attempts).toBe(2);
        expect(task.retry!.backoff).toBe("exponential");
      }],
    });

    unit("includes lifecycle effects", {
      given: ["a config", () => ({
        filePath: "/video.mp4",
        workspaceId: "ws_1",
      })],
      when: ["building the task", (config) => buildVideoAnalysisTask(config)],
      then: ["has onSuccess and onError effects", (task) => {
        expect(task.onSuccess).toHaveLength(2);
        expect(task.onError).toHaveLength(1);
      }],
    });

    unit("passes payload to step inputs via templates", {
      given: ["a config", () => ({
        filePath: "/video.mp4",
        workspaceId: "ws_1",
      })],
      when: ["building the task", (config) => buildVideoAnalysisTask(config)],
      then: ["steps reference payload via templates", (task) => {
        const siglip = task.steps!.find((s) => s.id === "siglip")!;
        expect(siglip.input!.filePath).toBe("{{payload.filePath}}");
        expect(siglip.input!.workspaceId).toBe("{{payload.workspaceId}}");
      }],
    });
  });

  rule("Mermaid Visualization", () => {
    unit("generates valid Mermaid DAG", {
      when: ["generating mermaid diagram", () => getVideoAnalysisMermaid()],
      then: ["contains graph header and edges", (mermaid) => {
        expect(mermaid).toContain("graph TD");
        expect(mermaid).toContain("siglip --> scenes");
        expect(mermaid).toContain("siglip --> tags");
        expect(mermaid).toContain("scenes --> objects");
      }],
    });
  });
});
