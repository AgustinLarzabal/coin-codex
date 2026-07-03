import { readFile } from "node:fs/promises";
import path from "node:path";

import { describe, expect, it } from "vitest";

describe("README operator console workflow", () => {
  it("documents the guided operator console workflow and its safety defaults", async () => {
    const readmePath = path.join(process.cwd(), "README.md");
    const readme = await readFile(readmePath, "utf8");

    expect(readme).toContain("## Operator Console");
    expect(readme).toContain("pnpm exec tsx src/cli.ts operator-console");
    expect(readme).toContain(
      "Seed Sources -> Create Crawl Run -> Process Jobs -> Inspect Results",
    );
    expect(readme).toContain(".private/sources.json");
    expect(readme).toContain("--seed-file");
    expect(readme).toContain("process next job");
    expect(readme).toContain("process until idle");
    expect(readme).toContain("default 100-job per-action safety cap");
    expect(readme).toContain("continues through failed jobs");
    expect(readme).toContain("private debug");
    expect(readme).toContain("existing non-interactive CLI commands remain available");
  });
});
