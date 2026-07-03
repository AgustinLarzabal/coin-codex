import { readFile } from "node:fs/promises";
import path from "node:path";

import { describe, expect, it } from "vitest";

const EXPECTED_OPERATOR_CONSOLE_README_SNIPPETS = [
  "## Operator Console",
  "pnpm exec tsx src/cli.ts operator-console",
  "Seed Sources -> Create Crawl Run -> Process Jobs -> Inspect Results",
  ".private/sources.json",
  "--seed-file",
  "process next job",
  "process until idle",
  "default 100-job per-action safety cap",
  "continues through failed jobs",
  "private debug",
  "existing non-interactive CLI commands remain available",
];

describe("README operator console workflow", () => {
  it("documents the guided operator console workflow and its safety defaults", async () => {
    const readmePath = path.join(process.cwd(), "README.md");
    const readme = await readFile(readmePath, "utf8");

    for (const snippet of EXPECTED_OPERATOR_CONSOLE_README_SNIPPETS) {
      expect(readme).toContain(snippet);
    }
  });
});
