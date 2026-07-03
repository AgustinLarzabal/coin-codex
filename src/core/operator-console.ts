import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import type { IngestionService } from "./ingestion-service.js";
import { parseSeedSourceRecords } from "./source-config.js";

export const DEFAULT_OPERATOR_CONSOLE_SEED_FILE = ".private/sources.json";
const DEFAULT_SCOPE = "default";
const DEFAULT_DETAIL_LIMIT = 10;

export type OperatorConsolePrompt = {
  text(input: { label: string; defaultValue?: string }): Promise<string>;
  close?: () => Promise<void> | void;
};

type RunOperatorConsoleInput = {
  ingestionService: IngestionService;
  prompt: OperatorConsolePrompt;
  initialSeedFilePath?: string;
};

function readPromptValue(answer: string, fallback: string): string {
  return answer.trim().length === 0 ? fallback : answer.trim();
}

function readPositiveInteger(answer: string, fallback: number, label: string): number {
  const value = answer.trim().length === 0 ? String(fallback) : answer.trim();
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }

  return parsed;
}

async function readSeedFile(path: string) {
  const content = await readFile(path, "utf8");
  return parseSeedSourceRecords(JSON.parse(content));
}

export async function runOperatorConsole({
  ingestionService,
  prompt,
  initialSeedFilePath,
}: RunOperatorConsoleInput): Promise<string> {
  const output: string[] = [
    "Operator Console",
    "workflow Seed Sources -> Create Crawl Run -> Process Jobs -> Inspect Results",
    "",
    "Seed Sources",
  ];

  const seedFilePath = readPromptValue(
    await prompt.text({
      label: "Seed file path",
      defaultValue: initialSeedFilePath ?? DEFAULT_OPERATOR_CONSOLE_SEED_FILE,
    }),
    initialSeedFilePath ?? DEFAULT_OPERATOR_CONSOLE_SEED_FILE,
  );

  let records;
  try {
    records = await readSeedFile(seedFilePath);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`seed failed for ${seedFilePath}: ${message}`);
  }

  const seeded = await ingestionService.seedSources(records);
  output.push(`seed file ${seedFilePath}`);
  output.push(`seeded source ids ${seeded.sourceIds.join(", ")}`);
  output.push("");
  output.push("Create Crawl Run");

  const sourceId = readPromptValue(
    await prompt.text({
      label: "Source id",
      defaultValue: seeded.sourceIds[0],
    }),
    seeded.sourceIds[0],
  );
  const scope = readPromptValue(
    await prompt.text({
      label: "Scope",
      defaultValue: DEFAULT_SCOPE,
    }),
    DEFAULT_SCOPE,
  );
  const detailLimit = readPositiveInteger(
    await prompt.text({
      label: "Detail limit",
      defaultValue: String(DEFAULT_DETAIL_LIMIT),
    }),
    DEFAULT_DETAIL_LIMIT,
    "detail limit",
  );

  const createdRun = await ingestionService.createRun({
    runId: randomUUID(),
    sourceId,
    scope,
    detailLimit,
  });

  output.push(`active run ${createdRun.runId}`);
  output.push(`source ${createdRun.sourceId}`);
  output.push(`scope ${scope}`);
  output.push(`status ${createdRun.status}`);

  return output.join("\n");
}
