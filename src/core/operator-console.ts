import { randomUUID } from "node:crypto";

import { JOB_STATUS } from "./ingestion.js";
import type { IngestionInspector } from "./inspector.js";
import type { IngestionService } from "./ingestion-service.js";
import type { Worker } from "./worker.js";
import { readSeedSourceFile, type SeedSourceRecord } from "./source-config.js";

export const DEFAULT_OPERATOR_CONSOLE_SEED_FILE = ".private/sources.json";
const DEFAULT_SCOPE = "default";
const DEFAULT_DETAIL_LIMIT = 10;
const DEFAULT_PROCESS_UNTIL_IDLE_CAP = 100;

export type OperatorConsolePrompt = {
  text(input: { label: string; defaultValue?: string }): Promise<string>;
  close?: () => Promise<void> | void;
};

type RunOperatorConsoleInput = {
  ingestionService: IngestionService;
  worker: Worker;
  inspector: IngestionInspector;
  prompt: OperatorConsolePrompt;
  initialSeedFilePath?: string;
};

type ProcessSummary = {
  completed: number;
  failed: number;
  retried: number;
  queued: number;
};

type ProcessResult = {
  lines: string[];
  summary: ProcessSummary;
};

type ProcessStatus =
  | typeof JOB_STATUS.completed
  | typeof JOB_STATUS.failed
  | typeof JOB_STATUS.queued;

type WorkerRunOnceResult = Awaited<ReturnType<Worker["runOnce"]>>;

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

function createEmptyProcessSummary(): ProcessSummary {
  return {
    completed: 0,
    failed: 0,
    retried: 0,
    queued: 0,
  };
}

function formatProcessSummary(summary: ProcessSummary): string {
  return `processing_summary completed=${summary.completed} failed=${summary.failed} retried=${summary.retried} queued=${summary.queued}`;
}

function tallyProcessStatus(summary: ProcessSummary, status: ProcessStatus) {
  switch (status) {
    case JOB_STATUS.failed:
      summary.failed += 1;
      break;
    case JOB_STATUS.queued:
      summary.retried += 1;
      break;
    case JOB_STATUS.completed:
      summary.completed += 1;
      break;
  }
}

async function finalizeProcessSummary(
  inspector: IngestionInspector,
  runId: string,
  summary: ProcessSummary,
): Promise<string> {
  const model = await inspector.inspectRunModel(runId);
  summary.queued = model.jobs.byStatus.queued;
  return formatProcessSummary(summary);
}

function readProcessStatus(result: WorkerRunOnceResult): ProcessStatus {
  switch (result.status) {
    case JOB_STATUS.failed:
      return JOB_STATUS.failed;
    case JOB_STATUS.queued:
      return JOB_STATUS.queued;
    default:
      return JOB_STATUS.completed;
  }
}

function createJobProcessLine(prefix: string, result: WorkerRunOnceResult): string {
  const status = readProcessStatus(result);
  return `${prefix} ${result.jobId} kind=${result.kind} status=${status}`;
}

async function processNextJob(worker: Worker): Promise<ProcessResult> {
  const result = await worker.runOnce();
  if (result.processed === 0) {
    return {
      lines: [],
      summary: createEmptyProcessSummary(),
    };
  }

  const summary = createEmptyProcessSummary();
  const status = readProcessStatus(result);
  tallyProcessStatus(summary, status);

  return {
    lines: [createJobProcessLine("process_next job", result)],
    summary,
  };
}

async function processUntilIdle(worker: Worker, cap: number): Promise<ProcessResult> {
  const summary = createEmptyProcessSummary();
  const lines = [`process_until_idle cap ${cap}`];
  let processedJobs = 0;

  while (processedJobs < cap) {
    const result = await worker.runOnce();
    if (result.processed === 0) {
      lines.push(`process_until_idle idle after ${processedJobs} jobs`);
      return { lines, summary };
    }

    processedJobs += 1;
    const status = readProcessStatus(result);
    lines.push(createJobProcessLine("process job", result));
    tallyProcessStatus(summary, status);
  }

  lines.push(`process_until_idle cap_reached after ${processedJobs} jobs`);
  return { lines, summary };
}

async function appendProcessOutput(
  output: string[],
  inspector: IngestionInspector,
  runId: string,
  result: ProcessResult,
) {
  output.push("");
  output.push("Process Jobs");
  output.push(...result.lines);
  output.push(await finalizeProcessSummary(inspector, runId, result.summary));
}

export async function runOperatorConsole({
  ingestionService,
  worker,
  inspector,
  prompt,
  initialSeedFilePath,
}: RunOperatorConsoleInput): Promise<string> {
  const output: string[] = [
    "Operator Console",
    "workflow Seed Sources -> Create Crawl Run -> Process Jobs -> Inspect Results",
    "",
    "Seed Sources",
  ];
  const defaultSeedFilePath = initialSeedFilePath ?? DEFAULT_OPERATOR_CONSOLE_SEED_FILE;

  const seedFilePath = readPromptValue(
    await prompt.text({
      label: "Seed file path",
      defaultValue: defaultSeedFilePath,
    }),
    defaultSeedFilePath,
  );

  let records: SeedSourceRecord[];
  try {
    records = await readSeedSourceFile(seedFilePath);
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

  let debugPrivate = false;
  while (true) {
    const action = readPromptValue(
      await prompt.text({
        label: "Action",
        defaultValue: "exit",
      }),
      "exit",
    );

    switch (action) {
      case "process-next": {
        const result = await processNextJob(worker);
        await appendProcessOutput(output, inspector, createdRun.runId, result);
        break;
      }
      case "process-until-idle": {
        const cap = readPositiveInteger(
          await prompt.text({
            label: "Process until idle cap",
            defaultValue: String(DEFAULT_PROCESS_UNTIL_IDLE_CAP),
          }),
          DEFAULT_PROCESS_UNTIL_IDLE_CAP,
          "process until idle cap",
        );
        const result = await processUntilIdle(worker, cap);
        await appendProcessOutput(output, inspector, createdRun.runId, result);
        break;
      }
      case "inspect": {
        output.push("");
        output.push("Inspect Results");
        output.push(
          await inspector.inspectRun(createdRun.runId, {
            debugPrivate,
          }),
        );
        break;
      }
      case "toggle-debug": {
        debugPrivate = !debugPrivate;
        output.push("");
        output.push(`private_debug ${debugPrivate ? "on" : "off"}`);
        break;
      }
      case "exit":
        return output.join("\n");
      default:
        throw new Error(`unknown operator-console action: ${action}`);
    }
  }
}
