import { randomUUID } from "node:crypto";

import { JOB_STATUS } from "./ingestion.js";
import type { IngestionInspector } from "./inspector.js";
import type { IngestionService } from "./ingestion-service.js";
import { readSeedSourceFile, type SeedSourceRecord } from "./source-config.js";
import type { Worker } from "./worker.js";

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

type WorkerFailure = {
  jobId: string;
  kind: string;
  status: string;
  code: string | null;
};

type ProcessResult = {
  lines: string[];
  summary: ProcessSummary;
  processedJobs: number;
  stopReason: "single-step" | "idle" | "cap";
  lastProcessedResult: WorkerRunOnceResult | null;
  iterationFailures: WorkerFailure[];
};

type ProcessStatus =
  | typeof JOB_STATUS.completed
  | typeof JOB_STATUS.failed
  | typeof JOB_STATUS.queued;

type WorkerRunOnceResult = Awaited<ReturnType<Worker["runOnce"]>>;
type InspectionModel = Awaited<ReturnType<IngestionInspector["inspectRunModel"]>>;

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

function readWorkerResultErrorCode(result: WorkerRunOnceResult): string | null {
  return "errorCode" in result && typeof result.errorCode === "string"
    ? result.errorCode
    : null;
}

function createJobProcessLine(prefix: string, result: WorkerRunOnceResult): string {
  const status = readProcessStatus(result);
  return `${prefix} ${result.jobId} kind=${result.kind} status=${status}`;
}

function createProcessResult(input: {
  lines: string[];
  summary: ProcessSummary;
  processedJobs: number;
  stopReason: ProcessResult["stopReason"];
  lastProcessedResult: WorkerRunOnceResult | null;
  iterationFailures: WorkerFailure[];
}): ProcessResult {
  return input;
}

function collectIterationFailure(
  failures: WorkerFailure[],
  result: WorkerRunOnceResult,
) {
  if (result.processed === 0 || !("jobId" in result) || !("kind" in result)) {
    return;
  }

  const status = readProcessStatus(result);
  if (status === JOB_STATUS.completed) {
    return;
  }

  const jobId = result.jobId;
  const kind = result.kind;
  if (!jobId || !kind) {
    return;
  }

  failures.push({
    jobId,
    kind,
    status,
    code: readWorkerResultErrorCode(result),
  });
}

function buildVisibleFailures(
  iterationFailures: WorkerFailure[],
  inspectionFailures: InspectionModel["jobs"]["details"],
): WorkerFailure[] {
  const failures = new Map<string, WorkerFailure>();

  for (const failure of iterationFailures) {
    failures.set(`${failure.jobId}:${failure.status}`, failure);
  }

  for (const job of inspectionFailures) {
    if (job.status !== JOB_STATUS.failed) {
      continue;
    }

    failures.set(`${job.id}:${JOB_STATUS.failed}`, {
      jobId: job.id,
      kind: job.kind,
      status: job.status,
      code: job.error?.code ?? null,
    });
  }

  return [...failures.values()];
}

async function processNextJob(worker: Worker): Promise<ProcessResult> {
  const result = await worker.runOnce();
  if (result.processed === 0) {
    return createProcessResult({
      lines: [],
      summary: createEmptyProcessSummary(),
      processedJobs: 0,
      stopReason: "idle",
      lastProcessedResult: null,
      iterationFailures: [],
    });
  }

  const summary = createEmptyProcessSummary();
  const iterationFailures: WorkerFailure[] = [];
  const status = readProcessStatus(result);
  tallyProcessStatus(summary, status);
  collectIterationFailure(iterationFailures, result);

  return createProcessResult({
    lines: [createJobProcessLine("process_next job", result)],
    summary,
    processedJobs: 1,
    stopReason: "single-step",
    lastProcessedResult: result,
    iterationFailures,
  });
}

async function processUntilIdle(worker: Worker, cap: number): Promise<ProcessResult> {
  const summary = createEmptyProcessSummary();
  const lines = [`process_until_idle cap ${cap}`];
  const iterationFailures: WorkerFailure[] = [];
  let processedJobs = 0;
  let lastProcessedResult: WorkerRunOnceResult | null = null;

  while (processedJobs < cap) {
    const result = await worker.runOnce();
    if (result.processed === 0) {
      lines.push(`process_until_idle idle after ${processedJobs} jobs`);
      return createProcessResult({
        lines,
        summary,
        processedJobs,
        stopReason: "idle",
        lastProcessedResult,
        iterationFailures,
      });
    }

    processedJobs += 1;
    lastProcessedResult = result;
    const status = readProcessStatus(result);
    lines.push(createJobProcessLine("process job", result));
    tallyProcessStatus(summary, status);
    collectIterationFailure(iterationFailures, result);
  }

  lines.push(`process_until_idle cap_reached after ${processedJobs} jobs`);
  return createProcessResult({
    lines,
    summary,
    processedJobs,
    stopReason: "cap",
    lastProcessedResult,
    iterationFailures,
  });
}

async function appendProcessOutput(
  output: string[],
  inspector: IngestionInspector,
  runId: string,
  action: string,
  result: ProcessResult,
) {
  const model = await inspector.inspectRunModel(runId);
  result.summary.queued = model.jobs.byStatus.queued;
  const visibleFailures = buildVisibleFailures(
    result.iterationFailures,
    model.jobs.details,
  );

  output.push("");
  output.push("Process Jobs");
  output.push(`action ${action}`);
  output.push(...result.lines);
  output.push(formatProcessSummary(result.summary));
  output.push(`processed jobs ${result.processedJobs}`);
  output.push(`stop reason ${result.stopReason}`);
  if (result.lastProcessedResult?.processed === 1) {
    output.push(
      `last job ${result.lastProcessedResult.kind} status=${readProcessStatus(
        result.lastProcessedResult,
      )}`,
    );
  }
  output.push(
    `jobs completed=${model.jobs.summary.completed} failed=${model.jobs.summary.failed} queued=${model.jobs.summary.queued} retries=${model.jobs.summary.retries}`,
  );
  output.push(`visible failures ${visibleFailures.length}`);
  for (const failure of visibleFailures) {
    const codeSuffix = failure.code ? ` code=${failure.code}` : "";
    output.push(
      `failure job=${failure.jobId} kind=${failure.kind} status=${failure.status}${codeSuffix}`,
    );
  }
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
      case "process-next":
      case "process-next-job": {
        const result = await processNextJob(worker);
        await appendProcessOutput(output, inspector, createdRun.runId, action, result);
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
        await appendProcessOutput(output, inspector, createdRun.runId, action, result);
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
