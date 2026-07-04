import { randomUUID } from "node:crypto";

import { JOB_STATUS } from "./ingestion.js";
import type { CrawlRunInspectionModel, IngestionInspector } from "./inspector.js";
import type { IngestionService } from "./ingestion-service.js";
import { readSeedSourceFile, type SeedSourceRecord } from "./source-config.js";
import type { Worker, WorkerRunResult } from "./worker.js";

export const DEFAULT_OPERATOR_CONSOLE_SEED_FILE = ".private/sources.json";
const DEFAULT_SCOPE = "default";
const DEFAULT_DETAIL_LIMIT = 10;
const DEFAULT_PROCESS_UNTIL_IDLE_CAP = 100;
const ACTION_OPTIONS = [
  "process-next-job",
  "process-until-idle",
  "inspect",
  "toggle-debug",
  "exit",
] as const;

type OperatorConsoleAction = "process-next" | "process-next-job" | "process-until-idle";
type OperatorConsoleStopReason = "single-step" | "cap" | "idle";
type InspectionFailure =
  Awaited<ReturnType<IngestionInspector["inspectRunModel"]>>["jobs"]["details"][number];

export type OperatorConsolePromptInput = {
  label: string;
  defaultValue?: string;
  options?: readonly string[];
};

export type OperatorConsolePrompt = {
  text(input: OperatorConsolePromptInput): Promise<string>;
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
  stopReason: OperatorConsoleStopReason;
  lastProcessedResult: WorkerRunResult | null;
  iterationFailures: WorkerFailure[];
};

type ProcessStatus =
  | typeof JOB_STATUS.completed
  | typeof JOB_STATUS.failed
  | typeof JOB_STATUS.queued;
type ProcessedWorkerRunResult = Extract<WorkerRunResult, { processed: 1 }>;

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

function readWorkerResultStatus(result: WorkerRunResult): ProcessStatus {
  const status = "status" in result ? result.status : undefined;
  switch (status) {
    case JOB_STATUS.failed:
      return JOB_STATUS.failed;
    case JOB_STATUS.queued:
      return JOB_STATUS.queued;
    default:
      return JOB_STATUS.completed;
  }
}

function readWorkerResultErrorCode(result: WorkerRunResult): string | null {
  if ("errorCode" in result && typeof result.errorCode === "string") {
    return result.errorCode;
  }

  return null;
}

function readProcessUntilIdleCap(
  action: OperatorConsoleAction,
  answer: string | null,
): number {
  if (action === "process-next" || action === "process-next-job") {
    return 1;
  }

  return readPositiveInteger(
    answer ?? String(DEFAULT_PROCESS_UNTIL_IDLE_CAP),
    DEFAULT_PROCESS_UNTIL_IDLE_CAP,
    "process until idle cap",
  );
}

function buildFailureKey(jobId: string, status: string): string {
  return `${jobId}:${status}`;
}

function readVisibleFailure(job: InspectionFailure): WorkerFailure | null {
  if (job.status !== JOB_STATUS.failed) {
    return null;
  }

  return {
    jobId: job.id,
    kind: job.kind,
    status: job.status,
    code: job.error?.code ?? null,
  };
}

function buildVisibleFailures(
  iterationFailures: WorkerFailure[],
  inspectionFailures: InspectionFailure[],
): WorkerFailure[] {
  const failures = new Map<string, WorkerFailure>();

  for (const failure of iterationFailures) {
    failures.set(buildFailureKey(failure.jobId, failure.status), failure);
  }

  for (const job of inspectionFailures) {
    const failure = readVisibleFailure(job);
    if (!failure) {
      continue;
    }

    failures.set(buildFailureKey(failure.jobId, failure.status), failure);
  }

  return [...failures.values()];
}

function appendSourcePrivateLines(
  lines: string[],
  sourcePrivate: CrawlRunInspectionModel["source"]["private"],
): void {
  if (!sourcePrivate) {
    return;
  }

  if (sourcePrivate.name) {
    lines.push(`source_name ${sourcePrivate.name}`);
  }
  if (sourcePrivate.domain) {
    lines.push(`source_domain ${sourcePrivate.domain}`);
  }
  lines.push(`start_url ${sourcePrivate.startUrl}`);
}

function appendJobPrivateLines(
  lines: string[],
  jobs: CrawlRunInspectionModel["jobs"]["details"],
): void {
  for (const job of jobs) {
    if (job.page?.private?.title) {
      lines.push(`title ${job.page.private.title}`);
    }
    if (job.page?.private?.normalizedUrl) {
      lines.push(`url ${job.page.private.normalizedUrl}`);
    }
    if (job.error?.private) {
      lines.push(`error_private ${JSON.stringify(job.error.private)}`);
    }
  }
}

function renderOperatorConsoleInspection(
  model: CrawlRunInspectionModel,
  debugPrivate: boolean,
): string[] {
  const lines = [
    "Inspect Active Run",
    `private debug ${debugPrivate ? "on" : "off"}`,
    `run ${model.run.id}`,
    `source ${model.run.sourceId}`,
    `status ${model.run.status}`,
    `jobs total=${model.jobs.total} completed=${model.jobs.summary.completed} failed=${model.jobs.summary.failed} queued=${model.jobs.summary.queued} running=${model.jobs.summary.running} retries=${model.jobs.summary.retries}`,
    `job_status queued=${model.jobs.byStatus.queued} running=${model.jobs.byStatus.running}`,
    `raw_pages total=${model.rawPages.total} listing=${model.rawPages.byType.listing} detail=${model.rawPages.byType.detail} unknown=${model.rawPages.byType.unknown}`,
    `candidates total=${model.candidates.total} accepted=${model.candidates.accepted} quarantined=${model.candidates.quarantined}`,
    `accepted_coins ${model.acceptedCoins.total}`,
    `quarantined_candidates ${model.candidates.quarantined}`,
    `accepted_coin_images ${model.acceptedCoinImages.total}`,
    `failures total=${model.jobs.failureCount}`,
  ];

  if (model.cursor) {
    lines.push(
      `cursor next_detail_index=${model.cursor.nextDetailIndex} total_detail_links=${model.cursor.totalDetailLinks}`,
    );
  }

  appendSourcePrivateLines(lines, model.source.private);

  for (const candidate of model.quarantinedCandidates) {
    lines.push(`quarantine_reason ${candidate.reason ?? "unknown"}`);
  }

  if (debugPrivate) {
    appendJobPrivateLines(lines, model.jobs.details);
  }

  return lines;
}

function createJobProcessLine(
  prefix: string,
  result: ProcessedWorkerRunResult,
): string {
  const status = readWorkerResultStatus(result);
  return `${prefix} ${result.jobId} kind=${result.kind} status=${status}`;
}

function collectIterationFailure(
  failures: WorkerFailure[],
  result: WorkerRunResult,
) {
  if (result.processed === 0) {
    return;
  }

  const status = readWorkerResultStatus(result);
  if (status === JOB_STATUS.completed) {
    return;
  }

  failures.push({
    jobId: result.jobId,
    kind: result.kind,
    status,
    code: readWorkerResultErrorCode(result),
  });
}

async function runWorkerAction(
  action: OperatorConsoleAction,
  worker: Worker,
  capAnswer: string | null,
): Promise<ProcessResult> {
  const cap = readProcessUntilIdleCap(action, capAnswer);
  const summary = createEmptyProcessSummary();
  const lines =
    action === "process-until-idle" ? [`process_until_idle cap ${cap}`] : [];
  const iterationFailures: WorkerFailure[] = [];
  let processedJobs = 0;
  let stopReason: OperatorConsoleStopReason =
    action === "process-until-idle" ? "cap" : "single-step";
  let lastProcessedResult: WorkerRunResult | null = null;

  for (let index = 0; index < cap; index += 1) {
    const result = await worker.runOnce();

    if (result.processed === 0) {
      stopReason = "idle";
      if (action === "process-until-idle") {
        lines.push(`process_until_idle idle after ${processedJobs} jobs`);
      }
      break;
    }

    processedJobs += 1;
    lastProcessedResult = result;
    const status = readWorkerResultStatus(result);
    tallyProcessStatus(summary, status);
    collectIterationFailure(iterationFailures, result);
    lines.push(
      createJobProcessLine(
        action === "process-until-idle" ? "process job" : "process_next job",
        result,
      ),
    );

    if (action === "process-next" || action === "process-next-job") {
      break;
    }
  }

  if (action === "process-until-idle" && stopReason === "cap") {
    lines.push(`process_until_idle cap_reached after ${processedJobs} jobs`);
  }

  return {
    lines,
    summary,
    processedJobs,
    stopReason,
    lastProcessedResult,
    iterationFailures,
  };
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
      `last job ${result.lastProcessedResult.kind} status=${readWorkerResultStatus(
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
        options: ACTION_OPTIONS,
      }),
      "exit",
    );

    switch (action) {
      case "process-next":
      case "process-next-job": {
        const result = await runWorkerAction(action, worker, null);
        await appendProcessOutput(output, inspector, createdRun.runId, action, result);
        break;
      }
      case "process-until-idle": {
        const capAnswer = await prompt.text({
          label: "Process until idle cap",
          defaultValue: String(DEFAULT_PROCESS_UNTIL_IDLE_CAP),
        });
        const result = await runWorkerAction(action, worker, capAnswer);
        await appendProcessOutput(output, inspector, createdRun.runId, action, result);
        break;
      }
      case "inspect": {
        const model = await inspector.inspectRunModel(createdRun.runId, {
          debugPrivate,
        });
        output.push("");
        output.push("Inspect Results");
        output.push(...renderOperatorConsoleInspection(model, debugPrivate));
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
