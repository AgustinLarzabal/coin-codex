import { asc, eq } from "drizzle-orm";

import type { Database } from "../db/setup.js";
import {
  acceptedCoinImages,
  acceptedCoins,
  coinCandidates,
  crawlRuns,
  jobs,
  rawSourcePages,
  sources,
} from "../db/schema.js";
import {
  DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
  COIN_CANDIDATE_STATUS,
  JOB_STATUS,
  RAW_PAGE_TYPE,
} from "./ingestion.js";
import { readStoredCursor } from "./page-processing.js";
import { parseSourceConfig } from "./source-config.js";

function redactHash(value: string): string {
  return value.slice(0, 12);
}

function readPageTitle(content: string): string | undefined {
  const titleMatch = content.match(/<h1>(.*?)<\/h1>/i);
  return titleMatch?.[1];
}

function countRetries(attempts: number): number {
  return Math.max(attempts - 1, 0);
}

function readErrorCode(
  errorPayload: Record<string, unknown> | null,
): string | null {
  return typeof errorPayload?.code === "string" ? errorPayload.code : null;
}

type JobRecord = typeof jobs.$inferSelect;
type PageRecord = typeof rawSourcePages.$inferSelect;

export type StatusSummary = {
  completed: number;
  failed: number;
  queued: number;
  running: number;
  retries: number;
  total: number;
};

export type CrawlRunInspectionModel = {
  run: {
    id: string;
    sourceId: string;
    status: string;
  };
  source: {
    id: string;
    private?: {
      name?: string;
      domain?: string;
      startUrl: string;
    };
  };
  jobs: {
    total: number;
    summary: StatusSummary;
    byStatus: {
      queued: number;
      running: number;
    };
    failureCount: number;
    details: InspectionJobDetail[];
  };
  rawPages: {
    total: number;
    byType: {
      detail: number;
      listing: number;
      unknown: number;
    };
  };
  candidates: {
    total: number;
    accepted: number;
    quarantined: number;
  };
  acceptedCoins: {
    total: number;
  };
  acceptedCoinImages: {
    total: number;
  };
  imageJobs: {
    total: number;
    stored: number;
    summary: StatusSummary;
  };
  cursor: {
    nextDetailIndex: number;
    totalDetailLinks: number;
  } | null;
  jobKinds: Array<{
    kind: string;
    summary: StatusSummary;
  }>;
  quarantinedCandidates: Array<{
    id: string;
    reason: string | null;
  }>;
};

export type InspectionJobDetail = {
  id: string;
  kind: string;
  status: string;
  attempts: number;
  lockToken: string | null;
  page?: {
    id: string;
    pageType: string;
    urlHash: string;
    contentHash: string;
    private?: {
      normalizedUrl: string;
      originalUrl: string;
      title?: string;
    };
  };
  error?: {
    code: string;
    retryable: boolean | null;
    statusCode: number | null;
    private?: Record<string, unknown>;
  };
};

function readOptionalBoolean(
  value: Record<string, unknown> | null,
  key: string,
): boolean | null {
  const field = value?.[key];
  return typeof field === "boolean" ? field : null;
}

function readOptionalNumber(
  value: Record<string, unknown> | null,
  key: string,
): number | null {
  const field = value?.[key];
  return typeof field === "number" ? field : null;
}

function summarizeJobs(jobsToSummarize: JobRecord[]): StatusSummary {
  const summary: StatusSummary = {
    completed: 0,
    failed: 0,
    queued: 0,
    running: 0,
    retries: 0,
    total: jobsToSummarize.length,
  };

  for (const job of jobsToSummarize) {
    summary.retries += countRetries(job.attempts);
    switch (job.status) {
      case JOB_STATUS.completed:
        summary.completed += 1;
        break;
      case JOB_STATUS.failed:
        summary.failed += 1;
        break;
      case JOB_STATUS.queued:
        summary.queued += 1;
        break;
      case JOB_STATUS.running:
        summary.running += 1;
        break;
      default:
        break;
    }
  }

  return summary;
}

function summarizePagesByType(pages: PageRecord[]) {
  const summary = {
    detail: 0,
    listing: 0,
    unknown: 0,
  };

  for (const page of pages) {
    switch (page.pageType) {
      case RAW_PAGE_TYPE.detail:
        summary.detail += 1;
        break;
      case RAW_PAGE_TYPE.listing:
        summary.listing += 1;
        break;
      case RAW_PAGE_TYPE.unknown:
        summary.unknown += 1;
        break;
      default:
        break;
    }
  }

  return summary;
}

function formatJobSummary(prefix: string, summary: StatusSummary): string {
  return `${prefix} total=${summary.total} completed=${summary.completed} failed=${summary.failed} retries=${summary.retries}`;
}

function buildSourcePrivate(
  sourceConfig: ReturnType<typeof parseSourceConfig> | null,
  debugPrivate: boolean,
): CrawlRunInspectionModel["source"]["private"] {
  if (!debugPrivate || !sourceConfig) {
    return undefined;
  }

  return {
    name: sourceConfig.name,
    domain: sourceConfig.domain,
    startUrl: sourceConfig.startUrl,
  };
}

function buildPageDetail(
  page: PageRecord | undefined,
  debugPrivate: boolean,
): InspectionJobDetail["page"] {
  if (!page) {
    return undefined;
  }

  return {
    id: page.id,
    pageType: page.pageType,
    urlHash: redactHash(page.urlHash),
    contentHash: redactHash(page.contentHash),
    private: debugPrivate
      ? {
          normalizedUrl: page.normalizedUrl,
          originalUrl: page.originalUrl,
          title: readPageTitle(page.content),
        }
      : undefined,
  };
}

function buildErrorDetail(
  errorPayload: Record<string, unknown> | null,
  debugPrivate: boolean,
): InspectionJobDetail["error"] {
  if (!errorPayload) {
    return undefined;
  }

  return {
    code: readErrorCode(errorPayload) ?? "unknown",
    retryable: readOptionalBoolean(errorPayload, "retryable"),
    statusCode: readOptionalNumber(errorPayload, "statusCode"),
    private: debugPrivate ? errorPayload : undefined,
  };
}

function buildCursorSummary(
  storedCursor: unknown,
): CrawlRunInspectionModel["cursor"] {
  const cursor = readStoredCursor(storedCursor);
  if (!cursor) {
    return null;
  }

  return {
    nextDetailIndex: cursor.nextDetailIndex,
    totalDetailLinks: cursor.totalDetailLinks,
  };
}

function buildJobKindSummaries(
  runJobs: JobRecord[],
): CrawlRunInspectionModel["jobKinds"] {
  const jobsByKind = new Map<string, JobRecord[]>();

  for (const job of runJobs) {
    const jobsForKind = jobsByKind.get(job.kind) ?? [];
    jobsForKind.push(job);
    jobsByKind.set(job.kind, jobsForKind);
  }

  return [...jobsByKind.entries()].map(([kind, jobsForKind]) => ({
    kind,
    summary: summarizeJobs(jobsForKind),
  }));
}

function renderInspectionText(model: CrawlRunInspectionModel): string {
  const lines = [
    `run ${model.run.id}`,
    `source ${model.run.sourceId}`,
    `status ${model.run.status}`,
    `jobs ${model.jobs.total}`,
    formatJobSummary("jobs", model.jobs.summary),
    `job_status queued=${model.jobs.byStatus.queued} running=${model.jobs.byStatus.running}`,
    `raw_pages ${model.rawPages.total}`,
    `raw_pages total=${model.rawPages.total} listing=${model.rawPages.byType.listing} detail=${model.rawPages.byType.detail} unknown=${model.rawPages.byType.unknown}`,
    `candidates accepted=${model.candidates.accepted} quarantined=${model.candidates.quarantined}`,
    `accepted_coins ${model.acceptedCoins.total}`,
    `accepted_coin_images ${model.acceptedCoinImages.total}`,
    `${formatJobSummary("image_jobs", model.imageJobs.summary)} stored=${model.imageJobs.stored}`,
    `failures total=${model.jobs.failureCount}`,
  ];

  if (model.cursor) {
    lines.push(
      `cursor next_detail_index=${model.cursor.nextDetailIndex} total_detail_links=${model.cursor.totalDetailLinks}`,
    );
  }

  if (model.source.private) {
    if (model.source.private.name) {
      lines.push(`source_name ${model.source.private.name}`);
    }
    if (model.source.private.domain) {
      lines.push(`source_domain ${model.source.private.domain}`);
    }
    lines.push(`start_url ${model.source.private.startUrl}`);
  }

  for (const jobKind of model.jobKinds) {
    lines.push(formatJobSummary(`job_kind ${jobKind.kind}`, jobKind.summary));
  }

  for (const job of model.jobs.details) {
    lines.push(
      `job ${job.id} ${job.kind} status=${job.status} attempts=${job.attempts} lock=${job.lockToken ?? "none"}`,
    );
    if (job.page) {
      lines.push(
        `page ${job.page.id} page_type=${job.page.pageType} url_hash=${job.page.urlHash} content_hash=${job.page.contentHash}`,
      );
      if (job.page.private) {
        lines.push(`url ${job.page.private.normalizedUrl}`);
        lines.push(`original_url ${job.page.private.originalUrl}`);
        if (job.page.private.title) {
          lines.push(`title ${job.page.private.title}`);
        }
      }
    }
    if (job.error) {
      lines.push(
        `error job=${job.id} error_code=${job.error.code} status_code=${job.error.statusCode ?? "unknown"} retryable=${job.error.retryable ?? "unknown"}`,
      );
      if (job.error.private) {
        lines.push(`error_private ${JSON.stringify(job.error.private)}`);
      }
    }
  }

  for (const candidate of model.quarantinedCandidates) {
    lines.push(`candidate ${candidate.id} status=quarantined reason=${candidate.reason}`);
  }

  return lines.join("\n");
}

type InspectRunOptions = {
  debugPrivate?: boolean;
};

export class IngestionInspector {
  constructor(private readonly db: Database) {}

  async inspectRunModel(
    runId: string,
    options: InspectRunOptions = {},
  ): Promise<CrawlRunInspectionModel> {
    const [run] = await this.db.select().from(crawlRuns).where(eq(crawlRuns.id, runId));
    if (!run) {
      throw new Error(`run not found: ${runId}`);
    }
    const [source] = await this.db.select().from(sources).where(eq(sources.id, run.sourceId));

    const runJobs = await this.db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, runId))
      .orderBy(asc(jobs.createdAt));

    const pages = await this.db
      .select()
      .from(rawSourcePages)
      .where(eq(rawSourcePages.crawlRunId, runId))
      .orderBy(asc(rawSourcePages.fetchedAt));
    const candidates = await this.db
      .select()
      .from(coinCandidates)
      .where(eq(coinCandidates.crawlRunId, runId))
      .orderBy(asc(coinCandidates.createdAt));
    const accepted = await this.db
      .select()
      .from(acceptedCoins)
      .where(eq(acceptedCoins.crawlRunId, runId))
      .orderBy(asc(acceptedCoins.createdAt));
    const images = await this.db
      .select()
      .from(acceptedCoinImages)
      .where(eq(acceptedCoinImages.crawlRunId, runId))
      .orderBy(asc(acceptedCoinImages.createdAt));

    const acceptedCandidates = candidates.filter(
      (candidate) => candidate.status === COIN_CANDIDATE_STATUS.accepted,
    );
    const quarantinedCandidates = candidates.filter(
      (candidate) => candidate.status === COIN_CANDIDATE_STATUS.quarantined,
    );
    const jobSummary = summarizeJobs(runJobs);
    const pageTypeCounts = summarizePagesByType(pages);
    const imageJobs = runJobs.filter(
      (job) => job.kind === DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
    );
    const imageJobSummary = summarizeJobs(imageJobs);
    const pagesByJobId = new Map(pages.map((page) => [page.jobId, page]));
    const debugPrivate = options.debugPrivate === true;
    const sourceConfig = source ? parseSourceConfig(source.config) : null;
    const jobKinds = buildJobKindSummaries(runJobs);

    return {
      run: {
        id: run.id,
        sourceId: run.sourceId,
        status: run.status,
      },
      source: {
        id: run.sourceId,
        private: buildSourcePrivate(sourceConfig, debugPrivate),
      },
      jobs: {
        total: runJobs.length,
        summary: jobSummary,
        byStatus: {
          queued: jobSummary.queued,
          running: jobSummary.running,
        },
        failureCount: jobSummary.failed,
        details: runJobs.map((job) => {
          const page = pagesByJobId.get(job.id);

          return {
            id: job.id,
            kind: job.kind,
            status: job.status,
            attempts: job.attempts,
            lockToken: job.lockToken,
            page: buildPageDetail(page, debugPrivate),
            error: buildErrorDetail(job.errorPayload, debugPrivate),
          };
        }),
      },
      rawPages: {
        total: pages.length,
        byType: pageTypeCounts,
      },
      candidates: {
        total: candidates.length,
        accepted: acceptedCandidates.length,
        quarantined: quarantinedCandidates.length,
      },
      acceptedCoins: {
        total: accepted.length,
      },
      acceptedCoinImages: {
        total: images.length,
      },
      imageJobs: {
        total: imageJobs.length,
        stored: images.length,
        summary: imageJobSummary,
      },
      cursor: buildCursorSummary(run.cursor),
      jobKinds,
      quarantinedCandidates: quarantinedCandidates.map((candidate) => ({
        id: candidate.id,
        reason: candidate.quarantineReason,
      })),
    };
  }

  async inspectRun(runId: string, options: InspectRunOptions = {}): Promise<string> {
    const model = await this.inspectRunModel(runId, options);
    return renderInspectionText(model);
  }
}
