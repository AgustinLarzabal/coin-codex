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

type InspectRunOptions = {
  debugPrivate?: boolean;
};

export class IngestionInspector {
  constructor(private readonly db: Database) {}

  async inspectRun(runId: string, options: InspectRunOptions = {}): Promise<string> {
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
    const completedJobs = runJobs.filter((job) => job.status === JOB_STATUS.completed);
    const failedJobs = runJobs.filter((job) => job.status === JOB_STATUS.failed);
    const runningJobs = runJobs.filter((job) => job.status === JOB_STATUS.running);
    const queuedJobs = runJobs.filter((job) => job.status === JOB_STATUS.queued);
    const totalRetries = runJobs.reduce((sum, job) => sum + countRetries(job.attempts), 0);
    const pageTypeCounts = {
      listing: pages.filter((page) => page.pageType === RAW_PAGE_TYPE.listing).length,
      detail: pages.filter((page) => page.pageType === RAW_PAGE_TYPE.detail).length,
      unknown: pages.filter((page) => page.pageType === RAW_PAGE_TYPE.unknown).length,
    };
    const imageJobs = runJobs.filter((job) => job.kind === "download_accepted_coin_image");

    const lines = [
      `run ${run.id}`,
      `source ${run.sourceId}`,
      `status ${run.status}`,
      `jobs ${runJobs.length}`,
      `jobs total=${runJobs.length} completed=${completedJobs.length} failed=${failedJobs.length} retries=${totalRetries}`,
      `job_status queued=${queuedJobs.length} running=${runningJobs.length}`,
      `raw_pages ${pages.length}`,
      `raw_pages total=${pages.length} listing=${pageTypeCounts.listing} detail=${pageTypeCounts.detail} unknown=${pageTypeCounts.unknown}`,
      `candidates accepted=${acceptedCandidates.length} quarantined=${quarantinedCandidates.length}`,
      `accepted_coins ${accepted.length}`,
      `accepted_coin_images ${images.length}`,
      `image_jobs total=${imageJobs.length} completed=${imageJobs.filter((job) => job.status === JOB_STATUS.completed).length} failed=${imageJobs.filter((job) => job.status === JOB_STATUS.failed).length} retries=${imageJobs.reduce((sum, job) => sum + countRetries(job.attempts), 0)} stored=${images.length}`,
      `failures total=${failedJobs.length}`,
    ];
    const cursor = readStoredCursor(run.cursor);
    if (cursor) {
      lines.push(
        `cursor next_detail_index=${cursor.nextDetailIndex} total_detail_links=${cursor.totalDetailLinks}`,
      );
    }
    const debugPrivate = options.debugPrivate === true;
    const sourceConfig = source ? parseSourceConfig(source.config) : null;
    if (debugPrivate && sourceConfig) {
      if (sourceConfig.name) {
        lines.push(`source_name ${sourceConfig.name}`);
      }
      if (sourceConfig.domain) {
        lines.push(`source_domain ${sourceConfig.domain}`);
      }
      lines.push(`start_url ${sourceConfig.startUrl}`);
    }
    const pagesByJobId = new Map(pages.map((page) => [page.jobId, page]));
    const jobsByKind = new Map<
      string,
      Array<(typeof runJobs)[number]>
    >();

    for (const job of runJobs) {
      const jobsForKind = jobsByKind.get(job.kind) ?? [];
      jobsForKind.push(job);
      jobsByKind.set(job.kind, jobsForKind);
    }

    for (const [kind, jobsForKind] of jobsByKind) {
      lines.push(
        `job_kind ${kind} total=${jobsForKind.length} completed=${jobsForKind.filter((job) => job.status === JOB_STATUS.completed).length} failed=${jobsForKind.filter((job) => job.status === JOB_STATUS.failed).length} retries=${jobsForKind.reduce((sum, job) => sum + countRetries(job.attempts), 0)}`,
      );
    }

    for (const job of runJobs) {
      lines.push(
        `job ${job.id} ${job.kind} status=${job.status} attempts=${job.attempts} lock=${job.lockToken ?? "none"}`,
      );
      const page = pagesByJobId.get(job.id);
      if (page) {
        lines.push(
          `page ${page.id} page_type=${page.pageType} url_hash=${redactHash(page.urlHash)} content_hash=${redactHash(page.contentHash)}`,
        );
        if (debugPrivate) {
          lines.push(`url ${page.normalizedUrl}`);
          lines.push(`original_url ${page.originalUrl}`);
          const title = readPageTitle(page.content);
          if (title) {
            lines.push(`title ${title}`);
          }
        }
      }
      if (job.errorPayload) {
        const code = readErrorCode(job.errorPayload) ?? "unknown";
        const statusCode =
          typeof job.errorPayload.statusCode === "number"
            ? String(job.errorPayload.statusCode)
            : "unknown";
        const retryable =
          typeof job.errorPayload.retryable === "boolean"
            ? String(job.errorPayload.retryable)
            : "unknown";
        lines.push(
          `error job=${job.id} error_code=${code} status_code=${statusCode} retryable=${retryable}`,
        );
        if (debugPrivate) {
          lines.push(`error_private ${JSON.stringify(job.errorPayload)}`);
        }
      }
    }

    for (const candidate of quarantinedCandidates) {
      lines.push(
        `candidate ${candidate.id} status=quarantined reason=${candidate.quarantineReason}`,
      );
    }

    return lines.join("\n");
  }
}
