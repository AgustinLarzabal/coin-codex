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
import { COIN_CANDIDATE_STATUS } from "./ingestion.js";
import { parseSourceConfig } from "./source-config.js";

function redactHash(value: string): string {
  return value.slice(0, 12);
}

function readPageTitle(content: string): string | undefined {
  const titleMatch = content.match(/<h1>(.*?)<\/h1>/i);
  return titleMatch?.[1];
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

    const lines = [
      `run ${run.id}`,
      `source ${run.sourceId}`,
      `status ${run.status}`,
      `jobs ${runJobs.length}`,
      `raw_pages ${pages.length}`,
      `candidates accepted=${acceptedCandidates.length} quarantined=${quarantinedCandidates.length}`,
      `accepted_coins ${accepted.length}`,
      `accepted_coin_images ${images.length}`,
    ];
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

    for (const job of runJobs) {
      lines.push(
        `job ${job.id} ${job.kind} status=${job.status} attempts=${job.attempts} lock=${job.lockToken ?? "none"}`,
      );
      const page = pagesByJobId.get(job.id);
      if (page) {
        lines.push(
          `page ${page.id} url_hash=${redactHash(page.urlHash)} content_hash=${redactHash(page.contentHash)}`,
        );
        if (debugPrivate) {
          lines.push(`url ${page.normalizedUrl}`);
          const title = readPageTitle(page.content);
          if (title) {
            lines.push(`title ${title}`);
          }
        }
      }
      if (job.errorPayload) {
        lines.push(`error ${JSON.stringify(job.errorPayload)}`);
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
