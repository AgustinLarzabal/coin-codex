import { asc, eq } from "drizzle-orm";

import type { Database } from "../db/setup.js";
import { crawlRuns, jobs, rawSourcePages, sources } from "../db/schema.js";
import { parseSourceConfig } from "./source-config.js";

function redactHash(value: string): string {
  return value.slice(0, 12);
}

export class IngestionInspector {
  constructor(private readonly db: Database) {}

  async inspectRun(runId: string, options: { debugPrivate?: boolean } = {}): Promise<string> {
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

    const lines = [
      `run ${run.id}`,
      `source ${run.sourceId}`,
      `status ${run.status}`,
      `jobs ${runJobs.length}`,
      `raw_pages ${pages.length}`,
    ];
    const sourceConfig = source ? parseSourceConfig(source.config) : null;
    if (options.debugPrivate && sourceConfig) {
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
        lines.push(`page ${page.id} url_hash=${redactHash(page.urlHash)} content_hash=${redactHash(page.contentHash)}`);
        if (options.debugPrivate) {
          lines.push(`url ${page.normalizedUrl}`);
          const titleMatch = page.content.match(/<h1>(.*?)<\/h1>/i);
          if (titleMatch) {
            lines.push(`title ${titleMatch[1]}`);
          }
        }
      }
      if (job.errorPayload) {
        lines.push(`error ${JSON.stringify(job.errorPayload)}`);
      }
    }

    return lines.join("\n");
  }
}
