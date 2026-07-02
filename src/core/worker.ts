import { createHash, randomUUID } from "node:crypto";

import { and, asc, eq, lte, sql } from "drizzle-orm";

import type { Database } from "../db/setup.js";
import { crawlRuns, jobs, rawSourcePages } from "../db/schema.js";
import {
  CRAWL_RUN_STATUS,
  type FetchRawSourcePagePayload,
  JOB_STATUS,
} from "./ingestion.js";
import type { CrawlProvider } from "./providers/crawl-provider.js";

function sha256(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

export class Worker {
  constructor(
    private readonly db: Database,
    private readonly crawlProvider: CrawlProvider,
  ) {}

  async runOnce() {
    const now = new Date();
    const [job] = await this.db
      .select()
      .from(jobs)
      .where(and(eq(jobs.status, JOB_STATUS.queued), lte(jobs.availableAt, now)))
      .orderBy(asc(jobs.scheduledAt))
      .limit(1);

    if (!job) {
      return { processed: 0 };
    }

    const lockToken = randomUUID();
    await this.db
      .update(jobs)
      .set({
        status: JOB_STATUS.running,
        attempts: sql`${jobs.attempts} + 1`,
        lockedAt: now,
        lockToken,
        updatedAt: now,
      })
      .where(eq(jobs.id, job.id));

    try {
      const payload = job.payload as FetchRawSourcePagePayload;
      const page = await this.crawlProvider.fetchPage({
        fixtureId: payload.fixtureId,
        requestUrl: payload.requestUrl,
      });

      await this.db.insert(rawSourcePages).values({
        id: randomUUID(),
        crawlRunId: job.crawlRunId,
        sourceId: payload.sourceId,
        jobId: job.id,
        normalizedUrl: page.normalizedUrl,
        urlHash: sha256(page.normalizedUrl),
        content: page.content,
        contentHash: sha256(page.content),
        providerPayload: page.providerPayload,
      });

      await this.db
        .update(jobs)
        .set({
          status: JOB_STATUS.completed,
          lockedAt: null,
          lockToken: null,
          errorPayload: null,
          updatedAt: new Date(),
        })
        .where(eq(jobs.id, job.id));

      await this.db
        .update(crawlRuns)
        .set({
          status: CRAWL_RUN_STATUS.completed,
          updatedAt: new Date(),
        })
        .where(eq(crawlRuns.id, job.crawlRunId));

      return { processed: 1, jobId: job.id, runId: job.crawlRunId };
    } catch (error) {
      const nextAttempt = job.attempts + 1;
      const shouldRetry = nextAttempt < job.maxAttempts;
      await this.db
        .update(jobs)
        .set({
          status: shouldRetry ? JOB_STATUS.queued : JOB_STATUS.failed,
          availableAt: new Date(Date.now() + nextAttempt * 1_000),
          lockedAt: null,
          lockToken: null,
          errorPayload: {
            message: error instanceof Error ? error.message : String(error),
          },
          updatedAt: new Date(),
        })
        .where(eq(jobs.id, job.id));

      await this.db
        .update(crawlRuns)
        .set({
          status: shouldRetry ? CRAWL_RUN_STATUS.queued : CRAWL_RUN_STATUS.failed,
          updatedAt: new Date(),
        })
        .where(eq(crawlRuns.id, job.crawlRunId));

      return {
        processed: 1,
        jobId: job.id,
        runId: job.crawlRunId,
        status: shouldRetry ? "queued" : "failed",
      };
    }
  }
}
