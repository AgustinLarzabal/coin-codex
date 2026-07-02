import { randomUUID } from "node:crypto";

import { and, desc, eq } from "drizzle-orm";

import type { Database } from "../db/setup.js";
import { crawlRuns, jobs, sources } from "../db/schema.js";
import {
  CRAWL_RUN_STATUS,
  clampDetailLimit,
  createCrawlCursor,
  type CrawlCursor,
  DEFAULT_JOB_MAX_ATTEMPTS,
  FETCH_RAW_SOURCE_PAGE_JOB_KIND,
  JOB_STATUS,
} from "./ingestion.js";
import { readStoredCursor } from "./page-processing.js";
import { parseSourceConfig, type SeedSourceRecord } from "./source-config.js";

type CreateRunInput = {
  runId: string;
  sourceId: string;
  scope: string;
  detailLimit: number;
};

export class IngestionService {
  constructor(private readonly db: Database) {}

  async seedSources(records: SeedSourceRecord[]) {
    for (const record of records) {
      await this.db
        .insert(sources)
        .values({
          id: record.id,
          config: record.config,
        })
        .onConflictDoUpdate({
          target: sources.id,
          set: { config: record.config },
        });
    }

    return {
      seeded: records.length,
      sourceIds: records.map((record) => record.id),
    };
  }

  async createRun(input: CreateRunInput) {
    const [source] = await this.db.select().from(sources).where(eq(sources.id, input.sourceId));
    if (!source) {
      throw new Error(`source not found: ${input.sourceId}`);
    }
    const sourceConfig = parseSourceConfig(source.config);
    const previousCursor = await this.readPreviousCursor(input.sourceId, input.scope);
    const detailLimit = clampDetailLimit(input.detailLimit);

    await this.db.insert(crawlRuns).values({
      id: input.runId,
      sourceId: input.sourceId,
      scope: input.scope,
      status: CRAWL_RUN_STATUS.queued,
      detailLimit,
      cursor: previousCursor,
    });

    const jobId = randomUUID();
    const scheduledAt = new Date();
    await this.db.insert(jobs).values({
      id: jobId,
      crawlRunId: input.runId,
      kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
      status: JOB_STATUS.queued,
      attempts: 0,
      maxAttempts: DEFAULT_JOB_MAX_ATTEMPTS,
      scheduledAt,
      availableAt: scheduledAt,
      payload: {
        sourceId: input.sourceId,
        fixtureId: sourceConfig.fixtureId,
        requestUrl: sourceConfig.startUrl,
        originalUrl: sourceConfig.startUrl,
        pageRole: "listing",
        detailLimit,
        cursor: previousCursor,
      },
    });

    const [run] = await this.db
      .select()
      .from(crawlRuns)
      .where(eq(crawlRuns.id, input.runId));

    return {
      runId: run.id,
      sourceId: run.sourceId,
      status: run.status,
      detailLimit: run.detailLimit,
      jobId,
    };
  }

  private async readPreviousCursor(sourceId: string, scope: string): Promise<CrawlCursor> {
    const priorRuns = await this.db
      .select({ cursor: crawlRuns.cursor })
      .from(crawlRuns)
      .where(and(eq(crawlRuns.sourceId, sourceId), eq(crawlRuns.scope, scope)))
      .orderBy(desc(crawlRuns.createdAt))
      .limit(10);

    return (
      priorRuns
        .map((run) => readStoredCursor(run.cursor))
        .find((cursor): cursor is CrawlCursor => cursor !== null) ?? createCrawlCursor()
    );
  }
}
