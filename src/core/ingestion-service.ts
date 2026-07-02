import { randomUUID } from "node:crypto";

import { eq } from "drizzle-orm";

import type { Database } from "../db/setup.js";
import { crawlRuns, jobs, sources } from "../db/schema.js";
import {
  buildFixtureRequestUrl,
  CRAWL_RUN_STATUS,
  DEFAULT_JOB_MAX_ATTEMPTS,
  FETCH_RAW_SOURCE_PAGE_JOB_KIND,
  JOB_STATUS,
} from "./ingestion.js";

type CreateRunInput = {
  runId: string;
  sourceId: string;
  scope: string;
  fixtureId: string;
};

export class IngestionService {
  constructor(private readonly db: Database) {}

  async createRun(input: CreateRunInput) {
    const sourceConfig = { fixtureId: input.fixtureId };

    await this.db
      .insert(sources)
      .values({
        id: input.sourceId,
        config: sourceConfig,
      })
      .onConflictDoUpdate({
        target: sources.id,
        set: { config: sourceConfig },
      });

    await this.db.insert(crawlRuns).values({
      id: input.runId,
      sourceId: input.sourceId,
      scope: input.scope,
      status: CRAWL_RUN_STATUS.queued,
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
        fixtureId: input.fixtureId,
        requestUrl: buildFixtureRequestUrl(input.fixtureId),
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
      jobId,
    };
  }
}
