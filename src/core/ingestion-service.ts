import { randomUUID } from "node:crypto";

import { eq } from "drizzle-orm";

import type { Database } from "../db/setup.js";
import { crawlRuns, jobs, sources } from "../db/schema.js";

type CreateRunInput = {
  runId: string;
  sourceId: string;
  scope: string;
  fixtureId: string;
};

export class IngestionService {
  constructor(private readonly db: Database) {}

  async createRun(input: CreateRunInput) {
    await this.db
      .insert(sources)
      .values({
        id: input.sourceId,
        config: { fixtureId: input.fixtureId },
      })
      .onConflictDoUpdate({
        target: sources.id,
        set: { config: { fixtureId: input.fixtureId } },
      });

    await this.db.insert(crawlRuns).values({
      id: input.runId,
      sourceId: input.sourceId,
      scope: input.scope,
      status: "queued",
    });

    const jobId = randomUUID();
    await this.db.insert(jobs).values({
      id: jobId,
      crawlRunId: input.runId,
      kind: "fetch_raw_source_page",
      status: "queued",
      attempts: 0,
      maxAttempts: 3,
      scheduledAt: new Date(),
      availableAt: new Date(),
      payload: {
        sourceId: input.sourceId,
        fixtureId: input.fixtureId,
        requestUrl: `private://fixture/${input.fixtureId}`,
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
