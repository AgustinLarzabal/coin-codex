import { randomUUID } from "node:crypto";

import { PGlite } from "@electric-sql/pglite";
import { eq } from "drizzle-orm";
import { afterEach, describe, expect, it } from "vitest";

import { executeCli } from "../src/cli.js";
import { migrate } from "../src/db/migrate.js";
import { createDatabase, registerDatabase, unregisterDatabase } from "../src/db/setup.js";
import { jobs, rawSourcePages } from "../src/db/schema.js";

const resources: Array<{ databaseUrl: string; close: () => Promise<void> }> = [];

async function createDatabaseUrl() {
  const databaseUrl = `memory://${randomUUID()}`;
  const client = new PGlite();
  registerDatabase(databaseUrl, client);
  resources.push({ databaseUrl, close: () => client.close() });
  await migrate(client);
  const db = createDatabase(client);
  return { databaseUrl, db };
}

afterEach(async () => {
  await Promise.all(
    resources.splice(0).map(async (resource) => {
      unregisterDatabase(resource.databaseUrl);
      await resource.close();
    }),
  );
});

describe("CLI ingestion skeleton", () => {
  it("creates a crawl run, processes a fake fetch job, and inspects it without leaking private URLs", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const runId = randomUUID();

    const createOutput = await executeCli(
      [
        "create-run",
        "--run-id",
        runId,
        "--source-id",
        "src_test_opaque",
        "--scope",
        "issuer_scope",
        "--fixture",
        "fixture-coin",
      ],
      { databaseUrl },
    );

    expect(JSON.parse(createOutput)).toMatchObject({
      runId,
      sourceId: "src_test_opaque",
      status: "queued",
    });

    const workerOutput = await executeCli(["run-worker"], { databaseUrl });
    expect(JSON.parse(workerOutput)).toMatchObject({
      processed: 1,
      runId,
    });

    const inspectOutput = await executeCli(
      ["inspect-run", "--run-id", runId],
      { databaseUrl },
    );

    const storedJobs = await db.select().from(jobs).where(eq(jobs.crawlRunId, runId));
    const storedPages = await db
      .select()
      .from(rawSourcePages)
      .where(eq(rawSourcePages.crawlRunId, runId));

    expect(storedJobs).toHaveLength(1);
    expect(storedJobs[0]).toMatchObject({
      kind: "fetch_raw_source_page",
      status: "completed",
      attempts: 1,
    });
    expect(storedJobs[0].lockedAt).toBeNull();
    expect(storedJobs[0].lockToken).toBeNull();
    expect(storedPages).toHaveLength(1);
    expect(storedPages[0].providerPayload).toMatchObject({
      fixtureId: "fixture-coin",
      mode: "fake",
    });
    expect(storedPages[0].content).toContain("Fixture Coin");

    expect(inspectOutput).toContain(`run ${runId}`);
    expect(inspectOutput).toContain("source src_test_opaque");
    expect(inspectOutput).toContain("status completed");
    expect(inspectOutput).toContain("raw_pages 1");
    expect(inspectOutput).not.toContain("private://fixture/fixture-coin");
  });
});
