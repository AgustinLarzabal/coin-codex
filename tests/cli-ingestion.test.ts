import { randomUUID } from "node:crypto";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { PGlite } from "@electric-sql/pglite";
import { eq } from "drizzle-orm";
import { afterEach, describe, expect, it } from "vitest";

import { executeCli } from "../src/cli.js";
import {
  FETCH_RAW_SOURCE_PAGE_JOB_KIND,
  JOB_STATUS,
} from "../src/core/ingestion.js";
import { migrate } from "../src/db/migrate.js";
import { createDatabase, registerDatabase, unregisterDatabase } from "../src/db/setup.js";
import { jobs, rawSourcePages, sources } from "../src/db/schema.js";

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
  it("seeds a private source config, uses it for a run, and only reveals private details in debug mode", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const runId = randomUUID();
    const privateDir = await mkdtemp(path.join(tmpdir(), "coincodex-private-"));
    const sourceConfigPath = path.join(privateDir, "sources.json");

    await writeFile(
      sourceConfigPath,
      JSON.stringify([
        {
          id: "src_test_opaque",
          config: {
            adapter: "fake",
            fixtureId: "fixture-coin",
            name: "Private Source Name",
            domain: "private.example.test",
            startUrl: "https://private.example.test/coins",
          },
        },
      ]),
    );

    const seedOutput = await executeCli(["seed-sources", "--file", sourceConfigPath], {
      databaseUrl,
    });

    expect(JSON.parse(seedOutput)).toMatchObject({
      seeded: 1,
      sourceIds: ["src_test_opaque"],
    });

    const createOutput = await executeCli(
      [
        "create-run",
        "--run-id",
        runId,
        "--source-id",
        "src_test_opaque",
        "--scope",
        "issuer_scope",
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
    const debugInspectOutput = await executeCli(
      ["inspect-run", "--run-id", runId, "--debug-private"],
      { databaseUrl },
    );

    const storedJobs = await db.select().from(jobs).where(eq(jobs.crawlRunId, runId));
    const storedPages = await db
      .select()
      .from(rawSourcePages)
      .where(eq(rawSourcePages.crawlRunId, runId));
    const [storedSource] = await db
      .select()
      .from(sources)
      .where(eq(sources.id, "src_test_opaque"));

    expect(storedJobs).toHaveLength(1);
    expect(storedJobs[0]).toMatchObject({
      kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
      status: JOB_STATUS.completed,
      attempts: 1,
    });
    expect(storedJobs[0].lockedAt).toBeNull();
    expect(storedJobs[0].lockToken).toBeNull();
    expect(storedJobs[0].payload).toMatchObject({
      sourceId: "src_test_opaque",
      requestUrl: "https://private.example.test/coins",
    });
    expect(storedPages).toHaveLength(1);
    expect(storedPages[0].providerPayload).toMatchObject({
      fixtureId: "fixture-coin",
      mode: "fake",
    });
    expect(storedPages[0].content).toContain("Fixture Coin");
    expect(storedSource?.config).toMatchObject({
      adapter: "fake",
      fixtureId: "fixture-coin",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    expect(inspectOutput).toContain(`run ${runId}`);
    expect(inspectOutput).toContain("source src_test_opaque");
    expect(inspectOutput).toContain("status completed");
    expect(inspectOutput).toContain("raw_pages 1");
    expect(inspectOutput).not.toContain("Private Source Name");
    expect(inspectOutput).not.toContain("private.example.test");
    expect(inspectOutput).not.toContain("https://private.example.test/coins");
    expect(inspectOutput).not.toContain("Fixture Coin");
    expect(debugInspectOutput).toContain("source_name Private Source Name");
    expect(debugInspectOutput).toContain("source_domain private.example.test");
    expect(debugInspectOutput).toContain("start_url https://private.example.test/coins");
    expect(debugInspectOutput).toContain("title Fixture Coin");
  });
});
