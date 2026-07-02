import { randomUUID } from "node:crypto";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { PGlite } from "@electric-sql/pglite";
import { asc, eq } from "drizzle-orm";
import { afterEach, describe, expect, it } from "vitest";

import { executeCli } from "../src/cli.js";
import {
  ACCEPT_COIN_CANDIDATE_JOB_KIND,
  DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
  EXTRACT_COIN_CANDIDATE_JOB_KIND,
  FETCH_RAW_SOURCE_PAGE_JOB_KIND,
  JOB_STATUS,
} from "../src/core/ingestion.js";
import { migrate } from "../src/db/migrate.js";
import { createDatabase, registerDatabase, unregisterDatabase } from "../src/db/setup.js";
import {
  acceptedCoinImages,
  acceptedCoins,
  coinCandidates,
  jobs,
  rawSourcePages,
  sources,
} from "../src/db/schema.js";

const resources: Array<{ databaseUrl: string; close: () => Promise<void> }> = [];
const SEEDED_SOURCE_ID = "src_test_opaque";

async function createDatabaseUrl() {
  const databaseUrl = `memory://${randomUUID()}`;
  const client = new PGlite();
  registerDatabase(databaseUrl, client);
  resources.push({ databaseUrl, close: () => client.close() });
  await migrate(client);
  const db = createDatabase(client);
  return { databaseUrl, db };
}

async function writeSeedSourceFile() {
  const privateDir = await mkdtemp(path.join(tmpdir(), "coincodex-private-"));
  const sourceConfigPath = path.join(privateDir, "sources.json");

  await writeFile(
    sourceConfigPath,
    JSON.stringify([
      {
        id: SEEDED_SOURCE_ID,
        config: {
          adapter: "fake",
          fixtureId: "fixture-run",
          name: "Private Source Name",
          domain: "private.example.test",
          startUrl: "https://private.example.test/coins",
        },
      },
    ]),
  );

  return sourceConfigPath;
}

async function runWorkerUntilEmpty(databaseUrl: string) {
  const outputs: Array<Record<string, unknown>> = [];

  while (true) {
    const output = JSON.parse(await executeCli(["run-worker"], { databaseUrl })) as Record<
      string,
      unknown
    >;
    outputs.push(output);
    if (output.processed === 0) {
      return outputs;
    }
  }
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
  it("runs the source-private MVP workflow through fetch, extract, accept, quarantine, and accepted-only image handling", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const runId = randomUUID();
    const sourceConfigPath = await writeSeedSourceFile();

    const seedOutput = await executeCli(["seed-sources", "--file", sourceConfigPath], {
      databaseUrl,
    });

    expect(JSON.parse(seedOutput)).toMatchObject({
      seeded: 1,
      sourceIds: [SEEDED_SOURCE_ID],
    });

    const createOutput = await executeCli(
      [
        "create-run",
        "--run-id",
        runId,
        "--source-id",
        SEEDED_SOURCE_ID,
        "--scope",
        "issuer_scope",
      ],
      { databaseUrl },
    );

    expect(JSON.parse(createOutput)).toMatchObject({
      runId,
      sourceId: SEEDED_SOURCE_ID,
      status: "queued",
    });

    const workerOutputs = await runWorkerUntilEmpty(databaseUrl);
    expect(workerOutputs.at(0)).toMatchObject({
      processed: 1,
      runId,
      kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
    });
    expect(workerOutputs.at(-1)).toMatchObject({ processed: 0 });

    const inspectOutput = await executeCli(
      ["inspect-run", "--run-id", runId],
      { databaseUrl },
    );
    const debugInspectOutput = await executeCli(
      ["inspect-run", "--run-id", runId, "--debug-private"],
      { databaseUrl },
    );

    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, runId))
      .orderBy(asc(jobs.createdAt));
    const storedPages = await db
      .select()
      .from(rawSourcePages)
      .where(eq(rawSourcePages.crawlRunId, runId))
      .orderBy(asc(rawSourcePages.fetchedAt));
    const storedCandidates = await db
      .select()
      .from(coinCandidates)
      .where(eq(coinCandidates.crawlRunId, runId))
      .orderBy(asc(coinCandidates.createdAt));
    const storedAcceptedCoins = await db
      .select()
      .from(acceptedCoins)
      .where(eq(acceptedCoins.crawlRunId, runId))
      .orderBy(asc(acceptedCoins.createdAt));
    const storedImages = await db
      .select()
      .from(acceptedCoinImages)
      .where(eq(acceptedCoinImages.crawlRunId, runId))
      .orderBy(asc(acceptedCoinImages.createdAt));
    const [storedSource] = await db
      .select()
      .from(sources)
      .where(eq(sources.id, SEEDED_SOURCE_ID));

    expect(storedJobs).toHaveLength(8);
    expect(storedJobs[0]).toMatchObject({
      kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
      status: JOB_STATUS.completed,
      attempts: 1,
    });
    expect(storedJobs[0].payload).toMatchObject({
      sourceId: SEEDED_SOURCE_ID,
      requestUrl: "https://private.example.test/coins",
    });
    expect(storedPages).toHaveLength(3);
    expect(storedPages[0].providerPayload).toMatchObject({
      fixtureId: "fixture-run",
      mode: "fake",
    });
    expect(storedPages.map((page) => page.normalizedUrl)).toEqual([
      "https://private.example.test/coins",
      "https://private.example.test/coins/accepted-coin",
      "https://private.example.test/coins/quarantine-coin",
    ]);
    expect(storedJobs.map((job) => job.kind)).toEqual([
      FETCH_RAW_SOURCE_PAGE_JOB_KIND,
      FETCH_RAW_SOURCE_PAGE_JOB_KIND,
      FETCH_RAW_SOURCE_PAGE_JOB_KIND,
      EXTRACT_COIN_CANDIDATE_JOB_KIND,
      EXTRACT_COIN_CANDIDATE_JOB_KIND,
      ACCEPT_COIN_CANDIDATE_JOB_KIND,
      ACCEPT_COIN_CANDIDATE_JOB_KIND,
      DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
    ]);
    expect(storedCandidates).toHaveLength(2);
    expect(storedCandidates).toMatchObject([
      {
        pageType: "coin-detail",
        status: "accepted",
        issuer: "Example Issuer",
        denomination: "1 Unit",
        issuedFromYear: 1901,
        issuedToYear: 1901,
        imageUrl: "https://private.example.test/images/accepted-coin.jpg",
        quarantineReason: null,
      },
      {
        pageType: "coin-detail",
        status: "quarantined",
        issuer: "Example Issuer",
        denomination: "",
        issuedFromYear: 1905,
        issuedToYear: 1903,
        imageUrl: "https://private.example.test/images/quarantine-coin.jpg",
        quarantineReason: "invalid_year_range",
      },
    ]);
    expect(storedAcceptedCoins).toHaveLength(1);
    expect(storedAcceptedCoins[0]).toMatchObject({
      issuer: "Example Issuer",
      denomination: "1 Unit",
      issuedFromYear: 1901,
      issuedToYear: 1901,
      sourceDetailUrl: "https://private.example.test/coins/accepted-coin",
    });
    expect(storedImages).toHaveLength(1);
    expect(storedImages[0]).toMatchObject({
      acceptedCoinId: storedAcceptedCoins[0].id,
      sourceImageUrl: "https://private.example.test/images/accepted-coin.jpg",
    });
    expect(storedSource?.config).toMatchObject({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    expect(inspectOutput).toContain(`run ${runId}`);
    expect(inspectOutput).toContain(`source ${SEEDED_SOURCE_ID}`);
    expect(inspectOutput).toContain("status completed");
    expect(inspectOutput).toContain("raw_pages 3");
    expect(inspectOutput).toContain("candidates accepted=1 quarantined=1");
    expect(inspectOutput).toContain("accepted_coins 1");
    expect(inspectOutput).toContain("accepted_coin_images 1");
    expect(inspectOutput).not.toContain("Private Source Name");
    expect(inspectOutput).not.toContain("private.example.test");
    expect(inspectOutput).not.toContain("https://private.example.test/coins");
    expect(inspectOutput).not.toContain("Accepted Fixture Coin");
    expect(debugInspectOutput).toContain("source_name Private Source Name");
    expect(debugInspectOutput).toContain("source_domain private.example.test");
    expect(debugInspectOutput).toContain("start_url https://private.example.test/coins");
    expect(debugInspectOutput).toContain("title Accepted Fixture Coin");
  });
});
