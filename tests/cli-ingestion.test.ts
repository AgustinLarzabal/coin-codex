import { randomUUID } from "node:crypto";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";

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
import type { OperatorConsolePrompt } from "../src/core/operator-console.js";
import {
  ImageProviderError,
  type ImageProvider,
} from "../src/core/providers/image-provider.js";
import { migrate } from "../src/db/migrate.js";
import {
  acceptedCoinImages,
  acceptedCoins,
  coinCandidates,
  crawlRuns,
  jobs,
  rawSourcePages,
  sources,
} from "../src/db/schema.js";
import { createDatabase, registerDatabase, unregisterDatabase } from "../src/db/setup.js";

const resources: Array<{ databaseUrl: string; close: () => Promise<void> }> = [];
const filesystemResources: Array<{ path: string }> = [];
const SEEDED_SOURCE_ID = "src_test_opaque";

function createStubOperatorConsolePrompt(
  answers: string[],
): OperatorConsolePrompt {
  let answerIndex = 0;

  return {
    async text() {
      const answer = answers[answerIndex];
      answerIndex += 1;
      return answer ?? "";
    },
  };
}

type StubImageProviderOptions = {
  resolveContent?: (imageUrl: string) => Uint8Array;
  onDownload?: (imageUrl: string) => void;
  errorFactory?: (imageUrl: string) => ImageProviderError;
};

function createStubImageProviderFactory(
  options: StubImageProviderOptions = {},
): () => ImageProvider {
  const resolveContent =
    options.resolveContent ?? ((imageUrl: string) => new TextEncoder().encode(imageUrl));

  return () => ({
    async downloadImage({ imageUrl }) {
      options.onDownload?.(imageUrl);
      if (options.errorFactory) {
        throw options.errorFactory(imageUrl);
      }

      return {
        contentType: "image/jpeg",
        content: resolveContent(imageUrl),
        providerPayload: {
          adapter: "test-image-provider",
        },
      };
    },
  });
}

async function createDatabaseUrl() {
  const databaseUrl = `memory://${randomUUID()}`;
  const client = new PGlite();
  registerDatabase(databaseUrl, client);
  resources.push({ databaseUrl, close: () => client.close() });
  await migrate(client);
  const db = createDatabase(client);
  return { databaseUrl, db };
}

type TestSourceConfig = {
  adapter: string;
  fixtureId?: string;
  name: string;
  domain: string;
  startUrl: string;
  [key: string]: unknown;
};

async function writeSeedSourceFile(
  fixtureIdOrConfig: string | TestSourceConfig,
  startUrl?: string,
) {
  const privateDir = await mkdtemp(path.join(tmpdir(), "coincodex-private-"));
  const sourceConfigPath = path.join(privateDir, "sources.json");
  const config =
    typeof fixtureIdOrConfig === "string"
      ? {
          adapter: "fake" as const,
          fixtureId: fixtureIdOrConfig,
          name: "Private Source Name",
          domain: "private.example.test",
          startUrl: startUrl ?? "",
        }
      : fixtureIdOrConfig;

  await writeFile(
    sourceConfigPath,
    JSON.stringify([
      {
        id: SEEDED_SOURCE_ID,
        config,
      },
    ]),
  );

  return sourceConfigPath;
}

async function runWorkerUntilEmpty(
  databaseUrl: string,
  deps: Parameters<typeof executeCli>[1] = {},
) {
  const outputs: Array<Record<string, unknown>> = [];

  while (true) {
    const output = JSON.parse(
      await executeCli(["run-worker"], {
        databaseUrl,
        ...deps,
      }),
    ) as Record<string, unknown>;
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
  await Promise.all(
    filesystemResources.splice(0).map(async (resource) => {
      await rm(resource.path, { force: true });
    }),
  );
});

describe("CLI ingestion skeleton", () => {
  it("launches the operator console seed and create-run workflow with the default seed path", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const privateDir = path.join(process.cwd(), ".private");
    const defaultSeedPath = path.join(privateDir, "sources.json");

    await mkdir(privateDir, { recursive: true });
    filesystemResources.push({ path: defaultSeedPath });
    await writeFile(
      defaultSeedPath,
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

    const output = await executeCli(["operator-console"], {
      databaseUrl,
      operatorConsolePrompt: createStubOperatorConsolePrompt([
        "",
        SEEDED_SOURCE_ID,
        "console_scope",
        "7",
      ]),
    });

    const storedRuns = await db.select().from(crawlRuns).orderBy(asc(crawlRuns.createdAt));

    expect(output).toContain(
      "Seed Sources -> Create Crawl Run -> Process Jobs -> Inspect Results",
    );
    expect(output).toContain(`seed file .private/sources.json`);
    expect(output).toContain(`seeded source ids ${SEEDED_SOURCE_ID}`);
    expect(output).toContain(`source ${SEEDED_SOURCE_ID}`);
    expect(output).toContain("scope console_scope");
    expect(output).toContain("status queued");
    expect(storedRuns).toHaveLength(1);
    expect(storedRuns[0]).toMatchObject({
      sourceId: SEEDED_SOURCE_ID,
      scope: "console_scope",
      detailLimit: 7,
      status: "queued",
    });
    expect(output).toContain(`active run ${storedRuns[0].id}`);
  });

  it("processes the active run until idle and inspects it from the operator console", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const imageProviderFactory = createStubImageProviderFactory();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    const output = await executeCli(["operator-console", "--seed-file", sourceConfigPath], {
      databaseUrl,
      imageProviderFactory,
      operatorConsolePrompt: createStubOperatorConsolePrompt([
        "",
        SEEDED_SOURCE_ID,
        "console_scope",
        "10",
        "process-until-idle",
        "",
        "inspect",
        "exit",
      ]),
    });

    const storedRuns = await db.select().from(crawlRuns).orderBy(asc(crawlRuns.createdAt));
    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, storedRuns[0].id))
      .orderBy(asc(jobs.scheduledAt), asc(jobs.createdAt));

    expect(storedRuns).toHaveLength(1);
    expect(output).toContain("Process Jobs");
    expect(output).toContain("process_until_idle cap 100");
    expect(output).toContain("process_until_idle idle after 8 jobs");
    expect(output).toContain("processing_summary completed=8 failed=0 retried=0 queued=0");
    expect(output).toContain("Inspect Results");
    expect(output).toContain(`run ${storedRuns[0].id}`);
    expect(output).toContain("status completed");
    expect(output).toContain("accepted_coins 1");
    expect(output).toContain("accepted_coin_images 1");
    expect(storedJobs).toHaveLength(8);
    expect(storedJobs.every((job) => job.status === JOB_STATUS.completed)).toBe(true);
  });

  it("processes one queued job at a time from the operator console and reports remaining queued work", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const imageProviderFactory = createStubImageProviderFactory();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    const output = await executeCli(["operator-console", "--seed-file", sourceConfigPath], {
      databaseUrl,
      imageProviderFactory,
      operatorConsolePrompt: createStubOperatorConsolePrompt([
        "",
        SEEDED_SOURCE_ID,
        "console_scope",
        "10",
        "process-next",
        "exit",
      ]),
    });

    const [storedRun] = await db.select().from(crawlRuns).orderBy(asc(crawlRuns.createdAt));
    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, storedRun.id))
      .orderBy(asc(jobs.scheduledAt), asc(jobs.createdAt));

    expect(output).toContain("process_next job");
    expect(output).toContain("processing_summary completed=1 failed=0 retried=0 queued=2");
    expect(storedJobs).toHaveLength(3);
    expect(storedJobs.map((job) => job.status)).toEqual([
      JOB_STATUS.completed,
      JOB_STATUS.queued,
      JOB_STATUS.queued,
    ]);
  });

  it("stops process-until-idle at the requested cap and reports queued work that remains", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const imageProviderFactory = createStubImageProviderFactory();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    const output = await executeCli(["operator-console", "--seed-file", sourceConfigPath], {
      databaseUrl,
      imageProviderFactory,
      operatorConsolePrompt: createStubOperatorConsolePrompt([
        "",
        SEEDED_SOURCE_ID,
        "console_scope",
        "10",
        "process-until-idle",
        "1",
        "exit",
      ]),
    });

    const [storedRun] = await db.select().from(crawlRuns).orderBy(asc(crawlRuns.createdAt));
    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, storedRun.id))
      .orderBy(asc(jobs.scheduledAt), asc(jobs.createdAt));

    expect(output).toContain("process_until_idle cap 1");
    expect(output).toContain("process_until_idle cap_reached after 1 jobs");
    expect(output).toContain("processing_summary completed=1 failed=0 retried=0 queued=2");
    expect(storedJobs).toHaveLength(3);
    expect(storedJobs.filter((job) => job.status === JOB_STATUS.queued)).toHaveLength(2);
  });

  it("continues process-until-idle through failed jobs and can reveal private inspection details on demand", async () => {
    const { databaseUrl } = await createDatabaseUrl();
    const imageProviderFactory = createStubImageProviderFactory({
      errorFactory: (imageUrl) =>
        new ImageProviderError(`failed to download ${imageUrl}`, {
          code: "IMAGE_TIMEOUT",
          retryable: true,
          statusCode: 504,
          providerPayload: {
            adapter: "test-image-provider",
          },
        }),
    });
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    const output = await executeCli(["operator-console", "--seed-file", sourceConfigPath], {
      databaseUrl,
      imageProviderFactory,
      operatorConsolePrompt: createStubOperatorConsolePrompt([
        "",
        SEEDED_SOURCE_ID,
        "console_scope",
        "10",
        "process-until-idle",
        "",
        "inspect",
        "toggle-debug",
        "inspect",
        "exit",
      ]),
    });

    const inspections = output.split("\n\nInspect Results\n").slice(1);

    expect(output).toContain("process job");
    expect(output).toContain("status=queued");
    expect(output).toContain("status=failed");
    expect(output).toContain("process_until_idle idle after 10 jobs");
    expect(output).toContain("processing_summary completed=7 failed=1 retried=2 queued=0");
    expect(inspections[0]).toContain("failures total=1");
    expect(inspections[0]).not.toContain("Private Source Name");
    expect(inspections[0]).not.toContain("private.example.test");
    expect(output).toContain("private_debug on");
    expect(inspections[1]).toContain("source_name Private Source Name");
    expect(inspections[1]).toContain("source_domain private.example.test");
    expect(inspections[1]).toContain("start_url https://private.example.test/coins");
  });

  it("fails operator console seeding visibly before creating a run when the seed file is invalid", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const privateDir = await mkdtemp(path.join(tmpdir(), "coincodex-private-invalid-"));
    const invalidSeedPath = path.join(privateDir, "sources.json");

    filesystemResources.push({ path: invalidSeedPath });
    await writeFile(invalidSeedPath, JSON.stringify({ id: SEEDED_SOURCE_ID }));

    await expect(
      executeCli(["operator-console"], {
        databaseUrl,
        operatorConsolePrompt: createStubOperatorConsolePrompt([invalidSeedPath]),
      }),
    ).rejects.toThrow(
      `seed failed for ${invalidSeedPath}: seed file must contain an array of sources`,
    );

    const storedRuns = await db.select().from(crawlRuns);

    expect(storedRuns).toHaveLength(0);
  });

  it("processes the next queued job from the operator console active run", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    const output = await executeCli(["operator-console", "--seed-file", sourceConfigPath], {
      databaseUrl,
      operatorConsolePrompt: createStubOperatorConsolePrompt([
        "",
        SEEDED_SOURCE_ID,
        "console_scope",
        "10",
        "process-next-job",
      ]),
    });

    const storedRuns = await db.select().from(crawlRuns).orderBy(asc(crawlRuns.createdAt));
    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, storedRuns[0].id))
      .orderBy(asc(jobs.scheduledAt), asc(jobs.createdAt));

    expect(output).toContain("Process Jobs");
    expect(output).toContain("action process-next-job");
    expect(output).toContain("processed jobs 1");
    expect(output).toContain("last job fetch_raw_source_page status=completed");
    expect(output).toContain("jobs completed=1 failed=0 queued=2 retries=0");
    expect(output).toContain("visible failures 0");
    expect(storedJobs).toHaveLength(3);
    expect(storedJobs.map((job) => ({ kind: job.kind, status: job.status }))).toEqual([
      {
        kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
        status: JOB_STATUS.completed,
      },
      {
        kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
        status: JOB_STATUS.queued,
      },
      {
        kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
        status: JOB_STATUS.queued,
      },
    ]);
  });

  it("processes jobs until idle from the operator console active run", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const imageProviderFactory = createStubImageProviderFactory();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    const output = await executeCli(["operator-console", "--seed-file", sourceConfigPath], {
      databaseUrl,
      imageProviderFactory,
      operatorConsolePrompt: createStubOperatorConsolePrompt([
        "",
        SEEDED_SOURCE_ID,
        "console_scope",
        "10",
        "process-until-idle",
        "",
      ]),
    });

    const storedRuns = await db.select().from(crawlRuns).orderBy(asc(crawlRuns.createdAt));
    const [run] = storedRuns;

    expect(output).toContain("action process-until-idle");
    expect(output).toContain("processed jobs 8");
    expect(output).toContain("stop reason idle");
    expect(output).toContain("last job download_accepted_coin_image status=completed");
    expect(output).toContain("jobs completed=8 failed=0 queued=0 retries=0");
    expect(output).toContain("visible failures 0");
    expect(run.status).toBe("completed");
  });

  it("stops operator console processing at the configured cap when work remains", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const sourceConfigPath = await writeSeedSourceFile(
      "fixture-catalog",
      "HTTPS://private.example.test/coins?b=2&a=1#top",
    );

    const output = await executeCli(["operator-console", "--seed-file", sourceConfigPath], {
      databaseUrl,
      operatorConsolePrompt: createStubOperatorConsolePrompt([
        "",
        SEEDED_SOURCE_ID,
        "console_scope",
        "10",
        "process-until-idle",
        "2",
      ]),
    });

    const storedRuns = await db.select().from(crawlRuns).orderBy(asc(crawlRuns.createdAt));
    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, storedRuns[0].id))
      .orderBy(asc(jobs.scheduledAt), asc(jobs.createdAt));

    expect(output).toContain("action process-until-idle");
    expect(output).toContain("processed jobs 2");
    expect(output).toContain("stop reason cap");
    expect(output).toContain("jobs completed=2 failed=0 queued=10 retries=0");
    expect(output).toContain("visible failures 0");
    expect(storedJobs.filter((job) => job.status === JOB_STATUS.queued)).toHaveLength(10);
    expect(storedRuns[0].status).toBe("queued");
  });

  it("continues operator console processing through failed jobs and surfaces failures", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const imageProviderFactory = createStubImageProviderFactory();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "firecrawl",
      apiKey: "fc-test-key",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
      ratePolicy: {
        minDelayMs: 0,
        backoffBaseMs: 0,
        attemptLimit: 2,
      },
    });

    const scrapeAttempts = new Map<string, number>();
    const firecrawlClientFactory = () => ({
      async scrapeUrl({ url }: { url: string }) {
        const nextAttempt = (scrapeAttempts.get(url) ?? 0) + 1;
        scrapeAttempts.set(url, nextAttempt);

        if (url === "https://private.example.test/coins") {
          return {
            requestId: "req-listing",
            data: {
              html: `
<html>
  <body>
    <section data-page-kind="listing">
      <h1>Live Listing</h1>
    </section>
  </body>
</html>`.trim(),
              links: [
                "https://private.example.test/coins/accepted-coin",
                "https://private.example.test/coins/failed-coin",
              ],
              metadata: {
                statusCode: 200,
                title: "Live Listing",
              },
            },
          };
        }

        if (url === "https://private.example.test/coins/accepted-coin") {
          return {
            requestId: "req-accepted",
            data: {
              html: `
<html>
  <body>
    <article data-page-kind="coin-detail" data-image-url="https://private.example.test/images/accepted-coin.jpg">
      <h1>Accepted Firecrawl Coin</h1>
      <p>Issuer: Example Issuer</p>
      <p>Denomination: 1 Unit</p>
      <p>Year: 1901</p>
    </article>
  </body>
</html>`.trim(),
              links: [],
              metadata: {
                statusCode: 200,
                title: "Accepted Firecrawl Coin",
              },
            },
          };
        }

        if (url === "https://private.example.test/coins/failed-coin") {
          throw Object.assign(new Error(`rate limited on attempt ${nextAttempt}`), {
            statusCode: 429,
            code: "RATE_LIMITED",
            requestId: `req-failed-${nextAttempt}`,
          });
        }

        throw new Error(`unexpected scrape url: ${url}`);
      },
    });

    const output = await executeCli(["operator-console", "--seed-file", sourceConfigPath], {
      databaseUrl,
      firecrawlClientFactory,
      imageProviderFactory,
      operatorConsolePrompt: createStubOperatorConsolePrompt([
        "",
        SEEDED_SOURCE_ID,
        "console_scope",
        "10",
        "process-until-idle",
        "",
      ]),
    });

    const storedRuns = await db.select().from(crawlRuns).orderBy(asc(crawlRuns.createdAt));
    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, storedRuns[0].id))
      .orderBy(asc(jobs.createdAt));

    expect(output).toContain("action process-until-idle");
    expect(output).toContain("processed jobs 7");
    expect(output).toContain("stop reason idle");
    expect(output).toContain("jobs completed=5 failed=1 queued=0 retries=1");
    expect(output).toContain("visible failures 2");
    expect(output).toContain("kind=fetch_raw_source_page status=failed code=RATE_LIMITED");
    expect(scrapeAttempts).toEqual(
      new Map([
        ["https://private.example.test/coins", 1],
        ["https://private.example.test/coins/accepted-coin", 1],
        ["https://private.example.test/coins/failed-coin", 2],
      ]),
    );
    expect(storedRuns[0].status).toBe("completed");
    expect(
      storedJobs.filter((job) => job.kind === FETCH_RAW_SOURCE_PAGE_JOB_KIND).map((job) => ({
        requestUrl: job.payload.requestUrl,
        attempts: job.attempts,
        status: job.status,
      })),
    ).toEqual([
      {
        requestUrl: "https://private.example.test/coins",
        attempts: 1,
        status: JOB_STATUS.completed,
      },
      {
        requestUrl: "https://private.example.test/coins/accepted-coin",
        attempts: 1,
        status: JOB_STATUS.completed,
      },
      {
        requestUrl: "https://private.example.test/coins/failed-coin",
        attempts: 2,
        status: JOB_STATUS.failed,
      },
    ]);
  });

  it("returns inspect output for file-backed CLI databases", async () => {
    const databaseDir = await mkdtemp(path.join(tmpdir(), "coincodex-db-"));
    const databaseUrl = path.join(databaseDir, "pglite");
    const runId = randomUUID();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    await executeCli(["seed-sources", "--file", sourceConfigPath], { databaseUrl });
    await executeCli(
      [
        "create-run",
        "--run-id",
        runId,
        "--source-id",
        SEEDED_SOURCE_ID,
        "--scope",
        "inspect_regression",
      ],
      { databaseUrl },
    );

    const inspectOutput = await executeCli(["inspect-run", "--run-id", runId], {
      databaseUrl,
    });

    expect(inspectOutput).toContain(`run ${runId}`);
    expect(inspectOutput).toContain("status queued");
    expect(inspectOutput).toContain("jobs total=1 completed=0 failed=0 retries=0");
  }, 10000);

  it("extracts rich coin candidates from fixture detail pages and quarantines specimen-like pages", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const runId = randomUUID();
    const imageProviderFactory = createStubImageProviderFactory();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-detail-pages",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    await executeCli(["seed-sources", "--file", sourceConfigPath], {
      databaseUrl,
      imageProviderFactory,
    });

    await executeCli(
      [
        "create-run",
        "--run-id",
        runId,
        "--source-id",
        SEEDED_SOURCE_ID,
        "--scope",
        "issuer_scope",
      ],
      { databaseUrl, imageProviderFactory },
    );

    const workerOutputs = await runWorkerUntilEmpty(databaseUrl, { imageProviderFactory });

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

    expect(storedCandidates).toHaveLength(3);
    expect(storedCandidates).toMatchObject([
      {
        pageType: "coin-detail",
        status: "accepted",
        originalDetailUrl: "https://private.example.test/coins/ceres-5-francs",
        normalizedDetailUrl: "https://private.example.test/coins/ceres-5-francs",
        nameRaw: "5   Francs   Ceres",
        nameNormalized: "5 Francs Ceres",
        issuerRaw: "République   française",
        issuerNormalized: "République française",
        denominationRaw: "5 Francs",
        denominationNormalized: "5 Francs",
        rawDateText: "1870-1871",
        issuedFromYear: 1870,
        issuedToYear: 1871,
        mintMark: "A",
        quarantineReason: null,
      },
      {
        pageType: "specimen-detail",
        status: "quarantined",
        nameNormalized: "5 Francs Specimen PCGS MS64",
        quarantineReason: "unrecognized_page_type",
      },
      {
        pageType: "reference-detail",
        status: "quarantined",
        nameNormalized: "Minting Reference Note",
        quarantineReason: "unrecognized_page_type",
      },
    ]);
    expect(storedAcceptedCoins).toHaveLength(1);
    expect(storedAcceptedCoins[0]).toMatchObject({
      name: "5 Francs Ceres",
      issuer: "République française",
      denomination: "5 Francs",
      issuedFromYear: 1870,
      issuedToYear: 1871,
      mintMark: "A",
      sourceDetailUrl: "https://private.example.test/coins/ceres-5-francs",
    });
    expect(storedAcceptedCoins[0].acceptedAt).toBeInstanceOf(Date);
    expect(storedImages).toHaveLength(1);
  });

  it("runs the source-private MVP workflow through fetch, extract, accept, quarantine, and accepted-only image handling", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const runId = randomUUID();
    const imageProviderFactory = createStubImageProviderFactory();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    const seedOutput = await executeCli(["seed-sources", "--file", sourceConfigPath], {
      databaseUrl,
      imageProviderFactory,
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
        "--detail-limit",
        "10",
      ],
      { databaseUrl, imageProviderFactory },
    );

    expect(JSON.parse(createOutput)).toMatchObject({
      runId,
      sourceId: SEEDED_SOURCE_ID,
      status: "queued",
    });

    const workerOutputs = await runWorkerUntilEmpty(databaseUrl, { imageProviderFactory });
    expect(workerOutputs.at(0)).toMatchObject({
      processed: 1,
      runId,
      kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
    });
    expect(workerOutputs.at(-1)).toMatchObject({ processed: 0 });

    const inspectOutput = await executeCli(
      ["inspect-run", "--run-id", runId],
      { databaseUrl, imageProviderFactory },
    );
    const debugInspectOutput = await executeCli(
      ["inspect-run", "--run-id", runId, "--debug-private"],
      { databaseUrl, imageProviderFactory },
    );

    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, runId))
      .orderBy(asc(jobs.scheduledAt), asc(jobs.createdAt));
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
      lockedAt: null,
      lockToken: null,
    });
    expect(storedJobs[0].payload).toMatchObject({
      sourceId: SEEDED_SOURCE_ID,
      requestUrl: "https://private.example.test/coins",
      pageRole: "listing",
    });
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
    expect(storedPages.map((page) => page.pageType)).toEqual(["listing", "detail", "detail"]);
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
      name: "Accepted Fixture Coin",
      issuer: "Example Issuer",
      denomination: "1 Unit",
      issuedFromYear: 1901,
      issuedToYear: 1901,
      mintMark: "",
      sourceDetailUrl: "https://private.example.test/coins/accepted-coin",
    });
    expect(storedAcceptedCoins[0].acceptedAt).toBeInstanceOf(Date);
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
    expect(inspectOutput).toContain("page_type=listing");
    expect(inspectOutput).not.toContain("Private Source Name");
    expect(inspectOutput).not.toContain("private.example.test/coins");
    expect(inspectOutput).not.toContain("Accepted Fixture Coin");
    expect(debugInspectOutput).toContain("source_name Private Source Name");
    expect(debugInspectOutput).toContain("source_domain private.example.test");
    expect(debugInspectOutput).toContain("start_url https://private.example.test/coins");
    expect(debugInspectOutput).toContain("title Accepted Fixture Coin");
  });

  it("downloads images only for accepted coins and stores image attribution plus duplicate-content detection", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const runId = randomUUID();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "firecrawl",
      apiKey: "fc-test-key",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
      ratePolicy: {
        minDelayMs: 0,
        backoffBaseMs: 0,
        attemptLimit: 2,
      },
    });

    const imageCalls: string[] = [];
    const imagePayload = new TextEncoder().encode("shared-image-binary");
    const firecrawlClientFactory = () => ({
      async scrapeUrl({ url }: { url: string }) {
        if (url === "https://private.example.test/coins") {
          return {
            requestId: "req-listing",
            data: {
              html: `
<html>
  <body>
    <section data-page-kind="listing">
      <a data-coin-detail-link="true" href="https://private.example.test/coins/accepted-a">Accepted A</a>
      <a data-coin-detail-link="true" href="https://private.example.test/coins/accepted-b">Accepted B</a>
      <a data-coin-detail-link="true" href="https://private.example.test/coins/quarantine">Quarantine</a>
    </section>
  </body>
</html>`.trim(),
              links: [
                "https://private.example.test/coins/accepted-a",
                "https://private.example.test/coins/accepted-b",
                "https://private.example.test/coins/quarantine",
              ],
              metadata: {
                statusCode: 200,
                title: "Image Listing",
              },
            },
          };
        }

        if (url === "https://private.example.test/coins/accepted-a") {
          return {
            requestId: "req-accepted-a",
            data: {
              html: `
<html>
  <body>
    <article data-page-kind="coin-detail" data-image-url="https://private.example.test/images/shared-a.jpg">
      <h1>Accepted Image Coin A</h1>
      <p>Issuer: Example Issuer</p>
      <p>Denomination: 1 Unit</p>
      <p>Year: 1901</p>
    </article>
  </body>
</html>`.trim(),
              links: [],
              metadata: {
                statusCode: 200,
                title: "Accepted Image Coin A",
              },
            },
          };
        }

        if (url === "https://private.example.test/coins/accepted-b") {
          return {
            requestId: "req-accepted-b",
            data: {
              html: `
<html>
  <body>
    <article data-page-kind="coin-detail" data-image-url="https://private.example.test/images/shared-b.jpg">
      <h1>Accepted Image Coin B</h1>
      <p>Issuer: Example Issuer</p>
      <p>Denomination: 2 Unit</p>
      <p>Year: 1902</p>
    </article>
  </body>
</html>`.trim(),
              links: [],
              metadata: {
                statusCode: 200,
                title: "Accepted Image Coin B",
              },
            },
          };
        }

        if (url === "https://private.example.test/coins/quarantine") {
          return {
            requestId: "req-quarantine",
            data: {
              html: `
<html>
  <body>
    <article data-page-kind="coin-detail" data-image-url="https://private.example.test/images/quarantine.jpg">
      <h1>Quarantine Image Coin</h1>
      <p>Issuer: Example Issuer</p>
      <p>Denomination: </p>
      <p>Year: 1905-1903</p>
    </article>
  </body>
</html>`.trim(),
              links: [],
              metadata: {
                statusCode: 200,
                title: "Quarantine Image Coin",
              },
            },
          };
        }

        throw new Error(`unexpected scrape url: ${url}`);
      },
    });
    const imageProviderFactory = createStubImageProviderFactory({
      resolveContent: () => imagePayload,
      onDownload: (imageUrl) => {
        imageCalls.push(imageUrl);
      },
    });

    await executeCli(["seed-sources", "--file", sourceConfigPath], {
      databaseUrl,
      firecrawlClientFactory,
      imageProviderFactory,
    });
    await executeCli(
      [
        "create-run",
        "--run-id",
        runId,
        "--source-id",
        SEEDED_SOURCE_ID,
        "--scope",
        "issuer_scope",
      ],
      { databaseUrl, firecrawlClientFactory, imageProviderFactory },
    );
    await runWorkerUntilEmpty(databaseUrl, {
      firecrawlClientFactory,
      imageProviderFactory,
    });

    const storedAcceptedCoins = await db
      .select()
      .from(acceptedCoins)
      .where(eq(acceptedCoins.crawlRunId, runId))
      .orderBy(asc(acceptedCoins.createdAt));
    const storedCandidates = await db
      .select()
      .from(coinCandidates)
      .where(eq(coinCandidates.crawlRunId, runId))
      .orderBy(asc(coinCandidates.createdAt));
    const storedImages = await db
      .select()
      .from(acceptedCoinImages)
      .where(eq(acceptedCoinImages.crawlRunId, runId))
      .orderBy(asc(acceptedCoinImages.createdAt));

    expect(storedAcceptedCoins).toHaveLength(2);
    expect(storedCandidates).toMatchObject([
      {
        normalizedDetailUrl: "https://private.example.test/coins/accepted-a",
        status: "accepted",
        imageUrl: "https://private.example.test/images/shared-a.jpg",
      },
      {
        normalizedDetailUrl: "https://private.example.test/coins/accepted-b",
        status: "accepted",
        imageUrl: "https://private.example.test/images/shared-b.jpg",
      },
      {
        normalizedDetailUrl: "https://private.example.test/coins/quarantine",
        status: "quarantined",
        imageUrl: "https://private.example.test/images/quarantine.jpg",
      },
    ]);
    expect(imageCalls).toEqual([
      "https://private.example.test/images/shared-a.jpg",
      "https://private.example.test/images/shared-b.jpg",
    ]);
    expect(storedImages).toHaveLength(2);
    expect(storedImages[0]).toMatchObject({
      acceptedCoinId: storedAcceptedCoins[0].id,
    });
    expect(storedImages[1]).toMatchObject({
      acceptedCoinId: storedAcceptedCoins[1].id,
    });
    expect(storedImages[0].contentHash).toBe(storedImages[1].contentHash);
    expect(storedImages[1].duplicateOfAcceptedCoinImageId).toBe(storedImages[0].id);

    const downloadJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, runId))
      .orderBy(asc(jobs.createdAt));
    expect(
      downloadJobs.filter((job) => job.kind === DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND),
    ).toHaveLength(2);
  });

  it("records and retries image download failures without reverting coin acceptance", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const runId = randomUUID();
    const imageProviderFactory = createStubImageProviderFactory({
      errorFactory: (imageUrl) =>
        new ImageProviderError(`failed to download ${imageUrl}`, {
          code: "IMAGE_TIMEOUT",
          retryable: true,
          statusCode: 504,
          providerPayload: {
            adapter: "test-image-provider",
          },
        }),
    });
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    await executeCli(["seed-sources", "--file", sourceConfigPath], {
      databaseUrl,
      imageProviderFactory,
    });
    await executeCli(
      [
        "create-run",
        "--run-id",
        runId,
        "--source-id",
        SEEDED_SOURCE_ID,
        "--scope",
        "issuer_scope",
      ],
      { databaseUrl, imageProviderFactory },
    );
    const workerOutputs = await runWorkerUntilEmpty(databaseUrl, { imageProviderFactory });

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
    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, runId))
      .orderBy(asc(jobs.createdAt));
    const downloadJob = storedJobs.find(
      (job) => job.kind === DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
    );
    const inspectOutput = await executeCli(
      ["inspect-run", "--run-id", runId],
      { databaseUrl, imageProviderFactory },
    );

    expect(storedAcceptedCoins).toHaveLength(1);
    expect(storedImages).toHaveLength(0);
    expect(downloadJob).toMatchObject({
      status: JOB_STATUS.failed,
      attempts: 3,
      errorPayload: {
        code: "IMAGE_TIMEOUT",
        retryable: true,
        statusCode: 504,
      },
    });
    expect(inspectOutput).toContain("jobs total=8 completed=7 failed=1 retries=2");
    expect(inspectOutput).toContain(
      "job_kind download_accepted_coin_image total=1 completed=0 failed=1 retries=2",
    );
    expect(inspectOutput).toContain("failures total=1");
    expect(inspectOutput).toContain("error_code=IMAGE_TIMEOUT");
    expect(inspectOutput).not.toContain("failed to download");
    expect(inspectOutput).not.toContain("https://private.example.test/images/accepted-coin.jpg");
  });

  it("quarantines strong fingerprint matches from a different source instead of deduping by shared url alone", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const firstRunId = randomUUID();
    const secondRunId = randomUUID();
    const firstSourceId = "src_fixture_a";
    const secondSourceId = "src_fixture_b";

    await db.insert(sources).values([
      {
        id: firstSourceId,
        config: {
          adapter: "fake",
          fixtureId: "fixture-run",
          name: "Private Source A",
          domain: "private.example.test",
          startUrl: "https://private.example.test/coins",
        },
      },
      {
        id: secondSourceId,
        config: {
          adapter: "fake",
          fixtureId: "fixture-run",
          name: "Private Source B",
          domain: "private.example.test",
          startUrl: "https://private.example.test/coins",
        },
      },
    ]);

    await executeCli(
      [
        "create-run",
        "--run-id",
        firstRunId,
        "--source-id",
        firstSourceId,
        "--scope",
        "issuer_scope_a",
      ],
      { databaseUrl },
    );
    await runWorkerUntilEmpty(databaseUrl);

    await executeCli(
      [
        "create-run",
        "--run-id",
        secondRunId,
        "--source-id",
        secondSourceId,
        "--scope",
        "issuer_scope_b",
      ],
      { databaseUrl },
    );
    await runWorkerUntilEmpty(databaseUrl);

    const storedCandidates = await db
      .select()
      .from(coinCandidates)
      .where(eq(coinCandidates.crawlRunId, secondRunId))
      .orderBy(asc(coinCandidates.createdAt));
    const storedAcceptedCoins = await db
      .select()
      .from(acceptedCoins)
      .where(eq(acceptedCoins.crawlRunId, secondRunId))
      .orderBy(asc(acceptedCoins.createdAt));

    expect(storedAcceptedCoins).toHaveLength(0);
    expect(storedCandidates).toMatchObject([
      {
        normalizedDetailUrl: "https://private.example.test/coins/accepted-coin",
        status: "quarantined",
        quarantineReason: "duplicate_fingerprint",
      },
      {
        normalizedDetailUrl: "https://private.example.test/coins/quarantine-coin",
        status: "quarantined",
        quarantineReason: "invalid_year_range",
      },
    ]);
  });

  it("quarantines duplicate source detail urls only within the same source", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const firstRunId = randomUUID();
    const secondRunId = randomUUID();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-run",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    await executeCli(["seed-sources", "--file", sourceConfigPath], {
      databaseUrl,
    });

    await executeCli(
      [
        "create-run",
        "--run-id",
        firstRunId,
        "--source-id",
        SEEDED_SOURCE_ID,
        "--scope",
        "issuer_scope_a",
      ],
      { databaseUrl },
    );
    await runWorkerUntilEmpty(databaseUrl);

    await executeCli(
      [
        "create-run",
        "--run-id",
        secondRunId,
        "--source-id",
        SEEDED_SOURCE_ID,
        "--scope",
        "issuer_scope_b",
      ],
      { databaseUrl },
    );
    await runWorkerUntilEmpty(databaseUrl);

    const storedCandidates = await db
      .select()
      .from(coinCandidates)
      .where(eq(coinCandidates.crawlRunId, secondRunId))
      .orderBy(asc(coinCandidates.createdAt));
    const storedAcceptedCoins = await db
      .select()
      .from(acceptedCoins)
      .where(eq(acceptedCoins.crawlRunId, secondRunId))
      .orderBy(asc(acceptedCoins.createdAt));

    expect(storedAcceptedCoins).toHaveLength(0);
    expect(storedCandidates).toMatchObject([
      {
        normalizedDetailUrl: "https://private.example.test/coins/accepted-coin",
        status: "quarantined",
        quarantineReason: "duplicate_source_detail_url",
      },
      {
        normalizedDetailUrl: "https://private.example.test/coins/quarantine-coin",
        status: "quarantined",
        quarantineReason: "invalid_year_range",
      },
    ]);
  });

  it("quarantines missing identity fields and extraction failures with actionable reasons", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const runId = randomUUID();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "fake",
      fixtureId: "fixture-acceptance-edge-cases",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });

    await executeCli(["seed-sources", "--file", sourceConfigPath], {
      databaseUrl,
    });
    await executeCli(
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
    await runWorkerUntilEmpty(databaseUrl);

    const storedCandidates = await db
      .select()
      .from(coinCandidates)
      .where(eq(coinCandidates.crawlRunId, runId))
      .orderBy(asc(coinCandidates.createdAt));
    const storedJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, runId))
      .orderBy(asc(jobs.createdAt));

    expect(storedCandidates).toMatchObject([
      {
        normalizedDetailUrl: "https://private.example.test/coins/missing-fields",
        status: "quarantined",
        quarantineReason: "missing_identity_fields",
      },
      {
        normalizedDetailUrl: "https://private.example.test/coins/broken-detail",
        status: "quarantined",
        quarantineReason: "extraction_failure",
      },
    ]);
    expect(
      storedJobs.filter((job) => job.kind === ACCEPT_COIN_CANDIDATE_JOB_KIND),
    ).toHaveLength(1);
  });

  it("stores one listing page, fans out deterministic detail jobs, resumes from a saved cursor, and keeps inspection privacy-safe", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const firstRunId = randomUUID();
    const secondRunId = randomUUID();
    const sourceConfigPath = await writeSeedSourceFile(
      "fixture-catalog",
      "HTTPS://private.example.test/coins?b=2&a=1#top",
    );

    await executeCli(["seed-sources", "--file", sourceConfigPath], { databaseUrl });

    const createOutput = await executeCli(
      [
        "create-run",
        "--run-id",
        firstRunId,
        "--source-id",
        SEEDED_SOURCE_ID,
        "--scope",
        "issuer_scope",
        "--detail-limit",
        "12",
      ],
      { databaseUrl },
    );

    expect(JSON.parse(createOutput)).toMatchObject({
      runId: firstRunId,
      sourceId: SEEDED_SOURCE_ID,
      status: "queued",
    });

    const firstRunWorkerOutputs = await runWorkerUntilEmpty(databaseUrl);
    expect(firstRunWorkerOutputs.at(0)).toMatchObject({
      processed: 1,
      runId: firstRunId,
      kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
    });

    const createSecondRunOutput = await executeCli(
      [
        "create-run",
        "--run-id",
        secondRunId,
        "--source-id",
        SEEDED_SOURCE_ID,
        "--scope",
        "issuer_scope",
        "--detail-limit",
        "10",
      ],
      { databaseUrl },
    );

    expect(JSON.parse(createSecondRunOutput)).toMatchObject({
      runId: secondRunId,
      sourceId: SEEDED_SOURCE_ID,
      status: "queued",
    });

    const secondRunWorkerOutputs = await runWorkerUntilEmpty(databaseUrl);
    expect(secondRunWorkerOutputs.at(0)).toMatchObject({
      processed: 1,
      runId: secondRunId,
      kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
    });

    const inspectOutput = await executeCli(["inspect-run", "--run-id", firstRunId], {
      databaseUrl,
    });
    const debugInspectOutput = await executeCli(
      ["inspect-run", "--run-id", firstRunId, "--debug-private"],
      { databaseUrl },
    );

    const firstRunJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, firstRunId))
      .orderBy(asc(jobs.scheduledAt), asc(jobs.createdAt));
    const secondRunJobs = await db
      .select()
      .from(jobs)
      .where(eq(jobs.crawlRunId, secondRunId))
      .orderBy(asc(jobs.scheduledAt), asc(jobs.createdAt));
    const storedPages = await db
      .select()
      .from(rawSourcePages)
      .where(eq(rawSourcePages.crawlRunId, firstRunId))
      .orderBy(asc(rawSourcePages.fetchedAt));
    const [storedSource] = await db
      .select()
      .from(sources)
      .where(eq(sources.id, SEEDED_SOURCE_ID));
    const [firstRun] = await db
      .select()
      .from(crawlRuns)
      .where(eq(crawlRuns.id, firstRunId));
    const [secondRun] = await db
      .select()
      .from(crawlRuns)
      .where(eq(crawlRuns.id, secondRunId));

    const detailJobUrls = firstRunJobs
      .filter(
        (job) =>
          job.kind === FETCH_RAW_SOURCE_PAGE_JOB_KIND &&
          job.payload.requestUrl !== "HTTPS://private.example.test/coins?b=2&a=1#top",
      )
      .map((job) => job.payload.requestUrl);
    const secondRunDetailJobUrls = secondRunJobs
      .filter(
        (job) =>
          job.kind === FETCH_RAW_SOURCE_PAGE_JOB_KIND &&
          job.payload.requestUrl !== "HTTPS://private.example.test/coins?b=2&a=1#top",
      )
      .map((job) => job.payload.requestUrl);

    expect(firstRunJobs).toHaveLength(31);
    expect(firstRunJobs[0]).toMatchObject({
      kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
      status: JOB_STATUS.completed,
      attempts: 1,
      lockedAt: null,
      lockToken: null,
    });
    expect(firstRunJobs[0].payload).toMatchObject({
      sourceId: SEEDED_SOURCE_ID,
      requestUrl: "HTTPS://private.example.test/coins?b=2&a=1#top",
      pageRole: "listing",
    });
    expect(detailJobUrls).toEqual([
      "https://private.example.test/coins/001?a=1&b=2",
      "https://private.example.test/coins/002?letter=b&view=full",
      "https://private.example.test/coins/003",
      "https://private.example.test/coins/004?edition=proof",
      "https://private.example.test/coins/005?ref=alpha",
      "https://private.example.test/coins/006?ref=beta",
      "https://private.example.test/coins/007",
      "https://private.example.test/coins/008?series=gold",
      "https://private.example.test/coins/009?series=silver",
      "https://private.example.test/coins/010",
    ]);
    expect(secondRunJobs).toHaveLength(7);
    expect(secondRunDetailJobUrls).toEqual([
      "https://private.example.test/coins/011",
      "https://private.example.test/coins/012?finish=matte",
    ]);
    expect(storedPages).toHaveLength(11);
    expect(storedPages[0].providerPayload).toMatchObject({
      fixtureId: "fixture-catalog",
      mode: "fake",
    });
    expect(storedPages[0].content).toContain("Catalog Listing");
    expect(storedPages[0].originalUrl).toBe(
      "HTTPS://private.example.test/coins?b=2&a=1#top",
    );
    expect(storedPages[0].normalizedUrl).toBe(
      "https://private.example.test/coins?a=1&b=2",
    );
    expect(storedPages[0].pageType).toBe("listing");
    expect(storedSource?.config).toMatchObject({
      adapter: "fake",
      fixtureId: "fixture-catalog",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "HTTPS://private.example.test/coins?b=2&a=1#top",
    });
    expect(firstRun?.cursor).toMatchObject({
      nextDetailIndex: 10,
      totalDetailLinks: 12,
    });
    expect(secondRun?.cursor).toMatchObject({
      nextDetailIndex: 12,
      totalDetailLinks: 12,
    });

    expect(inspectOutput).toContain(`run ${firstRunId}`);
    expect(inspectOutput).toContain(`source ${SEEDED_SOURCE_ID}`);
    expect(inspectOutput).toContain("status completed");
    expect(inspectOutput).toContain("raw_pages 11");
    expect(inspectOutput).toContain("candidates accepted=0 quarantined=10");
    expect(inspectOutput).toContain("page_type=listing");
    expect(inspectOutput).toContain("cursor next_detail_index=10 total_detail_links=12");
    expect(inspectOutput).not.toContain("Private Source Name");
    expect(inspectOutput).not.toContain("private.example.test");
    expect(inspectOutput).not.toContain("https://private.example.test/coins?a=1&b=2");
    expect(inspectOutput).not.toContain("Catalog Listing");
    expect(debugInspectOutput).toContain("source_name Private Source Name");
    expect(debugInspectOutput).toContain("source_domain private.example.test");
    expect(debugInspectOutput).toContain("start_url HTTPS://private.example.test/coins?b=2&a=1#top");
    expect(debugInspectOutput).toContain("url https://private.example.test/coins?a=1&b=2");
    expect(debugInspectOutput).toContain("title Catalog Listing");
  });

  it("integrates Firecrawl behind the crawl provider boundary, retries transient page failures, and isolates failed pages from the run", async () => {
    const { databaseUrl, db } = await createDatabaseUrl();
    const runId = randomUUID();
    const imageProviderFactory = createStubImageProviderFactory();
    const sourceConfigPath = await writeSeedSourceFile({
      adapter: "firecrawl",
      apiKey: "fc-test-key",
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
      ratePolicy: {
        minDelayMs: 0,
        backoffBaseMs: 0,
        attemptLimit: 2,
      },
    });

    const scrapeAttempts = new Map<string, number>();
    const firecrawlClientFactory = () => ({
      async scrapeUrl({ url }: { url: string }) {
        const nextAttempt = (scrapeAttempts.get(url) ?? 0) + 1;
        scrapeAttempts.set(url, nextAttempt);

        if (url === "https://private.example.test/coins") {
          return {
            requestId: "req-listing",
            data: {
              html: `
<html>
  <body>
    <section data-page-kind="listing">
      <h1>Live Listing</h1>
    </section>
  </body>
</html>`.trim(),
              links: [
                "https://private.example.test/coins/accepted-coin",
                "https://private.example.test/coins/failed-coin",
              ],
              metadata: {
                statusCode: 200,
                title: "Live Listing",
              },
            },
          };
        }

        if (url === "https://private.example.test/coins/accepted-coin") {
          return {
            requestId: "req-accepted",
            data: {
              html: `
<html>
  <body>
    <article data-page-kind="coin-detail" data-image-url="https://private.example.test/images/accepted-coin.jpg">
      <h1>Accepted Firecrawl Coin</h1>
      <p>Issuer: Example Issuer</p>
      <p>Denomination: 1 Unit</p>
      <p>Year: 1901</p>
    </article>
  </body>
</html>`.trim(),
              links: [],
              metadata: {
                statusCode: 200,
                title: "Accepted Firecrawl Coin",
              },
            },
          };
        }

        if (url === "https://private.example.test/coins/failed-coin") {
          throw Object.assign(new Error(`rate limited on attempt ${nextAttempt}`), {
            statusCode: 429,
            code: "RATE_LIMITED",
            requestId: `req-failed-${nextAttempt}`,
          });
        }

        throw new Error(`unexpected scrape url: ${url}`);
      },
    });

    const seedOutput = await executeCli(["seed-sources", "--file", sourceConfigPath], {
      databaseUrl,
      firecrawlClientFactory,
      imageProviderFactory,
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
      { databaseUrl, firecrawlClientFactory, imageProviderFactory },
    );

    expect(JSON.parse(createOutput)).toMatchObject({
      runId,
      sourceId: SEEDED_SOURCE_ID,
      status: "queued",
    });

    const workerOutputs = await runWorkerUntilEmpty(databaseUrl, {
      firecrawlClientFactory,
      imageProviderFactory,
    });
    expect(workerOutputs.some((output) => output.status === "failed")).toBe(true);

    const [run] = await db.select().from(crawlRuns).where(eq(crawlRuns.id, runId));
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

    expect(run?.status).toBe("completed");
    expect(scrapeAttempts).toEqual(
      new Map([
        ["https://private.example.test/coins", 1],
        ["https://private.example.test/coins/accepted-coin", 1],
        ["https://private.example.test/coins/failed-coin", 2],
      ]),
    );
    expect(storedPages).toHaveLength(2);
    expect(storedPages[0].providerPayload).toMatchObject({
      adapter: "firecrawl",
      requestId: "req-listing",
      statusCode: 200,
      links: [
        "https://private.example.test/coins/accepted-coin",
        "https://private.example.test/coins/failed-coin",
      ],
      metadata: {
        title: "Live Listing",
      },
    });
    expect(storedPages[1].providerPayload).toMatchObject({
      adapter: "firecrawl",
      requestId: "req-accepted",
      statusCode: 200,
      links: [],
      metadata: {
        title: "Accepted Firecrawl Coin",
      },
    });
    expect(
      storedJobs.filter((job) => job.kind === FETCH_RAW_SOURCE_PAGE_JOB_KIND).map((job) => ({
        requestUrl: job.payload.requestUrl,
        attempts: job.attempts,
        status: job.status,
      })),
    ).toEqual([
      {
        requestUrl: "https://private.example.test/coins",
        attempts: 1,
        status: JOB_STATUS.completed,
      },
      {
        requestUrl: "https://private.example.test/coins/accepted-coin",
        attempts: 1,
        status: JOB_STATUS.completed,
      },
      {
        requestUrl: "https://private.example.test/coins/failed-coin",
        attempts: 2,
        status: JOB_STATUS.failed,
      },
    ]);
    expect(storedJobs.find((job) => job.payload.requestUrl === "https://private.example.test/coins/failed-coin")?.errorPayload).toMatchObject({
      code: "RATE_LIMITED",
      statusCode: 429,
      requestId: "req-failed-2",
      retryable: true,
    });
    expect(storedCandidates).toHaveLength(1);
    expect(storedAcceptedCoins).toHaveLength(1);
  });
});
