import { randomUUID } from "node:crypto";

import { PGlite } from "@electric-sql/pglite";
import { afterEach, describe, expect, it } from "vitest";

import {
  COIN_CANDIDATE_STATUS,
  DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
  FETCH_RAW_SOURCE_PAGE_JOB_KIND,
  JOB_STATUS,
  RAW_PAGE_TYPE,
} from "../src/core/ingestion.js";
import { IngestionInspector } from "../src/core/inspector.js";
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

async function createDatabaseUrl() {
  const databaseUrl = `memory://${randomUUID()}`;
  const client = new PGlite();
  registerDatabase(databaseUrl, client);
  resources.push({ databaseUrl, close: () => client.close() });
  await migrate(client);
  const db = createDatabase(client);
  return { db };
}

afterEach(async () => {
  await Promise.all(
    resources.splice(0).map(async (resource) => {
      unregisterDatabase(resource.databaseUrl);
      await resource.close();
    }),
  );
});

describe("IngestionInspector", () => {
  it("builds a structured crawl run inspection model with privacy-gated details", async () => {
    const { db } = await createDatabaseUrl();
    const inspector = new IngestionInspector(db);
    const runId = "run-structured";
    const sourceId = "src-structured";
    const fetchJobId = "job-fetch";
    const imageJobId = "job-image";
    const pageId = "page-1";
    const acceptedCandidateId = "candidate-accepted";
    const quarantinedCandidateId = "candidate-quarantined";
    const acceptedCoinId = "accepted-1";
    const acceptedCoinImageId = "image-1";

    await db.insert(sources).values({
      id: sourceId,
      config: {
        adapter: "fake",
        fixtureId: "fixture-run",
        name: "Private Source Name",
        domain: "private.example.test",
        startUrl: "https://private.example.test/coins",
      },
    });
    await db.insert(crawlRuns).values({
      id: runId,
      sourceId,
      scope: "issuer_scope",
      status: "completed",
      detailLimit: 10,
      cursor: {
        nextDetailIndex: 1,
        totalDetailLinks: 2,
        listingNormalizedUrl: "https://private.example.test/coins",
      },
    });
    await db.insert(jobs).values([
      {
        id: fetchJobId,
        crawlRunId: runId,
        kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
        status: JOB_STATUS.completed,
        attempts: 1,
        maxAttempts: 3,
        scheduledAt: new Date("2026-07-03T00:00:00Z"),
        availableAt: new Date("2026-07-03T00:00:00Z"),
        payload: {
          sourceId,
          requestUrl: "https://private.example.test/coins",
          pageRole: "listing",
        },
      },
      {
        id: imageJobId,
        crawlRunId: runId,
        kind: DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
        status: JOB_STATUS.failed,
        attempts: 3,
        maxAttempts: 3,
        scheduledAt: new Date("2026-07-03T00:01:00Z"),
        availableAt: new Date("2026-07-03T00:01:00Z"),
        payload: {
          sourceId,
          acceptedCoinId,
          imageUrl: "https://private.example.test/images/coin.jpg",
        },
        errorPayload: {
          code: "IMAGE_TIMEOUT",
          retryable: true,
          statusCode: 504,
          message: "failed to download",
        },
      },
    ]);
    await db.insert(rawSourcePages).values({
      id: pageId,
      crawlRunId: runId,
      sourceId,
      jobId: fetchJobId,
      originalUrl: "https://private.example.test/coins",
      normalizedUrl: "https://private.example.test/coins",
      urlHash: "1234567890abcdef1234567890abcdef",
      pageType: RAW_PAGE_TYPE.listing,
      content: "<html><body><h1>Accepted Fixture Coin</h1></body></html>",
      contentHash: "abcdef1234567890abcdef1234567890",
      providerPayload: {
        fixtureId: "fixture-run",
      },
    });
    await db.insert(coinCandidates).values([
      {
        id: acceptedCandidateId,
        crawlRunId: runId,
        sourceId,
        rawSourcePageId: pageId,
        originalDetailUrl: "https://private.example.test/coins/accepted",
        normalizedDetailUrl: "https://private.example.test/coins/accepted",
        detailUrlHash: "accepted-detail-hash",
        pageType: "coin-detail",
        title: "Accepted Fixture Coin",
        nameRaw: "Accepted Fixture Coin",
        nameNormalized: "Accepted Fixture Coin",
        issuer: "Example Issuer",
        issuerRaw: "Example Issuer",
        issuerNormalized: "Example Issuer",
        denomination: "1 Unit",
        denominationRaw: "1 Unit",
        denominationNormalized: "1 Unit",
        rawDateText: "1901",
        issuedFromYear: 1901,
        issuedToYear: 1901,
        mintMark: "",
        imageUrl: "https://private.example.test/images/coin.jpg",
        fingerprint: "fingerprint-1",
        status: COIN_CANDIDATE_STATUS.accepted,
        quarantineReason: null,
      },
      {
        id: quarantinedCandidateId,
        crawlRunId: runId,
        sourceId,
        rawSourcePageId: pageId,
        originalDetailUrl: "https://private.example.test/coins/quarantine",
        normalizedDetailUrl: "https://private.example.test/coins/quarantine",
        detailUrlHash: "quarantine-detail-hash",
        pageType: "coin-detail",
        title: "Quarantined Fixture Coin",
        nameRaw: "Quarantined Fixture Coin",
        nameNormalized: "Quarantined Fixture Coin",
        issuer: "Example Issuer",
        issuerRaw: "Example Issuer",
        issuerNormalized: "Example Issuer",
        denomination: "",
        denominationRaw: "",
        denominationNormalized: "",
        rawDateText: "1905-1903",
        issuedFromYear: 1905,
        issuedToYear: 1903,
        mintMark: "",
        imageUrl: "https://private.example.test/images/quarantine.jpg",
        fingerprint: "fingerprint-2",
        status: COIN_CANDIDATE_STATUS.quarantined,
        quarantineReason: "invalid_year_range",
      },
    ]);
    await db.insert(acceptedCoins).values({
      id: acceptedCoinId,
      crawlRunId: runId,
      candidateId: acceptedCandidateId,
      sourceId,
      sourceDetailUrl: "https://private.example.test/coins/accepted",
      sourceDetailUrlHash: "accepted-detail-hash",
      name: "Accepted Fixture Coin",
      issuer: "Example Issuer",
      denomination: "1 Unit",
      issuedFromYear: 1901,
      issuedToYear: 1901,
      mintMark: "",
      fingerprint: "fingerprint-1",
    });
    await db.insert(acceptedCoinImages).values({
      id: acceptedCoinImageId,
      crawlRunId: runId,
      acceptedCoinId,
      rawSourcePageId: pageId,
      sourceId,
      sourceDetailUrlHash: "accepted-detail-hash",
      sourceImageUrl: "https://private.example.test/images/coin.jpg",
      sourceImageUrlHash: "accepted-image-hash",
      contentHash: "accepted-image-content-hash",
      duplicateOfAcceptedCoinImageId: null,
    });

    const publicModel = await inspector.inspectRunModel(runId);
    const privateModel = await inspector.inspectRunModel(runId, { debugPrivate: true });

    expect(publicModel).toMatchObject({
      run: {
        id: runId,
        sourceId,
        status: "completed",
      },
      jobs: {
        total: 2,
        summary: {
          total: 2,
          completed: 1,
          failed: 1,
          queued: 0,
          running: 0,
          retries: 2,
        },
        byStatus: {
          queued: 0,
          running: 0,
        },
        failureCount: 1,
      },
      rawPages: {
        total: 1,
        byType: {
          listing: 1,
          detail: 0,
          unknown: 0,
        },
      },
      candidates: {
        total: 2,
        accepted: 1,
        quarantined: 1,
      },
      acceptedCoins: {
        total: 1,
      },
      acceptedCoinImages: {
        total: 1,
      },
      imageJobs: {
        total: 1,
        stored: 1,
        summary: {
          total: 1,
          completed: 0,
          failed: 1,
          queued: 0,
          running: 0,
          retries: 2,
        },
      },
      cursor: {
        nextDetailIndex: 1,
        totalDetailLinks: 2,
      },
      jobKinds: [
        {
          kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
          summary: {
            total: 1,
            completed: 1,
            failed: 0,
            queued: 0,
            running: 0,
            retries: 0,
          },
        },
        {
          kind: DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
          summary: {
            total: 1,
            completed: 0,
            failed: 1,
            queued: 0,
            running: 0,
            retries: 2,
          },
        },
      ],
      quarantinedCandidates: [
        {
          id: quarantinedCandidateId,
          reason: "invalid_year_range",
        },
      ],
    });
    expect(publicModel.source).toEqual({
      id: sourceId,
    });
    expect(publicModel.jobs.details).toMatchObject([
      {
        id: fetchJobId,
        kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
        status: JOB_STATUS.completed,
        attempts: 1,
        lockToken: null,
        page: {
          id: pageId,
          pageType: RAW_PAGE_TYPE.listing,
          urlHash: "1234567890ab",
          contentHash: "abcdef123456",
        },
      },
      {
        id: imageJobId,
        kind: DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
        status: JOB_STATUS.failed,
        attempts: 3,
        lockToken: null,
        error: {
          code: "IMAGE_TIMEOUT",
          retryable: true,
          statusCode: 504,
        },
      },
    ]);
    expect(publicModel.jobs.details[0]?.page?.private).toBeUndefined();
    expect(publicModel.jobs.details[1]?.error?.private).toBeUndefined();

    expect(privateModel.source.private).toEqual({
      name: "Private Source Name",
      domain: "private.example.test",
      startUrl: "https://private.example.test/coins",
    });
    expect(privateModel.jobs.details[0]?.page?.private).toEqual({
      normalizedUrl: "https://private.example.test/coins",
      originalUrl: "https://private.example.test/coins",
      title: "Accepted Fixture Coin",
    });
    expect(privateModel.jobs.details[1]?.error?.private).toEqual({
      code: "IMAGE_TIMEOUT",
      retryable: true,
      statusCode: 504,
      message: "failed to download",
    });
  });
});
