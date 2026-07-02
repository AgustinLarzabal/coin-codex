import { createHash, randomUUID } from "node:crypto";

import { and, asc, eq, lte, sql } from "drizzle-orm";

import type { Database } from "../db/setup.js";
import {
  acceptedCoinImages,
  acceptedCoins,
  coinCandidates,
  crawlRuns,
  jobs,
  rawSourcePages,
} from "../db/schema.js";
import {
  ACCEPT_COIN_CANDIDATE_JOB_KIND,
  COIN_CANDIDATE_STATUS,
  CRAWL_RUN_STATUS,
  DEFAULT_JOB_MAX_ATTEMPTS,
  DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
  EXTRACT_COIN_CANDIDATE_JOB_KIND,
  FETCH_RAW_SOURCE_PAGE_JOB_KIND,
  JOB_STATUS,
  MAX_DETAIL_PAGE_LIMIT,
  QUARANTINE_REASON,
  RAW_PAGE_TYPE,
  type AcceptCoinCandidatePayload,
  type DownloadAcceptedCoinImagePayload,
  type ExtractCoinCandidatePayload,
  type FetchRawSourcePagePayload,
} from "./ingestion.js";
import { extractCoinCandidate, type ExtractedCoinCandidate } from "./extraction.js";
import {
  classifyRawPage,
  extractDetailLinks,
  normalizeUrl,
  readStoredCursor,
} from "./page-processing.js";
import type { CrawlProvider } from "./providers/crawl-provider.js";

function sha256(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

function getRunStatus(jobStatuses: string[]): string {
  if (jobStatuses.includes(JOB_STATUS.failed)) {
    return CRAWL_RUN_STATUS.failed;
  }

  if (jobStatuses.includes(JOB_STATUS.queued) || jobStatuses.includes(JOB_STATUS.running)) {
    return CRAWL_RUN_STATUS.queued;
  }

  return CRAWL_RUN_STATUS.completed;
}

function buildCandidateFingerprint(candidate: ExtractedCoinCandidate): string | null {
  const { issuer, denomination, issuedFromYear, issuedToYear } = candidate;
  if (!issuer || !denomination || issuedFromYear === null || issuedToYear === null) {
    return null;
  }

  return sha256(
    [issuer.toLowerCase(), denomination.toLowerCase(), issuedFromYear, issuedToYear].join("|"),
  );
}

function getQuarantineReason(candidate: typeof coinCandidates.$inferSelect): string | null {
  if (candidate.pageType !== "coin-detail") {
    return QUARANTINE_REASON.unrecognizedPageType;
  }

  if (
    candidate.issuedFromYear !== null &&
    candidate.issuedToYear !== null &&
    candidate.issuedFromYear > candidate.issuedToYear
  ) {
    return QUARANTINE_REASON.invalidYearRange;
  }

  if (
    !candidate.issuer ||
    !candidate.denomination ||
    candidate.issuedFromYear === null ||
    candidate.issuedToYear === null
  ) {
    return QUARANTINE_REASON.missingIdentityFields;
  }

  return null;
}

function assertAcceptedCandidate(
  candidate: typeof coinCandidates.$inferSelect,
): asserts candidate is typeof coinCandidates.$inferSelect & {
  issuedFromYear: number;
  issuedToYear: number;
  fingerprint: string;
} {
  if (
    candidate.issuedFromYear === null ||
    candidate.issuedToYear === null ||
    candidate.fingerprint === null
  ) {
    throw new Error(`accepted candidate missing required fields: ${candidate.id}`);
  }
}

export class Worker {
  constructor(
    private readonly db: Database,
    private readonly crawlProvider: CrawlProvider,
  ) {}

  private async enqueueJob(
    crawlRunId: string,
    kind: string,
    payload: Record<string, unknown>,
    scheduledAt = new Date(),
  ) {
    await this.db.insert(jobs).values({
      id: randomUUID(),
      crawlRunId,
      kind,
      status: JOB_STATUS.queued,
      attempts: 0,
      maxAttempts: DEFAULT_JOB_MAX_ATTEMPTS,
      scheduledAt,
      availableAt: scheduledAt,
      payload,
    });
  }

  private async syncRunStatus(crawlRunId: string) {
    const runJobs = await this.db
      .select({ status: jobs.status })
      .from(jobs)
      .where(eq(jobs.crawlRunId, crawlRunId));

    await this.db
      .update(crawlRuns)
      .set({
        status: getRunStatus(runJobs.map((job) => job.status)),
        updatedAt: new Date(),
      })
      .where(eq(crawlRuns.id, crawlRunId));
  }

  private async handleFetch(jobId: string, crawlRunId: string, payload: FetchRawSourcePagePayload) {
    const page = await this.crawlProvider.fetchPage({
      fixtureId: payload.fixtureId,
      requestUrl: payload.requestUrl,
    });
    const originalUrl = page.originalUrl || payload.originalUrl || payload.requestUrl;
    const normalizedUrl = page.normalizedUrl || normalizeUrl(originalUrl);
    const pageType = classifyRawPage(page.content);

    const rawSourcePageId = randomUUID();
    await this.db.insert(rawSourcePages).values({
      id: rawSourcePageId,
      crawlRunId,
      sourceId: payload.sourceId,
      jobId,
      originalUrl,
      normalizedUrl,
      urlHash: sha256(normalizedUrl),
      pageType,
      content: page.content,
      contentHash: sha256(page.content),
      providerPayload: page.providerPayload,
    });

    if (payload.pageRole === "listing") {
      await this.enqueueDetailJobs(crawlRunId, payload, originalUrl, normalizedUrl, page.content);
    }

    if (pageType === RAW_PAGE_TYPE.detail) {
      await this.enqueueJob(crawlRunId, EXTRACT_COIN_CANDIDATE_JOB_KIND, {
        sourceId: payload.sourceId,
        rawSourcePageId,
      });
    }
  }

  private async enqueueDetailJobs(
    crawlRunId: string,
    payload: FetchRawSourcePagePayload,
    originalUrl: string,
    normalizedUrl: string,
    content: string,
  ) {
    const detailLinks = extractDetailLinks(content, originalUrl);
    const storedCursor = readStoredCursor(payload.cursor) ?? {
      nextDetailIndex: 0,
      totalDetailLinks: 0,
      listingNormalizedUrl: normalizedUrl,
    };
    const nextIndex = Math.max(0, storedCursor.nextDetailIndex);
    const detailLimit = Math.min(payload.detailLimit, MAX_DETAIL_PAGE_LIMIT);
    const selectedLinks = detailLinks.slice(nextIndex, nextIndex + detailLimit);
    const now = Date.now();

    for (const [index, link] of selectedLinks.entries()) {
      const scheduledAt = new Date(now + index);
      await this.enqueueJob(
        crawlRunId,
        FETCH_RAW_SOURCE_PAGE_JOB_KIND,
        {
          sourceId: payload.sourceId,
          fixtureId: payload.fixtureId,
          requestUrl: link.normalizedUrl,
          originalUrl: link.originalUrl,
          pageRole: "detail",
          detailLimit: payload.detailLimit,
          cursor: {
            nextDetailIndex: 0,
            totalDetailLinks: 0,
            listingNormalizedUrl: normalizedUrl,
          },
        },
        scheduledAt,
      );
    }

    await this.db
      .update(crawlRuns)
      .set({
        cursor: {
          nextDetailIndex: nextIndex + selectedLinks.length,
          totalDetailLinks: detailLinks.length,
          listingNormalizedUrl: normalizedUrl,
        },
        updatedAt: new Date(),
      })
      .where(eq(crawlRuns.id, crawlRunId));
  }

  private async handleExtract(crawlRunId: string, payload: ExtractCoinCandidatePayload) {
    const [page] = await this.db
      .select()
      .from(rawSourcePages)
      .where(eq(rawSourcePages.id, payload.rawSourcePageId));
    if (!page) {
      throw new Error(`raw source page not found: ${payload.rawSourcePageId}`);
    }

    const extracted = extractCoinCandidate(page.content);
    const candidateId = randomUUID();
    await this.db.insert(coinCandidates).values({
      id: candidateId,
      crawlRunId,
      sourceId: payload.sourceId,
      rawSourcePageId: page.id,
      normalizedDetailUrl: page.normalizedUrl,
      detailUrlHash: page.urlHash,
      pageType: extracted.pageType,
      title: extracted.title,
      issuer: extracted.issuer,
      denomination: extracted.denomination,
      rawDateText: extracted.rawDateText,
      issuedFromYear: extracted.issuedFromYear,
      issuedToYear: extracted.issuedToYear,
      imageUrl: extracted.imageUrl ?? null,
      fingerprint: buildCandidateFingerprint(extracted),
      status: COIN_CANDIDATE_STATUS.pending,
      quarantineReason: null,
    });

    await this.enqueueJob(crawlRunId, ACCEPT_COIN_CANDIDATE_JOB_KIND, {
      sourceId: payload.sourceId,
      candidateId,
    });
  }

  private async handleAccept(crawlRunId: string, payload: AcceptCoinCandidatePayload) {
    const [candidate] = await this.db
      .select()
      .from(coinCandidates)
      .where(eq(coinCandidates.id, payload.candidateId));
    if (!candidate) {
      throw new Error(`coin candidate not found: ${payload.candidateId}`);
    }

    let quarantineReason = getQuarantineReason(candidate);
    if (!quarantineReason) {
      const [existingAcceptedCoin] = await this.db
        .select()
        .from(acceptedCoins)
        .where(eq(acceptedCoins.sourceDetailUrlHash, candidate.detailUrlHash));
      if (existingAcceptedCoin) {
        quarantineReason = QUARANTINE_REASON.duplicateSourceDetailUrl;
      }
    }

    if (quarantineReason) {
      await this.db
        .update(coinCandidates)
        .set({
          status: COIN_CANDIDATE_STATUS.quarantined,
          quarantineReason,
        })
        .where(eq(coinCandidates.id, candidate.id));
      return;
    }

    assertAcceptedCandidate(candidate);

    const acceptedCoinId = randomUUID();
    await this.db.insert(acceptedCoins).values({
      id: acceptedCoinId,
      crawlRunId,
      candidateId: candidate.id,
      sourceId: payload.sourceId,
      sourceDetailUrl: candidate.normalizedDetailUrl,
      sourceDetailUrlHash: candidate.detailUrlHash,
      issuer: candidate.issuer,
      denomination: candidate.denomination,
      issuedFromYear: candidate.issuedFromYear,
      issuedToYear: candidate.issuedToYear,
      fingerprint: candidate.fingerprint,
    });

    await this.db
      .update(coinCandidates)
      .set({
        status: COIN_CANDIDATE_STATUS.accepted,
        quarantineReason: null,
      })
      .where(eq(coinCandidates.id, candidate.id));

    if (candidate.imageUrl) {
      await this.enqueueJob(crawlRunId, DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND, {
        sourceId: payload.sourceId,
        acceptedCoinId,
        imageUrl: candidate.imageUrl,
      });
    }
  }

  private async handleImage(crawlRunId: string, payload: DownloadAcceptedCoinImagePayload) {
    const [acceptedCoin] = await this.db
      .select()
      .from(acceptedCoins)
      .where(eq(acceptedCoins.id, payload.acceptedCoinId));
    if (!acceptedCoin) {
      throw new Error(`accepted coin not found: ${payload.acceptedCoinId}`);
    }

    await this.db.insert(acceptedCoinImages).values({
      id: randomUUID(),
      crawlRunId,
      acceptedCoinId: acceptedCoin.id,
      sourceId: payload.sourceId,
      sourceImageUrl: payload.imageUrl,
      sourceImageUrlHash: sha256(payload.imageUrl),
      contentHash: sha256(`image:${payload.imageUrl}`),
    });
  }

  async runOnce() {
    const now = new Date();
    const [job] = await this.db
      .select()
      .from(jobs)
      .where(and(eq(jobs.status, JOB_STATUS.queued), lte(jobs.availableAt, now)))
      .orderBy(asc(jobs.scheduledAt), asc(jobs.createdAt))
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
      switch (job.kind) {
        case FETCH_RAW_SOURCE_PAGE_JOB_KIND:
          await this.handleFetch(
            job.id,
            job.crawlRunId,
            job.payload as FetchRawSourcePagePayload,
          );
          break;
        case EXTRACT_COIN_CANDIDATE_JOB_KIND:
          await this.handleExtract(
            job.crawlRunId,
            job.payload as ExtractCoinCandidatePayload,
          );
          break;
        case ACCEPT_COIN_CANDIDATE_JOB_KIND:
          await this.handleAccept(
            job.crawlRunId,
            job.payload as AcceptCoinCandidatePayload,
          );
          break;
        case DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND:
          await this.handleImage(
            job.crawlRunId,
            job.payload as DownloadAcceptedCoinImagePayload,
          );
          break;
        default:
          throw new Error(`unsupported job kind: ${job.kind}`);
      }

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

      await this.syncRunStatus(job.crawlRunId);

      return { processed: 1, jobId: job.id, runId: job.crawlRunId, kind: job.kind };
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

      await this.syncRunStatus(job.crawlRunId);

      return {
        processed: 1,
        jobId: job.id,
        runId: job.crawlRunId,
        kind: job.kind,
        status: shouldRetry ? "queued" : "failed",
      };
    }
  }
}
