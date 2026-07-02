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
  sources,
} from "../db/schema.js";
import {
  ACCEPT_COIN_CANDIDATE_JOB_KIND,
  COIN_CANDIDATE_STATUS,
  CRAWL_RUN_STATUS,
  clampDetailLimit,
  createCrawlCursor,
  DEFAULT_JOB_MAX_ATTEMPTS,
  DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
  EXTRACT_COIN_CANDIDATE_JOB_KIND,
  FETCH_RAW_SOURCE_PAGE_JOB_KIND,
  JOB_STATUS,
  QUARANTINE_REASON,
  type AcceptCoinCandidatePayload,
  type DownloadAcceptedCoinImagePayload,
  type ExtractCoinCandidatePayload,
  type FetchRawSourcePagePayload,
} from "./ingestion.js";
import { extractCoinCandidate, type ExtractedCoinCandidate } from "./extraction.js";
import {
  classifyRawPage,
  type DetailLink,
  extractDetailLinks,
  normalizeUrl,
  readStoredCursor,
} from "./page-processing.js";
import { CrawlProviderError, type CrawlProvider } from "./providers/crawl-provider.js";
import {
  parseSourceConfig,
  readSourceAttemptLimit,
  readSourceFetchDelayMs,
  readSourceRetryBackoffMs,
  type SourceConfig,
} from "./source-config.js";

function sha256(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

function getRunStatus(jobStatuses: string[]): string {
  if (jobStatuses.includes(JOB_STATUS.queued) || jobStatuses.includes(JOB_STATUS.running)) {
    return CRAWL_RUN_STATUS.queued;
  }

  return CRAWL_RUN_STATUS.completed;
}

function buildCandidateFingerprint(candidate: ExtractedCoinCandidate): string | null {
  const {
    nameNormalized,
    issuerNormalized,
    denominationNormalized,
    issuedFromYear,
    issuedToYear,
    mintMark,
  } = candidate;
  if (
    !nameNormalized ||
    !issuerNormalized ||
    !denominationNormalized ||
    issuedFromYear === null ||
    issuedToYear === null
  ) {
    return null;
  }

  return sha256(
    [
      nameNormalized.toLowerCase(),
      issuerNormalized.toLowerCase(),
      denominationNormalized.toLowerCase(),
      issuedFromYear,
      issuedToYear,
      mintMark.toLowerCase(),
    ].join("|"),
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
    const sourceConfig = await this.readSourceConfig(payload.sourceId);
    const page = await this.crawlProvider.fetchPage({
      sourceConfig,
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
      await this.enqueueDetailJobs(
        crawlRunId,
        payload,
        sourceConfig,
        originalUrl,
        normalizedUrl,
        page.content,
        page.extractedLinks,
      );
    }

    if (payload.pageRole === "detail") {
      await this.enqueueJob(crawlRunId, EXTRACT_COIN_CANDIDATE_JOB_KIND, {
        sourceId: payload.sourceId,
        rawSourcePageId,
      });
    }
  }

  private async readSourceConfig(sourceId: string): Promise<SourceConfig> {
    const [source] = await this.db.select().from(sources).where(eq(sources.id, sourceId));
    if (!source) {
      throw new Error(`source not found: ${sourceId}`);
    }

    return parseSourceConfig(source.config);
  }

  private async enqueueDetailJobs(
    crawlRunId: string,
    payload: FetchRawSourcePagePayload,
    sourceConfig: SourceConfig,
    originalUrl: string,
    normalizedUrl: string,
    content: string,
    extractedLinks: string[],
  ) {
    const detailLinks =
      extractedLinks.length > 0
        ? extractStoredDetailLinks(extractedLinks, originalUrl)
        : extractDetailLinks(content, originalUrl);
    const storedCursor = readStoredCursor(payload.cursor) ?? createCrawlCursor(normalizedUrl);
    const nextIndex = Math.max(0, storedCursor.nextDetailIndex);
    const detailLimit = clampDetailLimit(payload.detailLimit);
    const selectedLinks = detailLinks.slice(nextIndex, nextIndex + detailLimit);
    const now = Date.now();
    const fetchDelayMs = readSourceFetchDelayMs(sourceConfig);
    const scheduleStepMs = Math.max(fetchDelayMs, 1);

    if (selectedLinks.length > 0) {
      await this.db.insert(jobs).values(
        selectedLinks.map((link, index) => {
          const scheduledAt = new Date(now + index * scheduleStepMs);
          return this.buildDetailJob(
            crawlRunId,
            payload,
            sourceConfig,
            normalizedUrl,
            link,
            scheduledAt,
          );
        }),
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

  private buildDetailJob(
    crawlRunId: string,
    payload: FetchRawSourcePagePayload,
    sourceConfig: SourceConfig,
    listingNormalizedUrl: string,
    link: DetailLink,
    scheduledAt: Date,
  ) {
    return {
      id: randomUUID(),
      crawlRunId,
      kind: FETCH_RAW_SOURCE_PAGE_JOB_KIND,
      status: JOB_STATUS.queued,
      attempts: 0,
      maxAttempts: readSourceAttemptLimit(sourceConfig),
      scheduledAt,
      availableAt: scheduledAt,
      payload: {
        sourceId: payload.sourceId,
        requestUrl: link.normalizedUrl,
        originalUrl: link.originalUrl,
        pageRole: "detail" as const,
        detailLimit: payload.detailLimit,
        cursor: createCrawlCursor(listingNormalizedUrl),
      },
    };
  }

  private async handleExtract(crawlRunId: string, payload: ExtractCoinCandidatePayload) {
    const [page] = await this.db
      .select()
      .from(rawSourcePages)
      .where(eq(rawSourcePages.id, payload.rawSourcePageId));
    if (!page) {
      throw new Error(`raw source page not found: ${payload.rawSourcePageId}`);
    }

    try {
      const extracted = extractCoinCandidate(page.content);
      const candidateId = randomUUID();
      await this.db.insert(coinCandidates).values({
        id: candidateId,
        crawlRunId,
        sourceId: payload.sourceId,
        rawSourcePageId: page.id,
        originalDetailUrl: page.originalUrl,
        normalizedDetailUrl: page.normalizedUrl,
        detailUrlHash: page.urlHash,
        pageType: extracted.pageType,
        title: extracted.nameNormalized,
        nameRaw: extracted.nameRaw,
        nameNormalized: extracted.nameNormalized,
        issuer: extracted.issuerNormalized,
        issuerRaw: extracted.issuerRaw,
        issuerNormalized: extracted.issuerNormalized,
        denomination: extracted.denominationNormalized,
        denominationRaw: extracted.denominationRaw,
        denominationNormalized: extracted.denominationNormalized,
        rawDateText: extracted.rawDateText,
        issuedFromYear: extracted.issuedFromYear,
        issuedToYear: extracted.issuedToYear,
        mintMark: extracted.mintMark,
        imageUrl: extracted.imageUrl ?? null,
        fingerprint: buildCandidateFingerprint(extracted),
        status: COIN_CANDIDATE_STATUS.pending,
        quarantineReason: null,
      });

      await this.enqueueJob(crawlRunId, ACCEPT_COIN_CANDIDATE_JOB_KIND, {
        sourceId: payload.sourceId,
        candidateId,
      });
    } catch {
      await this.db.insert(coinCandidates).values({
        id: randomUUID(),
        crawlRunId,
        sourceId: payload.sourceId,
        rawSourcePageId: page.id,
        originalDetailUrl: page.originalUrl,
        normalizedDetailUrl: page.normalizedUrl,
        detailUrlHash: page.urlHash,
        pageType: page.pageType,
        title: "",
        nameRaw: "",
        nameNormalized: "",
        issuer: "",
        issuerRaw: "",
        issuerNormalized: "",
        denomination: "",
        denominationRaw: "",
        denominationNormalized: "",
        rawDateText: "",
        issuedFromYear: null,
        issuedToYear: null,
        mintMark: "",
        imageUrl: null,
        fingerprint: null,
        status: COIN_CANDIDATE_STATUS.quarantined,
        quarantineReason: QUARANTINE_REASON.extractionFailure,
      });
    }
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
        .where(
          and(
            eq(acceptedCoins.sourceId, payload.sourceId),
            eq(acceptedCoins.sourceDetailUrlHash, candidate.detailUrlHash),
          ),
        );
      if (existingAcceptedCoin) {
        quarantineReason = QUARANTINE_REASON.duplicateSourceDetailUrl;
      }
    }

    if (!quarantineReason && candidate.fingerprint) {
      const [existingFingerprintMatch] = await this.db
        .select()
        .from(acceptedCoins)
        .where(eq(acceptedCoins.fingerprint, candidate.fingerprint));
      if (existingFingerprintMatch) {
        quarantineReason = QUARANTINE_REASON.duplicateFingerprint;
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
      name: candidate.nameNormalized,
      issuer: candidate.issuer,
      denomination: candidate.denomination,
      issuedFromYear: candidate.issuedFromYear,
      issuedToYear: candidate.issuedToYear,
      mintMark: candidate.mintMark,
      fingerprint: candidate.fingerprint,
      acceptedAt: new Date(),
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
      const sourceId = readJobSourceId(job.payload);
      const sourceConfig = sourceId ? await this.readSourceConfig(sourceId) : null;
      const shouldRetry = shouldRetryJob(job.kind, nextAttempt, job.maxAttempts, error);
      const retryDelayMs = readJobRetryDelayMs(job.kind, sourceConfig, nextAttempt);
      await this.db
        .update(jobs)
        .set({
          status: shouldRetry ? JOB_STATUS.queued : JOB_STATUS.failed,
          availableAt: new Date(Date.now() + retryDelayMs),
          lockedAt: null,
          lockToken: null,
          errorPayload: buildJobErrorPayload(error),
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

function extractStoredDetailLinks(urls: string[], baseUrl: string): DetailLink[] {
  const seenNormalizedUrls = new Set<string>();
  const detailLinks: DetailLink[] = [];

  for (const url of urls) {
    const originalUrl = new URL(url, baseUrl).toString();
    const normalizedUrl = normalizeUrl(originalUrl);
    if (seenNormalizedUrls.has(normalizedUrl)) {
      continue;
    }

    seenNormalizedUrls.add(normalizedUrl);
    detailLinks.push({ originalUrl, normalizedUrl });
  }

  return detailLinks;
}

function buildJobErrorPayload(error: unknown): Record<string, unknown> {
  if (error instanceof CrawlProviderError) {
    return {
      message: error.message,
      code: error.details.code,
      retryable: error.details.retryable,
      statusCode: error.details.statusCode ?? null,
      requestId: error.details.requestId ?? null,
      providerPayload: error.details.providerPayload ?? null,
    };
  }

  return {
    message: error instanceof Error ? error.message : String(error),
  };
}

function readJobSourceId(payload: Record<string, unknown>): string | null {
  return typeof payload.sourceId === "string" ? payload.sourceId : null;
}

function shouldRetryJob(
  jobKind: string,
  nextAttempt: number,
  maxAttempts: number,
  error: unknown,
): boolean {
  if (nextAttempt >= maxAttempts) {
    return false;
  }

  if (jobKind !== FETCH_RAW_SOURCE_PAGE_JOB_KIND) {
    return true;
  }

  return error instanceof CrawlProviderError && error.details.retryable;
}

function readJobRetryDelayMs(
  jobKind: string,
  sourceConfig: SourceConfig | null,
  nextAttempt: number,
): number {
  if (jobKind === FETCH_RAW_SOURCE_PAGE_JOB_KIND && sourceConfig) {
    return readSourceRetryBackoffMs(sourceConfig, nextAttempt);
  }

  return nextAttempt * 1_000;
}
