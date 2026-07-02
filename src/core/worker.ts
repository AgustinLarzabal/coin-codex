import { createHash, randomUUID } from "node:crypto";

import { and, asc, eq, lte, sql } from "drizzle-orm";

import type { Database } from "../db/setup.js";
import {
  acceptedCoins,
  acceptedCoinImages,
  coinCandidates,
  crawlRuns,
  jobs,
  rawSourcePages,
} from "../db/schema.js";
import {
  ACCEPT_COIN_CANDIDATE_JOB_KIND,
  CRAWL_RUN_STATUS,
  DEFAULT_JOB_MAX_ATTEMPTS,
  DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND,
  EXTRACT_COIN_CANDIDATE_JOB_KIND,
  type AcceptCoinCandidatePayload,
  type DownloadAcceptedCoinImagePayload,
  type ExtractCoinCandidatePayload,
  type FetchRawSourcePagePayload,
  FETCH_RAW_SOURCE_PAGE_JOB_KIND,
  JOB_STATUS,
} from "./ingestion.js";
import { classifyPage, extractCoinCandidate, extractListingLinks } from "./extraction.js";
import type { CrawlProvider } from "./providers/crawl-provider.js";

function sha256(input: string): string {
  return createHash("sha256").update(input).digest("hex");
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
  ) {
    const scheduledAt = new Date();
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
    const runJobs = await this.db.select().from(jobs).where(eq(jobs.crawlRunId, crawlRunId));
    const hasFailed = runJobs.some((job) => job.status === JOB_STATUS.failed);
    const hasPending = runJobs.some(
      (job) => job.status === JOB_STATUS.queued || job.status === JOB_STATUS.running,
    );

    await this.db
      .update(crawlRuns)
      .set({
        status: hasFailed
          ? CRAWL_RUN_STATUS.failed
          : hasPending
            ? CRAWL_RUN_STATUS.queued
            : CRAWL_RUN_STATUS.completed,
        updatedAt: new Date(),
      })
      .where(eq(crawlRuns.id, crawlRunId));
  }

  private async handleFetch(jobId: string, crawlRunId: string, payload: FetchRawSourcePagePayload) {
    const page = await this.crawlProvider.fetchPage({
      fixtureId: payload.fixtureId,
      requestUrl: payload.requestUrl,
    });

    const rawSourcePageId = randomUUID();
    await this.db.insert(rawSourcePages).values({
      id: rawSourcePageId,
      crawlRunId,
      sourceId: payload.sourceId,
      jobId,
      normalizedUrl: page.normalizedUrl,
      urlHash: sha256(page.normalizedUrl),
      content: page.content,
      contentHash: sha256(page.content),
      providerPayload: page.providerPayload,
    });

    const pageType = classifyPage(page.content);
    if (pageType === "listing") {
      for (const detailUrl of extractListingLinks(page.content).slice(0, 10)) {
        await this.enqueueJob(crawlRunId, FETCH_RAW_SOURCE_PAGE_JOB_KIND, {
          sourceId: payload.sourceId,
          fixtureId: payload.fixtureId,
          requestUrl: detailUrl,
        });
      }
    }

    if (pageType === "coin-detail") {
      await this.enqueueJob(crawlRunId, EXTRACT_COIN_CANDIDATE_JOB_KIND, {
        sourceId: payload.sourceId,
        rawSourcePageId,
      });
    }
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
    const fingerprint =
      extracted.issuer && extracted.denomination && extracted.issuedFromYear && extracted.issuedToYear
        ? sha256(
            [
              extracted.issuer.toLowerCase(),
              extracted.denomination.toLowerCase(),
              extracted.issuedFromYear,
              extracted.issuedToYear,
            ].join("|"),
          )
        : null;

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
      fingerprint,
      status: "pending",
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

    let quarantineReason: string | null = null;
    if (candidate.pageType !== "coin-detail") {
      quarantineReason = "unrecognized_page_type";
    } else if (
      candidate.issuedFromYear !== null &&
      candidate.issuedToYear !== null &&
      candidate.issuedFromYear > candidate.issuedToYear
    ) {
      quarantineReason = "invalid_year_range";
    } else if (
      !candidate.issuer ||
      !candidate.denomination ||
      candidate.issuedFromYear === null ||
      candidate.issuedToYear === null
    ) {
      quarantineReason = "missing_identity_fields";
    } else {
      const [existingAcceptedCoin] = await this.db
        .select()
        .from(acceptedCoins)
        .where(eq(acceptedCoins.sourceDetailUrlHash, candidate.detailUrlHash));
      if (existingAcceptedCoin) {
        quarantineReason = "duplicate_source_detail_url";
      }
    }

    if (quarantineReason) {
      await this.db
        .update(coinCandidates)
        .set({
          status: "quarantined",
          quarantineReason,
        })
        .where(eq(coinCandidates.id, candidate.id));
      return;
    }

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
      issuedFromYear: candidate.issuedFromYear!,
      issuedToYear: candidate.issuedToYear!,
      fingerprint: candidate.fingerprint!,
    });

    await this.db
      .update(coinCandidates)
      .set({
        status: "accepted",
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
