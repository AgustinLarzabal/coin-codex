export const CRAWL_RUN_STATUS = {
  queued: "queued",
  completed: "completed",
  failed: "failed",
} as const;

export const JOB_STATUS = {
  queued: "queued",
  running: "running",
  completed: "completed",
  failed: "failed",
} as const;

export const FETCH_RAW_SOURCE_PAGE_JOB_KIND = "fetch_raw_source_page";
export const EXTRACT_COIN_CANDIDATE_JOB_KIND = "extract_coin_candidate";
export const ACCEPT_COIN_CANDIDATE_JOB_KIND = "accept_coin_candidate";
export const DOWNLOAD_ACCEPTED_COIN_IMAGE_JOB_KIND = "download_accepted_coin_image";
export const DEFAULT_JOB_MAX_ATTEMPTS = 3;
export const MAX_DETAIL_PAGE_LIMIT = 10;

export const RAW_PAGE_TYPE = {
  listing: "listing",
  detail: "detail",
  unknown: "unknown",
} as const;

export type RawPageType = (typeof RAW_PAGE_TYPE)[keyof typeof RAW_PAGE_TYPE];

export type CrawlCursor = {
  nextDetailIndex: number;
  totalDetailLinks: number;
  listingNormalizedUrl: string;
};

export function createCrawlCursor(listingNormalizedUrl = ""): CrawlCursor {
  return {
    nextDetailIndex: 0,
    totalDetailLinks: 0,
    listingNormalizedUrl,
  };
}

export function clampDetailLimit(detailLimit: number): number {
  return Math.min(detailLimit, MAX_DETAIL_PAGE_LIMIT);
}

export const COIN_CANDIDATE_STATUS = {
  pending: "pending",
  accepted: "accepted",
  quarantined: "quarantined",
} as const;

export const QUARANTINE_REASON = {
  unrecognizedPageType: "unrecognized_page_type",
  invalidYearRange: "invalid_year_range",
  missingIdentityFields: "missing_identity_fields",
  duplicateSourceDetailUrl: "duplicate_source_detail_url",
} as const;

export type FetchRawSourcePagePayload = {
  sourceId: string;
  requestUrl: string;
  originalUrl: string;
  pageRole: "listing" | "detail";
  detailLimit: number;
  cursor: CrawlCursor;
};

export type ExtractCoinCandidatePayload = {
  sourceId: string;
  rawSourcePageId: string;
};

export type AcceptCoinCandidatePayload = {
  sourceId: string;
  candidateId: string;
};

export type DownloadAcceptedCoinImagePayload = {
  sourceId: string;
  acceptedCoinId: string;
  imageUrl: string;
};
