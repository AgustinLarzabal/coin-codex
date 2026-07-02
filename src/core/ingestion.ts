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

export type FetchRawSourcePagePayload = {
  sourceId: string;
  fixtureId: string;
  requestUrl: string;
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
