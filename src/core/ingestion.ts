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
export const DEFAULT_JOB_MAX_ATTEMPTS = 3;

export type FetchRawSourcePagePayload = {
  sourceId: string;
  fixtureId: string;
  requestUrl: string;
};

export function buildFixtureRequestUrl(fixtureId: string): string {
  return `private://fixture/${fixtureId}`;
}
