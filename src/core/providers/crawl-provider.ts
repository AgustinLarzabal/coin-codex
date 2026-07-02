export type FetchPageInput = {
  fixtureId: string;
  requestUrl: string;
};

export type FetchPageResult = {
  normalizedUrl: string;
  content: string;
  providerPayload: Record<string, unknown>;
};

export interface CrawlProvider {
  fetchPage(input: FetchPageInput): Promise<FetchPageResult>;
}
