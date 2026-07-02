import type { SourceConfig } from "../source-config.js";

export type FetchPageInput = {
  sourceConfig: SourceConfig;
  requestUrl: string;
};

export type FetchPageResult = {
  originalUrl: string;
  normalizedUrl: string;
  content: string;
  extractedLinks: string[];
  providerPayload: Record<string, unknown>;
};

export class CrawlProviderError extends Error {
  constructor(
    message: string,
    readonly details: {
      code: string;
      retryable: boolean;
      statusCode?: number;
      requestId?: string;
      providerPayload?: Record<string, unknown>;
    },
  ) {
    super(message);
  }
}

export interface CrawlProvider {
  fetchPage(input: FetchPageInput): Promise<FetchPageResult>;
}
