import { normalizeUrl } from "../page-processing.js";
import type { FirecrawlSourceConfig } from "../source-config.js";
import {
  CrawlProviderError,
  type CrawlProvider,
  type FetchPageInput,
  type FetchPageResult,
} from "./crawl-provider.js";

type FirecrawlScrapeResponse = {
  success?: boolean;
  requestId?: string;
  data?: {
    html?: string;
    links?: string[];
    metadata?: Record<string, unknown>;
  };
};

export interface FirecrawlClient {
  scrapeUrl(input: {
    apiKey: string;
    url: string;
  }): Promise<FirecrawlScrapeResponse>;
}

export class HttpFirecrawlClient implements FirecrawlClient {
  async scrapeUrl(input: { apiKey: string; url: string }): Promise<FirecrawlScrapeResponse> {
    const response = await fetch("https://api.firecrawl.dev/v2/scrape", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${input.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        url: input.url,
        formats: ["html", "links"],
        onlyMainContent: false,
      }),
    });

    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    const requestId = readOptionalString(body, "requestId");
    const data = readOptionalRecord(body, "data");
    const metadata = data ? readOptionalRecord(data, "metadata") : undefined;
    const providerPayload = {
      adapter: "firecrawl",
      requestId: requestId ?? null,
      statusCode: typeof metadata?.statusCode === "number" ? metadata.statusCode : response.status,
      metadata: metadata ?? {},
      links: data?.links ?? [],
      success: body.success === true,
      error: readOptionalString(body, "error") ?? readOptionalString(metadata, "error") ?? null,
    };

    if (!response.ok) {
      throw new CrawlProviderError(
        readOptionalString(body, "error") ?? `firecrawl request failed with status ${response.status}`,
        {
          code: readOptionalString(body, "code") ?? "FIRECRAWL_REQUEST_FAILED",
          retryable: response.status === 429 || response.status >= 500 || response.status === 408,
          statusCode: response.status,
          requestId: requestId ?? undefined,
          providerPayload,
        },
      );
    }

    return {
      success: body.success === true,
      requestId,
      data: {
        html: readOptionalString(data, "html"),
        links: Array.isArray(data?.links) ? (data.links as string[]) : [],
        metadata: metadata ?? {},
      },
    };
  }
}

export class FirecrawlProvider implements CrawlProvider {
  constructor(private readonly client: FirecrawlClient) {}

  async fetchPage(input: FetchPageInput): Promise<FetchPageResult> {
    if (input.sourceConfig.adapter !== "firecrawl") {
      throw new Error(`unsupported firecrawl provider adapter: ${input.sourceConfig.adapter}`);
    }

    return this.fetchFirecrawlPage(input.sourceConfig, input.requestUrl);
  }

  private async fetchFirecrawlPage(
    sourceConfig: FirecrawlSourceConfig,
    requestUrl: string,
  ): Promise<FetchPageResult> {
    try {
      const response = await this.client.scrapeUrl({
        apiKey: sourceConfig.apiKey,
        url: requestUrl,
      });
      const html = response.data?.html;
      if (!html) {
        throw new CrawlProviderError("firecrawl response missing html content", {
          code: "FIRECRAWL_EMPTY_HTML",
          retryable: false,
          requestId: response.requestId,
          providerPayload: {
            adapter: "firecrawl",
            requestId: response.requestId ?? null,
            statusCode:
              typeof response.data?.metadata?.statusCode === "number"
                ? response.data.metadata.statusCode
                : null,
            metadata: response.data?.metadata ?? {},
            links: response.data?.links ?? [],
          },
        });
      }

      return {
        originalUrl: requestUrl,
        normalizedUrl: normalizeUrl(requestUrl),
        content: html,
        extractedLinks: response.data?.links ?? [],
        providerPayload: {
          adapter: "firecrawl",
          requestId: response.requestId ?? null,
          statusCode:
            typeof response.data?.metadata?.statusCode === "number"
              ? response.data.metadata.statusCode
              : null,
          metadata: response.data?.metadata ?? {},
          links: response.data?.links ?? [],
        },
      };
    } catch (error) {
      if (error instanceof CrawlProviderError) {
        throw error;
      }

      const statusCode =
        typeof (error as { statusCode?: unknown })?.statusCode === "number"
          ? ((error as { statusCode: number }).statusCode)
          : undefined;
      const requestId =
        typeof (error as { requestId?: unknown })?.requestId === "string"
          ? ((error as { requestId: string }).requestId)
          : undefined;
      const code =
        typeof (error as { code?: unknown })?.code === "string"
          ? ((error as { code: string }).code)
          : "FIRECRAWL_REQUEST_FAILED";

      throw new CrawlProviderError(
        error instanceof Error ? error.message : String(error),
        {
          code,
          retryable: statusCode === 429 || statusCode === 408 || (statusCode ?? 0) >= 500,
          statusCode,
          requestId,
          providerPayload: {
            adapter: "firecrawl",
            requestId: requestId ?? null,
            statusCode: statusCode ?? null,
          },
        },
      );
    }
  }
}

function readOptionalRecord(
  record: Record<string, unknown> | undefined,
  key: string,
): Record<string, unknown> | undefined {
  const value = record?.[key];
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }

  return value as Record<string, unknown>;
}

function readOptionalString(
  record: Record<string, unknown> | undefined,
  key: string,
): string | undefined {
  const value = record?.[key];
  return typeof value === "string" ? value : undefined;
}
