import type { SourceConfig } from "../source-config.js";
import type { CrawlProvider, FetchPageInput, FetchPageResult } from "./crawl-provider.js";
import { FakeCrawlProvider } from "./fake-crawl-provider.js";
import { FirecrawlProvider, type FirecrawlClient } from "./firecrawl-provider.js";

export class AppCrawlProvider implements CrawlProvider {
  private readonly fakeProvider = new FakeCrawlProvider();
  private readonly firecrawlProvider: FirecrawlProvider;

  constructor(firecrawlClient: FirecrawlClient) {
    this.firecrawlProvider = new FirecrawlProvider(firecrawlClient);
  }

  fetchPage(input: FetchPageInput): Promise<FetchPageResult> {
    return this.selectProvider(input.sourceConfig).fetchPage(input);
  }

  private selectProvider(sourceConfig: SourceConfig): CrawlProvider {
    if (sourceConfig.adapter === "fake") {
      return this.fakeProvider;
    }

    return this.firecrawlProvider;
  }
}
