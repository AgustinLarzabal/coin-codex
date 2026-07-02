import type { CrawlProvider, FetchPageInput, FetchPageResult } from "./crawl-provider.js";

const FIXTURES: Record<string, string> = {
  "fixture-coin": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Fixture Coin</h1>
      <p>Issuer: Example Issuer</p>
      <p>Denomination: 1 Unit</p>
      <p>Year: 1901</p>
    </article>
  </body>
</html>`.trim(),
};

export class FakeCrawlProvider implements CrawlProvider {
  async fetchPage(input: FetchPageInput): Promise<FetchPageResult> {
    const content = FIXTURES[input.fixtureId];
    if (!content) {
      throw new Error(`unknown fixture: ${input.fixtureId}`);
    }

    return {
      normalizedUrl: input.requestUrl,
      content,
      providerPayload: {
        fixtureId: input.fixtureId,
        mode: "fake",
      },
    };
  }
}
