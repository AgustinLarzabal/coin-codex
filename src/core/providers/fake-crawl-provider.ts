import type { CrawlProvider, FetchPageInput, FetchPageResult } from "./crawl-provider.js";

const FIXTURES: Record<string, Record<string, string>> = {
  "fixture-coin": {
    "https://private.example.test/coins": `
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
  },
  "fixture-run": {
    "https://private.example.test/coins": `
<html>
  <body>
    <section data-page-kind="listing">
      <a data-coin-detail-link="true" href="https://private.example.test/coins/accepted-coin">Accepted coin</a>
      <a data-coin-detail-link="true" href="https://private.example.test/coins/quarantine-coin">Quarantine coin</a>
    </section>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/accepted-coin": `
<html>
  <body>
    <article data-page-kind="coin-detail" data-image-url="https://private.example.test/images/accepted-coin.jpg">
      <h1>Accepted Fixture Coin</h1>
      <p>Issuer: Example Issuer</p>
      <p>Denomination: 1 Unit</p>
      <p>Year: 1901</p>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/quarantine-coin": `
<html>
  <body>
    <article data-page-kind="coin-detail" data-image-url="https://private.example.test/images/quarantine-coin.jpg">
      <h1>Quarantine Fixture Coin</h1>
      <p>Issuer: Example Issuer</p>
      <p>Denomination: </p>
      <p>Year: 1905-1903</p>
    </article>
  </body>
</html>`.trim(),
  },
};

export class FakeCrawlProvider implements CrawlProvider {
  async fetchPage(input: FetchPageInput): Promise<FetchPageResult> {
    const fixtureSet = FIXTURES[input.fixtureId];
    const content = fixtureSet?.[input.requestUrl];
    if (!content) {
      throw new Error(`unknown fixture page: ${input.fixtureId} ${input.requestUrl}`);
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
