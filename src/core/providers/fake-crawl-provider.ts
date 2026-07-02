import { normalizeUrl } from "../page-processing.js";
import type { CrawlProvider, FetchPageInput, FetchPageResult } from "./crawl-provider.js";

const FIXTURES: Record<string, Record<string, string>> = {
  "fixture-coin": {
    "https://private.example.test/coins": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 001</h1>
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
  "fixture-detail-pages": {
    "https://private.example.test/coins": `
<html>
  <body>
    <section data-page-kind="listing">
      <a data-coin-detail-link="true" href="https://private.example.test/coins/ceres-5-francs">Ceres 5 Francs</a>
      <a data-coin-detail-link="true" href="https://private.example.test/coins/specimen-5-francs">Specimen 5 Francs</a>
      <a data-coin-detail-link="true" href="https://private.example.test/coins/reference-note">Reference Note</a>
    </section>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/ceres-5-francs": `
<html>
  <body>
    <article data-page-kind="coin-detail" data-image-url="https://private.example.test/images/ceres-5-francs.jpg">
      <h1>  5   Francs   Ceres </h1>
      <p>Issuer: République   française</p>
      <p>Denomination: 5 Francs</p>
      <p>Year: 1870-1871</p>
      <p>Mint Mark: A</p>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/specimen-5-francs": `
<html>
  <body>
    <article data-page-kind="specimen-detail">
      <h1> 5 Francs Specimen   PCGS MS64 </h1>
      <p>Issuer: République française</p>
      <p>Denomination: 5 Francs</p>
      <p>Year: 1870</p>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/reference-note": `
<html>
  <body>
    <article data-page-kind="reference-detail">
      <h1>Minting   Reference Note</h1>
    </article>
  </body>
</html>`.trim(),
  },
  "fixture-catalog": {
    "https://private.example.test/coins?a=1&b=2": `
<html>
  <body>
    <section data-page-kind="listing">
      <h1>Catalog Listing</h1>
      <a href="/coins/001?b=2&a=1#top">Coin 001</a>
      <a href="https://private.example.test/coins/002?view=full&letter=b">Coin 002</a>
      <a href="/coins/003">Coin 003</a>
      <a href="/coins/004?edition=proof">Coin 004</a>
      <a href="/coins/005?ref=alpha">Coin 005</a>
      <a href="/coins/006?ref=beta">Coin 006</a>
      <a href="/coins/007#ignore">Coin 007</a>
      <a href="/coins/008?series=gold">Coin 008</a>
      <a href="/coins/009?series=silver">Coin 009</a>
      <a href="/coins/010">Coin 010</a>
      <a href="/coins/011">Coin 011</a>
      <a href="/coins/012?finish=matte">Coin 012</a>
    </section>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/001?a=1&b=2": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 001</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/002?letter=b&view=full": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 002</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/003": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 003</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/004?edition=proof": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 004</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/005?ref=alpha": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 005</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/006?ref=beta": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 006</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/007": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 007</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/008?series=gold": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 008</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/009?series=silver": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 009</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/010": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 010</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/011": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 011</h1>
    </article>
  </body>
</html>`.trim(),
    "https://private.example.test/coins/012?finish=matte": `
<html>
  <body>
    <article data-page-kind="coin-detail">
      <h1>Coin 012</h1>
    </article>
  </body>
</html>`.trim(),
  },
};

export class FakeCrawlProvider implements CrawlProvider {
  async fetchPage(input: FetchPageInput): Promise<FetchPageResult> {
    if (input.sourceConfig.adapter !== "fake") {
      throw new Error(`unsupported fake provider adapter: ${input.sourceConfig.adapter}`);
    }

    const fixturePages = FIXTURES[input.sourceConfig.fixtureId];
    if (!fixturePages) {
      throw new Error(`unknown fixture: ${input.sourceConfig.fixtureId}`);
    }

    const normalizedUrl = normalizeUrl(input.requestUrl);
    const content = fixturePages[normalizedUrl];
    if (!content) {
      throw new Error(`unknown fixture page: ${normalizedUrl}`);
    }

    return {
      originalUrl: input.requestUrl,
      normalizedUrl,
      content,
      extractedLinks: [],
      providerPayload: {
        adapter: "fake",
        fixtureId: input.sourceConfig.fixtureId,
        mode: "fake",
      },
    };
  }
}
