import { describe, expect, it } from "vitest";

import { classifyPage, extractCoinCandidate } from "../src/core/extraction.js";

describe("detail page extraction", () => {
  it("extracts raw and normalized coin identity fields from recognized coin detail fixtures", () => {
    const content = `
<html>
  <body>
    <article data-page-kind="coin-detail" data-image-url="https://private.example.test/images/coin.jpg">
      <h1>  5   Francs   Ceres  </h1>
      <p>Issuer:  République   française </p>
      <p>Denomination:   5 Francs </p>
      <p>Year: 1870-1871</p>
      <p>Mint Mark: A </p>
    </article>
  </body>
</html>`.trim();

    expect(classifyPage(content)).toBe("coin-detail");
    expect(extractCoinCandidate(content)).toMatchObject({
      pageType: "coin-detail",
      nameRaw: "5   Francs   Ceres",
      nameNormalized: "5 Francs Ceres",
      issuerRaw: "République   française",
      issuerNormalized: "République française",
      denominationRaw: "5 Francs",
      denominationNormalized: "5 Francs",
      rawDateText: "1870-1871",
      issuedFromYear: 1870,
      issuedToYear: 1871,
      mintMark: "A",
      imageUrl: "https://private.example.test/images/coin.jpg",
    });
  });

  it("classifies specimen-like detail fixtures so they can be quarantined", () => {
    const content = `
<html>
  <body>
    <article data-page-kind="specimen-detail">
      <h1>Specimen 5 Francs</h1>
      <p>Issuer: République française</p>
      <p>Denomination: 5 Francs</p>
      <p>Year: 1870</p>
    </article>
  </body>
</html>`.trim();

    expect(classifyPage(content)).toBe("specimen-detail");
    expect(extractCoinCandidate(content)).toMatchObject({
      pageType: "specimen-detail",
      issuedFromYear: 1870,
      issuedToYear: 1870,
    });
  });
});
