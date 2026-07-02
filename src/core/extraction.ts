export type IssuedYearRange = {
  issuedFromYear: number | null;
  issuedToYear: number | null;
};

export type ExtractedCoinCandidate = {
  pageType: string;
  nameRaw: string;
  nameNormalized: string;
  issuerRaw: string;
  issuerNormalized: string;
  denominationRaw: string;
  denominationNormalized: string;
  rawDateText: string;
  issuedFromYear: number | null;
  issuedToYear: number | null;
  mintMark: string;
  imageUrl: string | undefined;
};

function readAttr(content: string, attribute: string): string | undefined {
  const match = content.match(new RegExp(`${attribute}="([^"]+)"`, "i"));
  return match?.[1];
}

function readTag(content: string, tag: string): string {
  const match = content.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, "i"));
  return match?.[1]?.trim() ?? "";
}

function readField(content: string, label: string): string {
  const match = content.match(new RegExp(`<p>${label}:\\s*([^<]*)</p>`, "i"));
  return match?.[1]?.trim() ?? "";
}

export function classifyPage(content: string): string {
  return readAttr(content, "data-page-kind") ?? "unknown";
}

export function extractListingLinks(content: string): string[] {
  return Array.from(
    content.matchAll(/<a[^>]*data-coin-detail-link="true"[^>]*href="([^"]+)"/gi),
    (match) => match[1],
  );
}

function normalizeFieldValue(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function parseYearRange(rawDateText: string): IssuedYearRange {
  const match = normalizeFieldValue(rawDateText).match(/^(\d{4})(?:\s*[-–]\s*(\d{4}))?$/);
  if (!match) {
    return { issuedFromYear: null, issuedToYear: null };
  }

  const issuedFromYear = Number.parseInt(match[1], 10);
  const issuedToYear = Number.parseInt(match[2] ?? match[1], 10);
  return { issuedFromYear, issuedToYear };
}

export function extractCoinCandidate(content: string): ExtractedCoinCandidate {
  const rawDateText = readField(content, "Year");
  const { issuedFromYear, issuedToYear } = parseYearRange(rawDateText);
  const nameRaw = readTag(content, "h1");
  const issuerRaw = readField(content, "Issuer");
  const denominationRaw = readField(content, "Denomination");
  const mintMark = readField(content, "Mint Mark");

  return {
    pageType: classifyPage(content),
    nameRaw,
    nameNormalized: normalizeFieldValue(nameRaw),
    issuerRaw,
    issuerNormalized: normalizeFieldValue(issuerRaw),
    denominationRaw,
    denominationNormalized: normalizeFieldValue(denominationRaw),
    rawDateText,
    issuedFromYear,
    issuedToYear,
    mintMark: normalizeFieldValue(mintMark),
    imageUrl: readAttr(content, "data-image-url"),
  };
}
