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

function parseYearRange(rawDateText: string) {
  const match = rawDateText.match(/^(\d{4})(?:-(\d{4}))?$/);
  if (!match) {
    return { issuedFromYear: null, issuedToYear: null };
  }

  const issuedFromYear = Number.parseInt(match[1], 10);
  const issuedToYear = Number.parseInt(match[2] ?? match[1], 10);
  return { issuedFromYear, issuedToYear };
}

export function extractCoinCandidate(content: string) {
  const rawDateText = readField(content, "Year");
  const { issuedFromYear, issuedToYear } = parseYearRange(rawDateText);

  return {
    pageType: classifyPage(content),
    title: readTag(content, "h1"),
    issuer: readField(content, "Issuer"),
    denomination: readField(content, "Denomination"),
    rawDateText,
    issuedFromYear,
    issuedToYear,
    imageUrl: readAttr(content, "data-image-url"),
  };
}
