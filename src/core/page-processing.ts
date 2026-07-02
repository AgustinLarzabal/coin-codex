import { RAW_PAGE_TYPE, type CrawlCursor, type RawPageType } from "./ingestion.js";

export function normalizeUrl(input: string, baseUrl?: string): string {
  const url = new URL(input, baseUrl);
  url.hash = "";
  url.protocol = url.protocol.toLowerCase();
  url.hostname = url.hostname.toLowerCase();

  const sortedSearchEntries = [...url.searchParams.entries()].sort(([leftKey, leftValue], [rightKey, rightValue]) => {
    if (leftKey === rightKey) {
      return leftValue.localeCompare(rightValue);
    }

    return leftKey.localeCompare(rightKey);
  });

  url.search = "";
  for (const [key, value] of sortedSearchEntries) {
    url.searchParams.append(key, value);
  }

  return url.toString();
}

export function classifyRawPage(content: string): RawPageType {
  if (content.includes('data-page-kind="listing"')) {
    return RAW_PAGE_TYPE.listing;
  }
  if (content.includes('data-page-kind="coin-detail"')) {
    return RAW_PAGE_TYPE.detail;
  }

  return RAW_PAGE_TYPE.unknown;
}

export function extractDetailLinks(
  content: string,
  baseUrl: string,
): Array<{ originalUrl: string; normalizedUrl: string }> {
  const hrefPattern = /<a\b[^>]*href="([^"]+)"[^>]*>/gi;
  const seenNormalizedUrls = new Set<string>();
  const detailLinks: Array<{ originalUrl: string; normalizedUrl: string }> = [];

  for (const match of content.matchAll(hrefPattern)) {
    const href = match[1];
    const originalUrl = new URL(href, baseUrl).toString();
    const normalizedUrl = normalizeUrl(originalUrl);
    if (seenNormalizedUrls.has(normalizedUrl)) {
      continue;
    }

    seenNormalizedUrls.add(normalizedUrl);
    detailLinks.push({ originalUrl, normalizedUrl });
  }

  return detailLinks;
}

export function readStoredCursor(value: unknown): CrawlCursor | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const record = value as Record<string, unknown>;
  const nextDetailIndex = record.nextDetailIndex;
  const totalDetailLinks = record.totalDetailLinks;
  const listingNormalizedUrl = record.listingNormalizedUrl;
  if (
    typeof nextDetailIndex !== "number" ||
    typeof totalDetailLinks !== "number" ||
    typeof listingNormalizedUrl !== "string"
  ) {
    return null;
  }

  return {
    nextDetailIndex,
    totalDetailLinks,
    listingNormalizedUrl,
  };
}
