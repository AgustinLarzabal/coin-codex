import { RAW_PAGE_TYPE, type CrawlCursor, type RawPageType } from "./ingestion.js";

const DETAIL_LINK_HREF_PATTERN = /<a\b[^>]*href="([^"]+)"[^>]*>/gi;
const DETAIL_PAGE_KIND_PATTERN = /data-page-kind="[^"]*detail"/i;

export type DetailLink = {
  originalUrl: string;
  normalizedUrl: string;
};

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
  if (DETAIL_PAGE_KIND_PATTERN.test(content)) {
    return RAW_PAGE_TYPE.detail;
  }

  return RAW_PAGE_TYPE.unknown;
}

export function extractDetailLinks(
  content: string,
  baseUrl: string,
): DetailLink[] {
  const seenNormalizedUrls = new Set<string>();
  const detailLinks: DetailLink[] = [];

  for (const match of content.matchAll(DETAIL_LINK_HREF_PATTERN)) {
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
