import { jsonb, integer, pgTable, text, timestamp } from "drizzle-orm/pg-core";

export const sources = pgTable("sources", {
  id: text("id").primaryKey(),
  config: jsonb("config").$type<Record<string, unknown>>().notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
});

export const crawlRuns = pgTable("crawl_runs", {
  id: text("id").primaryKey(),
  sourceId: text("source_id")
    .notNull()
    .references(() => sources.id),
  scope: text("scope").notNull(),
  status: text("status").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

export const jobs = pgTable("jobs", {
  id: text("id").primaryKey(),
  crawlRunId: text("crawl_run_id")
    .notNull()
    .references(() => crawlRuns.id),
  kind: text("kind").notNull(),
  status: text("status").notNull(),
  attempts: integer("attempts").notNull().default(0),
  maxAttempts: integer("max_attempts").notNull().default(3),
  scheduledAt: timestamp("scheduled_at", { withTimezone: true }).notNull(),
  availableAt: timestamp("available_at", { withTimezone: true }).notNull(),
  lockedAt: timestamp("locked_at", { withTimezone: true }),
  lockToken: text("lock_token"),
  errorPayload: jsonb("error_payload").$type<Record<string, unknown> | null>(),
  payload: jsonb("payload").$type<Record<string, unknown>>().notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

export const rawSourcePages = pgTable("raw_source_pages", {
  id: text("id").primaryKey(),
  crawlRunId: text("crawl_run_id")
    .notNull()
    .references(() => crawlRuns.id),
  sourceId: text("source_id")
    .notNull()
    .references(() => sources.id),
  jobId: text("job_id")
    .notNull()
    .references(() => jobs.id),
  normalizedUrl: text("normalized_url").notNull(),
  urlHash: text("url_hash").notNull(),
  content: text("content").notNull(),
  contentHash: text("content_hash").notNull(),
  providerPayload: jsonb("provider_payload").$type<Record<string, unknown>>().notNull(),
  fetchedAt: timestamp("fetched_at", { withTimezone: true }).defaultNow().notNull(),
});

export const coinCandidates = pgTable("coin_candidates", {
  id: text("id").primaryKey(),
  crawlRunId: text("crawl_run_id")
    .notNull()
    .references(() => crawlRuns.id),
  sourceId: text("source_id")
    .notNull()
    .references(() => sources.id),
  rawSourcePageId: text("raw_source_page_id")
    .notNull()
    .references(() => rawSourcePages.id),
  normalizedDetailUrl: text("normalized_detail_url").notNull(),
  detailUrlHash: text("detail_url_hash").notNull(),
  pageType: text("page_type").notNull(),
  title: text("title").notNull(),
  issuer: text("issuer").notNull(),
  denomination: text("denomination").notNull(),
  rawDateText: text("raw_date_text").notNull(),
  issuedFromYear: integer("issued_from_year"),
  issuedToYear: integer("issued_to_year"),
  imageUrl: text("image_url"),
  fingerprint: text("fingerprint"),
  status: text("status").notNull(),
  quarantineReason: text("quarantine_reason"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
});

export const acceptedCoins = pgTable("accepted_coins", {
  id: text("id").primaryKey(),
  crawlRunId: text("crawl_run_id")
    .notNull()
    .references(() => crawlRuns.id),
  candidateId: text("candidate_id")
    .notNull()
    .references(() => coinCandidates.id),
  sourceId: text("source_id")
    .notNull()
    .references(() => sources.id),
  sourceDetailUrl: text("source_detail_url").notNull(),
  sourceDetailUrlHash: text("source_detail_url_hash").notNull(),
  issuer: text("issuer").notNull(),
  denomination: text("denomination").notNull(),
  issuedFromYear: integer("issued_from_year").notNull(),
  issuedToYear: integer("issued_to_year").notNull(),
  fingerprint: text("fingerprint").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
});

export const acceptedCoinImages = pgTable("accepted_coin_images", {
  id: text("id").primaryKey(),
  crawlRunId: text("crawl_run_id")
    .notNull()
    .references(() => crawlRuns.id),
  acceptedCoinId: text("accepted_coin_id")
    .notNull()
    .references(() => acceptedCoins.id),
  sourceId: text("source_id")
    .notNull()
    .references(() => sources.id),
  sourceImageUrl: text("source_image_url").notNull(),
  sourceImageUrlHash: text("source_image_url_hash").notNull(),
  contentHash: text("content_hash").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
});
