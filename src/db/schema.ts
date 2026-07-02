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
