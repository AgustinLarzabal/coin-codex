ALTER TABLE crawl_runs
  ADD COLUMN IF NOT EXISTS detail_limit integer NOT NULL DEFAULT 10,
  ADD COLUMN IF NOT EXISTS cursor jsonb;

ALTER TABLE raw_source_pages
  ADD COLUMN IF NOT EXISTS original_url text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS page_type text NOT NULL DEFAULT 'unknown';
