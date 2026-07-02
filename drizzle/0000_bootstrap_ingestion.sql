CREATE TABLE IF NOT EXISTS sources (
  id text PRIMARY KEY,
  config jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS crawl_runs (
  id text PRIMARY KEY,
  source_id text NOT NULL REFERENCES sources(id),
  scope text NOT NULL,
  status text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
  id text PRIMARY KEY,
  crawl_run_id text NOT NULL REFERENCES crawl_runs(id),
  kind text NOT NULL,
  status text NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 3,
  scheduled_at timestamptz NOT NULL,
  available_at timestamptz NOT NULL,
  locked_at timestamptz,
  lock_token text,
  error_payload jsonb,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_source_pages (
  id text PRIMARY KEY,
  crawl_run_id text NOT NULL REFERENCES crawl_runs(id),
  source_id text NOT NULL REFERENCES sources(id),
  job_id text NOT NULL REFERENCES jobs(id),
  normalized_url text NOT NULL,
  url_hash text NOT NULL,
  content text NOT NULL,
  content_hash text NOT NULL,
  provider_payload jsonb NOT NULL,
  fetched_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_lookup_idx
  ON jobs (status, available_at, scheduled_at);

CREATE INDEX IF NOT EXISTS raw_source_pages_run_idx
  ON raw_source_pages (crawl_run_id);
