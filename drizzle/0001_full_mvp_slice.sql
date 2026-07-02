CREATE TABLE IF NOT EXISTS coin_candidates (
  id text PRIMARY KEY,
  crawl_run_id text NOT NULL REFERENCES crawl_runs(id),
  source_id text NOT NULL REFERENCES sources(id),
  raw_source_page_id text NOT NULL REFERENCES raw_source_pages(id),
  normalized_detail_url text NOT NULL,
  detail_url_hash text NOT NULL,
  page_type text NOT NULL,
  title text NOT NULL,
  issuer text NOT NULL,
  denomination text NOT NULL,
  raw_date_text text NOT NULL,
  issued_from_year integer,
  issued_to_year integer,
  image_url text,
  fingerprint text,
  status text NOT NULL,
  quarantine_reason text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accepted_coins (
  id text PRIMARY KEY,
  crawl_run_id text NOT NULL REFERENCES crawl_runs(id),
  candidate_id text NOT NULL REFERENCES coin_candidates(id),
  source_id text NOT NULL REFERENCES sources(id),
  source_detail_url text NOT NULL,
  source_detail_url_hash text NOT NULL,
  issuer text NOT NULL,
  denomination text NOT NULL,
  issued_from_year integer NOT NULL,
  issued_to_year integer NOT NULL,
  fingerprint text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accepted_coin_images (
  id text PRIMARY KEY,
  crawl_run_id text NOT NULL REFERENCES crawl_runs(id),
  accepted_coin_id text NOT NULL REFERENCES accepted_coins(id),
  source_id text NOT NULL REFERENCES sources(id),
  source_image_url text NOT NULL,
  source_image_url_hash text NOT NULL,
  content_hash text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS coin_candidates_run_idx
  ON coin_candidates (crawl_run_id);

CREATE INDEX IF NOT EXISTS accepted_coins_run_idx
  ON accepted_coins (crawl_run_id);

CREATE INDEX IF NOT EXISTS accepted_coins_detail_url_hash_idx
  ON accepted_coins (source_detail_url_hash);

CREATE INDEX IF NOT EXISTS accepted_coins_fingerprint_idx
  ON accepted_coins (fingerprint);

CREATE INDEX IF NOT EXISTS accepted_coin_images_run_idx
  ON accepted_coin_images (crawl_run_id);
