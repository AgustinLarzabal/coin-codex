ALTER TABLE accepted_coin_images
  ADD COLUMN IF NOT EXISTS raw_source_page_id text REFERENCES raw_source_pages(id),
  ADD COLUMN IF NOT EXISTS source_detail_url_hash text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS duplicate_of_accepted_coin_image_id text;
