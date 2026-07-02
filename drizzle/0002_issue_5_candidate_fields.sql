ALTER TABLE coin_candidates
  ADD COLUMN IF NOT EXISTS original_detail_url text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS name_raw text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS name_normalized text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS issuer_raw text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS issuer_normalized text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS denomination_raw text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS denomination_normalized text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS mint_mark text NOT NULL DEFAULT '';
