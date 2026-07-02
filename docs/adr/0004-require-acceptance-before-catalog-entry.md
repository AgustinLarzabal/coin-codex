# Require Automated Acceptance Before Catalog Entry

CoinCodex will not treat raw extraction as equivalent to a canonical catalog entry. Coin Candidates may be promoted automatically, but only after passing explicit Acceptance Rules; this keeps external source claims separate from the CoinCodex catalog while still allowing the ingestion system to run autonomously.

For the MVP, automatic acceptance requires complete Coin Identity fields, a source detail URL that has not already been accepted, no strong normalized fingerprint match against a different Accepted Coin, a recognized coin detail page, and basic field validation. Candidates that fail these checks are quarantined instead of blocking ingestion.
