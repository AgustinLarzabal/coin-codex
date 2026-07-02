# Define Full MVP Ingestion Slice

The CoinCodex MVP is complete when a CLI-triggered Crawl Run can use one private Source config and one country scope with a limit of 10 coin detail pages to fetch and store raw pages through Firecrawl, extract Coin Candidates, automatically accept high-confidence candidates, quarantine uncertain candidates with reasons, download images for Accepted Coins, and print a privacy-safe run report. The MVP must include fixture-based extraction and acceptance tests and must not expose source names in code or default logs.
