# Build Local Ingestion Skeleton Before Live Crawling

CoinCodex will first build a local ingestion skeleton with TypeScript, Postgres, Drizzle, private source config seeding, CLI commands, Postgres-backed jobs, crawl run creation, and a fake crawl provider. Live Firecrawl integration will wait until the pipeline state machine is proven without external cost, rate limits, or network variability.
