# Use Postgres-Backed Jobs for MVP

CoinCodex will use Postgres-backed jobs for the MVP ingestion queue instead of adding Redis or a dedicated queue service immediately. This keeps catalog data, crawl state, retries, and job observability in one operational datastore while preserving the option to move high-volume queues elsewhere later.
