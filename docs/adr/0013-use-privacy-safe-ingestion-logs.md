# Use Privacy-Safe Ingestion Logs

CoinCodex ingestion logs will identify work with opaque source ids, crawl run ids, job ids, URL hashes, page types, statuses, attempt counts, and error codes rather than source names, domains, full URLs, or page titles. A local debug mode may expose private details during development, but default logs should not leak source identity.
