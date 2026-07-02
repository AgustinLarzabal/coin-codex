# Hide Crawl Vendors Behind a Provider Boundary

CoinCodex will call Firecrawl through an internal crawl provider boundary rather than letting application workflows depend directly on Firecrawl types or APIs. Firecrawl can be the only MVP implementation, but this keeps crawled page storage, queues, retries, and extraction workflows independent of a single vendor.
