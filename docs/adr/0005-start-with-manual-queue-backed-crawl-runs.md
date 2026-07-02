# Start With Manual Queue-Backed Crawl Runs

CoinCodex will start Crawl Runs manually in the MVP, with private source id, scope, and page limits supplied at run time. Internally, the run will still enqueue fetch, extract, accept, and image jobs so the same workflow can later be scheduled or made autonomous without redesigning ingestion around a synchronous command.
