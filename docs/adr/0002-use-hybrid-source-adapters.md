# Use Hybrid Source Adapters

CoinCodex will use hybrid Source Adapters: private source configuration provides domains, start URLs, allowed paths, locale, selectors, and crawl settings, while generic adapter code handles source-specific page shapes and extraction workflows. A purely configuration-only scraper was rejected for the MVP because real catalog pages are likely to contain edge cases that need code-level normalization and control flow.
