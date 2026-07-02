# Store Source Configs as Private Data

CoinCodex will store Source configuration in the database, seeded from ignored private files such as `.private/sources/*.json`. Code will reference opaque source ids and generic concepts only, while private config may contain source names, domains, start URLs, allowed paths, locale, selectors, crawl settings, and rate limits.
