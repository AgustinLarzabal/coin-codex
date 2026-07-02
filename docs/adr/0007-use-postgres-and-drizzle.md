# Use Postgres and Drizzle

CoinCodex will use Postgres as the primary datastore and Drizzle for schema definition and migrations. This fits catalog records, raw page storage, JSON provider payloads, and Postgres-backed jobs while keeping SQL structure visible for ingestion-heavy workflows.
