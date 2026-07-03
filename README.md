# CoinCodex

> _Every coin has a story. CoinCodex is here to preserve it._

In ancient times, a **codex** was more than a book—it was a carefully curated collection of knowledge.

CoinCodex follows that same philosophy.

Its mission is to discover, collect, organize, and preserve numismatic knowledge from across the world, transforming fragmented information into a single, structured source of truth.

By connecting multiple sources, reconciling conflicting information, validating records, and continuously expanding its knowledge base, CoinCodex lays the foundation for the world's most comprehensive catalog of physical coins.

Every record is another page in the history of money.

**The knowledge engine behind Coin Archive.**

## Current Scope

This repository currently contains the ingestion engine behind that goal. It seeds private source definitions, creates crawl runs, fetches listing/detail pages, extracts coin candidates, applies acceptance/quarantine rules, downloads accepted coin images, and provides privacy-safe run inspection.

## Requirements

- Node.js 24+
- pnpm 10+

Install dependencies:

```sh
pnpm install
```

Run checks:

```sh
pnpm run typecheck
pnpm test
```

## Database

The CLI uses PGlite and requires `DATABASE_URL`. Migrations in `drizzle/` are applied automatically when a command starts.

For local development, copy the example environment file:

```sh
cp .env.example .env
mkdir -p .private
```

The default value uses a persistent PGlite database under `.private/`, which is ignored by Git:

```sh
DATABASE_URL=.private/coincodex-db
```

Before running CLI commands in a shell session, export the `.env` values:

```sh
set -a
source .env
set +a
```

You can also set the value inline for one command:

```sh
DATABASE_URL=.private/coincodex-db pnpm exec tsx src/cli.ts run-worker
```

For a throwaway in-memory run, use:

```sh
export DATABASE_URL="memory://coincodex-dev"
```

## Source File

Create a JSON file containing an array of sources. Each source has an opaque `id` and a private `config`.

Fake fixture source, useful for local smoke tests:

```json
[
  {
    "id": "src_fixture",
    "config": {
      "adapter": "fake",
      "fixtureId": "fixture-run",
      "name": "Private Source Name",
      "domain": "private.example.test",
      "startUrl": "https://private.example.test/coins"
    }
  }
]
```

Firecrawl source:

```json
[
  {
    "id": "src_live",
    "config": {
      "adapter": "firecrawl",
      "apiKey": "fc-your-api-key",
      "name": "Private Source Name",
      "domain": "example.com",
      "startUrl": "https://example.com/coins",
      "ratePolicy": {
        "minDelayMs": 1000,
        "backoffBaseMs": 2000,
        "attemptLimit": 3
      }
    }
  }
]
```

Supported source config fields:

- `adapter`: `fake` or `firecrawl`
- `fixtureId`: required for `fake`
- `apiKey`: required for `firecrawl`
- `startUrl`: listing page to crawl
- `name`, `domain`: optional private labels shown only with debug inspection
- `ratePolicy.minDelayMs`: delay between scheduled detail fetches
- `ratePolicy.backoffBaseMs`: retry backoff base for fetch failures
- `ratePolicy.attemptLimit`: max fetch attempts

## CLI Usage

Run commands with:

```sh
pnpm exec tsx src/cli.ts <command>
```

Seed sources:

```sh
pnpm exec tsx src/cli.ts seed-sources --file ./sources.json
```

Create a crawl run:

```sh
pnpm exec tsx src/cli.ts create-run \
  --source-id src_fixture \
  --scope issuer_scope \
  --detail-limit 10
```

Process queued work. Run this repeatedly until it returns `"processed": 0`:

```sh
pnpm exec tsx src/cli.ts run-worker
```

Inspect a run:

```sh
pnpm exec tsx src/cli.ts inspect-run --run-id <run-id>
```

By default, inspection redacts private source URLs and names. To include private debugging details:

```sh
pnpm exec tsx src/cli.ts inspect-run --run-id <run-id> --debug-private
```

## Ingestion Flow

1. `seed-sources` stores source configuration.
2. `create-run` creates a crawl run and queues the listing page fetch.
3. `run-worker` processes one queued job at a time:
   - fetch listing page
   - enqueue detail pages up to `--detail-limit`
   - fetch detail pages
   - extract coin candidates
   - accept or quarantine candidates
   - download images for accepted coins
4. `inspect-run` summarizes job status, page counts, accepted/quarantined candidates, accepted coins, image records, cursor progress, and errors.

Accepted coins are deduplicated by source detail URL within a source and by strong fingerprint across sources. Invalid or incomplete candidates are quarantined with a reason instead of accepted.
