# CoinCodex

CoinCodex is the knowledge engine for discovering, collecting, organizing, and preserving structured numismatic knowledge. It feeds user-facing catalog products such as Coin Archive.

## Language

**CoinCodex**:
The backend knowledge engine that ingests, reconciles, normalizes, and stores information about physical coins.
_Avoid_: Coin Archive

**Coin Archive**:
A separate user-facing catalog app that consumes knowledge from CoinCodex.
_Avoid_: CoinCodex

**Coin**:
A minted coin type or issue: the design/version that was officially produced, independent of any individual physical specimen.
_Avoid_: Specimen, token, currency, record

**Specimen**:
One individual physical example of a Coin, such as a graded object, auction lot, museum holding, or collector-owned piece. Specimens are outside the MVP catalog scope.
_Avoid_: Coin

**Coin Identity**:
The minimum information needed to identify and reconcile a Coin: issuer, denomination, date or date range, mint mark when present, coin name or type, source URL, and raw source payload.
_Avoid_: Full coin profile

**Issuer**:
The authority responsible for issuing a Coin, such as a modern country, empire, kingdom, colony, city-state, territory, or other political authority.
_Avoid_: Country

**Source**:
An external numismatic database or website that CoinCodex reads from to discover and enrich Coins. Source identities are private operational data and should be kept out of code, filenames, logs, queue names, and public UI.
_Avoid_: Scraper, adapter

**Source Adapter**:
A source-specific ingestion component that knows how to navigate and extract Coin Identity data from one Source.
_Avoid_: Source, crawler

**Raw Source Page**:
The stored result of reading a Source URL before Coin normalization, including fetched content, metadata, links, hashes, and the provider payload needed for later replay or debugging.
_Avoid_: Coin, normalized data

**Coin Candidate**:
A normalized claim that a Source page appears to describe a Coin, produced from one or more Raw Source Pages before deduplication and acceptance into the catalog.
_Avoid_: Coin, raw source page

**Accepted Coin**:
A Coin Candidate that has passed the automated acceptance rules and become part of the canonical CoinCodex catalog.
_Avoid_: Coin Candidate

**Acceptance Rules**:
The automated criteria that decide whether a Coin Candidate can become an Accepted Coin or must be held back for later review.
_Avoid_: Manual review

**Quarantined Candidate**:
A Coin Candidate that failed automated Acceptance Rules because it is incomplete, conflicting, low confidence, or likely to duplicate an existing catalog entry.
_Avoid_: Accepted Coin

**Source Record Identity**:
The private Source plus source detail URL that identifies one record from one Source and prevents re-importing the same source page.
_Avoid_: Coin Identity

**Coin Fingerprint**:
A normalized comparison key derived from Coin Identity fields and used to detect possible duplicate Coins or Coin Candidates.
_Avoid_: Source Record Identity

**Coin Image**:
An image associated with an Accepted Coin and attributed back to its Source evidence. Image URLs may be discovered during extraction, but image files are downloaded only after acceptance.
_Avoid_: Raw source page

**Crawl Run**:
A bounded execution of Source discovery and page fetching, defined by private source configuration plus limits such as country, listing path, maximum coin detail pages, and rate policy.
_Avoid_: Full source import

**Rate Policy**:
The per-Source rules that control external fetch timing, concurrency, and backoff. MVP fetches should be conservative and single-flight per Source.
_Avoid_: Worker speed
