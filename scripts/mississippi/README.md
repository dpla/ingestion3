# Mississippi prefix-trie harvest and verification (Python)

These two scripts were written on the ingest EC2 in September 2026 to work out
whether Mississippi's Primo VE collection could be harvested at all after Ex
Libris changed two things under us (see
[#777](https://github.com/dpla/ingestion3/issues/777)). They are committed here
because they produced the result the current plan rests on, and until now they
existed only on the box's local disk.

**They are not wired into the pipeline.** Nothing calls them; they are run by
hand with `python3`. The long-term replacement for the discovery half is
`PrimoIdDiscovery` / `scripts/discover.sh`; the refresh half is modelled on
`GettyRefreshHarvester`. Keep these until Mississippi is harvesting through the
Scala path, then delete them.

## `msharvest.py`

Enumerates the collection by partitioning it into title-prefix buckets small
enough to page under the tenant's offset ceiling, then pages each bucket.

Repaired from the March 2026 version, which was silently broken by the platform:
it measured with `title,begins_with,<prefix>*`, and after Ex Libris dropped
wildcard support for the `starts with` operator that form returns ~1% of the true
count. Every planned bucket was therefore wrong, downward, and the harvest
"succeeded" ~6% short.

Last run: **3,586 buckets, `unsplittable: 0`, 136,561 of 138,818 distinct
records (98.37%)**.

## `msrefresh.py`

Resolves every known id one at a time and reports `resolved` / `gone` / `errors`
against a freshly measured universe.

This is the measurement, not just a data pull. Set *size* proves nothing about
set *membership* — an excess and a deficit can coexist and cancel out — so only
resolving every id says whether coverage is real.

Last run: **resolved 138,565, gone 405, errors 0 — a deficit of 253 (99.82%)**.

## Two things to know before re-running either

- **Retrieval uses `any,contains,<bare MMS number>`.** `rid,exact` returns 0 on
  this tenant, as do `docid`, `control` and `sourcerecordid`. The `alma` prefix
  on the full `recordid` breaks the match, so it has to be stripped.
- **The offset ceiling is 500, not 5,000.** Mississippi's API key is served on
  Primo's *guest* tier, so `offset + limit` must stay at or under 500. Anything
  above returns `401 "Restricted offset for guest users"`. If USM ever gets moved
  to the authenticated tier the ceiling becomes 2,000 and the trie collapses from
  ~3,600 buckets to a few hundred.
