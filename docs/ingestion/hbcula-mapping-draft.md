# HBCU Library Alliance — Draft OAI Qualified Dublin Core → DPLA Mapping

**Status:** DRAFT / test hub — not approved for production, not synced to the index.
See [README_TEST_HUBS.md](README_TEST_HUBS.md).

- **Provider (hub):** HBCU Library Alliance — the HBCU Library Alliance Digital
  Collections, a shared CONTENTdm repository (`hbcudigitallibrary.auctr.edu`)
  aggregating the collections of 30+ member institutions as separate OAI sets.
- **Metadata format:** OAI Qualified Dublin Core (`oai_qdc`) — `dc:`/`dcterms:`
  elements inside an OAI-PMH `<record>`. One `<record>` per item.
- **Mapper:** [`HbculaMapping.scala`](../../src/main/scala/dpla/ingestion3/mappers/providers/experimental/HbculaMapping.scala)
- **Tests:** *(deferred — no `HbculaMappingTest` or sample fixtures yet; see §4.)*
- **Basis:** this draft documents the **existing experimental mapper** (built April
  2026 against the live CONTENTdm OAI-PMH feed, registry key `hbcula`). It records what
  the mapper currently does; field-by-field validation against a fresh sample set is
  **deferred** (see §4).
- **Harvest method:** OAI-PMH via the generic
  [`LocalOaiHarvester`](../../src/main/scala/dpla/ingestion3/harvesters/oai/LocalOaiHarvester.scala)
  (`harvest.type = "localoai"`) — no custom harvester class. Scope is set per member
  institution via the OAI setlist.
- **DPLA model & serialization:** field types in
  [`DplaMapData.scala`](../../src/main/scala/dpla/ingestion3/model/DplaMapData.scala);
  base field defaults and required/optional validation flags in the
  [`Mapping`](../../src/main/scala/dpla/ingestion3/mappers/utils/Mapping.scala) trait;
  the JSON-L index serializer in
  [`model/package.scala`](../../src/main/scala/dpla/ingestion3/model/package.scala).
  Registered via [`CHProviderProfiles.scala`](../../src/main/scala/dpla/ingestion3/profiles/CHProviderProfiles.scala)
  (`HbculaProfile`) and [`CHProviderRegistry.scala`](../../src/main/scala/dpla/ingestion3/utils/CHProviderRegistry.scala)
  (registry key `hbcula`).

Notes on notation: `\` = direct child, `\\` = descendant. `@x` = attribute. The mapper
anchors to the OAI `<record>` and reads descriptive fields under `\ "metadata"` via
descendant (`\\`) selectors, so a `dc:`/`dcterms:` prefix is matched by local name
regardless of namespace declaration.

---

## 1. Mapped elements (oai_qdc source → DPLA field)

### OreAggregation (object-level)

| DPLA field | Source | Logic / notes |
|---|---|---|
| `dplaUri` | *(minted)* | `mintDplaItemUri` — hash of the salted `originalId`. |
| *(originalId — for ID minting & sidecar)* | `header/identifier` | The OAI record header identifier. Salted with provider name `hbcula`. |
| `provider` | *(constant)* | `EdmAgent("HBCU Library Alliance", uri=http://dp.la/api/contributor/hbcula)`. |
| `dataProvider` | `metadata\\source` (`dc:source`) | Split on `;`, `nameOnlyAgent`, **first value only**. Holds the member institution name. See §4 — some sets omit it. |
| `isShownAt` | `metadata\\identifier` (`dc:identifier`) | First value that `startsWith("http")` → `stringOnlyWebResource`. The CONTENTdm item landing page. |
| `preview` (thumbnail) | *(constructed)* | Built from the http `identifier`: a CONTENTdm item URL `…/cdm/ref/collection/{collection}/id/{id}` is parsed for `{collection}` and `{id}` and rewritten to `http://hbcudigitallibrary.auctr.edu/utils/getthumbnail/collection/{collection}/id/{id}`. Emitted only when both parts parse. Serialized to the API as the field literally named `object`. |
| `originalRecord` | *(whole record)* | Full record XML, `Utils.formatXml`. |
| `sidecar` | *(minted)* | `prehashId` + `dplaId`. |

### SourceResource (descriptive)

| DPLA field | Source | Logic / notes |
|---|---|---|
| `title` | `metadata\\title` (`dc:title`) | Trailing `.` stripped. |
| `creator` | `metadata\\creator` (`dc:creator`) | Split on `;` → `nameOnlyAgent`. |
| `contributor` | `metadata\\contributor` (`dc:contributor`) | Split on `;` → `nameOnlyAgent`. |
| `date` | `metadata\\date` (`dc:date`) | Split on `;` → `stringOnlyTimeSpan` (displayDate only). |
| `description` | `metadata\\description` (`dc:description`) | All values, verbatim. |
| `format` | `metadata\\format` (`dc:format`) | Trailing `;` stripped. Plain string. |
| `language` | `metadata\\language` (`dc:language`) | Split on `;` → `nameOnlyConcept`. |
| `place` | `metadata\\spatial` (`dcterms:spatial`) | Split on `;` → `nameOnlyPlace` (name only). |
| `rights` | `metadata\\rights` (`dc:rights`) | Free-text rights statement, verbatim. |
| `subject` | `metadata\\subject` (`dc:subject`) | Split on `;` → `nameOnlyConcept`. |
| `type` | `metadata\\type` (`dc:type`) | Split on `;`. Plain string. |
| `collection` | `metadata\\isPartOf` (`dcterms:isPartOf`) | → `nameOnlyCollection`. |

**Config:** `useProviderName = true`, `getProviderName = "hbcula"`.

---

## 2. Source fields not currently mapped

The mapper reads only the elements in §1. Other `dc:`/`dcterms:` elements that may
appear in the feed (e.g. `dc:publisher`, `dcterms:temporal`, `dcterms:extent`,
`dcterms:medium`, `dc:relation`, `dc:coverage`, and non-http `dc:identifier` values)
are **not currently mapped**. A field-by-field inventory of the actual feed and any
mapping decisions for these are **deferred** (see §4) — no source-field audit has been
done for this draft.

---

## 3. DPLA fields — coverage

### Required fields

The hard-required DPLA fields (a record is rejected without them) are mapped:
`dplaUri`, `dataProvider`, `isShownAt`, `title`, `rights`, and a persistent
`originalId`.

> **`dataProvider` caveat (already decided in code):** `dataProvider` comes from
> `dc:source`. Some sets (e.g. `rwwl`) omit `dc:source` from the OAI export even though
> the CONTENTdm Repository field is populated. The mapper does **not** paper over this —
> records without `dc:source` fail the required-field check cleanly. An earlier
> `dcterms:isPartOf` fallback was added and then deliberately reverted; the resolution
> is for HBCULA to fix the export (see §4).

### Recommended / opportunities — deferred

`edmRights` (a standardized rights URI), and any richer mapping of the §2 fields, are
**not** currently mapped. Whether these can be populated from the feed is **deferred**
pending a sample-based review. `preview` (thumbnail) **is** mapped (constructed), so it
is not a gap.

---

## 4. Notes, disclaimers, and deferred work

This hub was drafted in the April 2026 test-hub cycle (alongside Dartmouth and AAPB)
and is being carried onto a clean feature branch here. **Focus has shifted to NGA**, so
the HBCULA intellectual work below is intentionally left **deferred** — this doc records
the current state without making new mapping decisions.

### Decided (recorded, not open)

- **`dataProvider` = `dc:source`, no fallback.** The `dcterms:isPartOf` fallback for
  `dataProvider` was tried and reverted; records lacking `dc:source` should fail
  cleanly rather than be back-filled. The fix belongs on HBCULA's export.

### Deferred

- **No mapping test / fixtures.** `HbculaMappingTest` and sample record fixtures under
  `src/test/resources/` are not yet written. (Contrast Dartmouth/AAPB, which each ship a
  test.)
- **No source-field audit.** §2 has not been validated against a real sample set; the
  list of present-but-unmapped fields is indicative, not exhaustive.
- **No end-to-end / full-feed validation.** Unlike AAPB, no full test harvest + pipeline
  run has been done to measure map/reject rates.
- **`edmRights` and richer field coverage** (§3) — pending sample review.

### Open questions for the partner (HBCU Library Alliance)

- **Missing `dc:source` on some sets** (e.g. `rwwl`): can the OAI export be corrected so
  every set emits the holding institution in `dc:source`? Until then, those sets'
  records will be rejected for missing `dataProvider`.

### Reminder

Everything here is DRAFT. The hub is `status = test`; output is not synced to S3 and
cannot reach the index. Field decisions should be reviewed with HBCULA before any
production consideration.

---

## 5. Test-ingest results (full pipeline, EC2, 2026-07-28)

A complete harvest → mapping → enrichment → JSON-L run on the ingest EC2 instance
(test hub — **not** synced to S3), from a **fresh OAI harvest**. Total pipeline ~3m27s.

> **Re-run vs the 2026-04-10 baseline.** The fresh harvest picked up **+18 new records**
> (5,201 → 5,219 attempted), but **135 fewer mapped** (3,952 → 3,817): "missing
> `dataProvider`" errors rose from 1,245 to **1,398 (+153)**. The `dc:source` gap
> (§3/§4) is **widening** — newly added records also omit it — which is worth raising
> with HBCULA.

### Totals

| | count | of harvested |
|---|---|---|
| Harvested (OAI records) | 5,219 | — |
| **Mapped → JSON-L** | **3,817** | **73.1%** |
| Failed (rejected) | 1,402 | 26.9% |

### Errors (reject the record)

| reason | records |
|---|---|
| Missing required field: dataProvider | 1,398 |
| Missing required field: rights or edmRights | 243 |

The dominant — and growing — failure is **missing `dataProvider`** (sets whose OAI
export omits `dc:source`, e.g. `rwwl`; see §3/§4). That is the top data issue to raise
with HBCULA. (1,641 error messages across 1,402 rejected records — some fail both checks.)

### Warnings (informational; do not reject)

| reason | records |
|---|---|
| Missing recommended: publisher | 5,219 |
| Missing recommended: place | 2,439 |
| Missing recommended: creator | 1,824 |
| Missing recommended: type | 202 |
| Missing recommended: date | 13 |
| Missing recommended: subject / format / language / description | ≤5 each |

### Field coverage (of the 3,817 mapped records)

| field | coverage |
|---|---|
| `dataProvider`, `provider`, `isShownAt`, `preview`(object), `title`, `rights` (free text) | 100% |
| `description`, `language` | ~100% (3,816) |
| `format`, `subject`, `date` | 99.9% |
| `place` | 64.7% |
| `creator` | 59.5% |
| `type` | 21.5% |
| `collection` | 16.4% |
| `edmRights`, `mediaMaster` | 0% (not mapped — plain-text `dc:rights`, no rights URI; no media-master field) |

HBCULA satisfies the rights requirement via free-text `dc:rights` (100%), not a
rights URI.
