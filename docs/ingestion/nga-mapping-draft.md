# National Gallery of Art — Draft CSV → DPLA Mapping

**Status:** DRAFT / test hub — not approved for production, not synced to the index.
See [README_TEST_HUBS.md](README_TEST_HUBS.md).

- **Provider (hub):** National Gallery of Art (Washington, D.C.) — a single-institution hub.
- **Metadata format:** CC0 CSV files from NGA's Open Data Program
  ([github.com/NationalGalleryOfArt/opendata](https://github.com/NationalGalleryOfArt/opendata)),
  exported from their TMS collection-management system, UTF-8, refreshed ~daily.
- **Harvester:** [`NgaFileHarvester`](../../src/main/scala/dpla/ingestion3/harvesters/file/NgaFileHarvester.scala)
  (`harvest.type = "file"`). Acquisition: [`scripts/harvest/harvest-nga.sh`](../../scripts/harvest/harvest-nga.sh)
  shallow-clones the repo; the harvester **assembles one JSON document per object**
  by joining the relational CSVs (a DPLA `OriginalRecord` must be self-contained —
  the mapper does no cross-file joins):
  `objects.csv` (core) + `published_images.csv` (via `depictstmsobjectid`) +
  `objects_constituents.csv` + `constituents.csv` (creators, with ULAN/Wikidata IDs) +
  `objects_terms.csv` + `objects_text_entries.csv`.
- **Mapper:** [`NgaMapping.scala`](../../src/main/scala/dpla/ingestion3/mappers/providers/experimental/NgaMapping.scala)
  (a `JsonMapping` over the assembled document).
- **Tests:** [`NgaMappingTest.scala`](../../src/test/scala/dpla/ingestion3/mappers/providers/experimental/NgaMappingTest.scala)
  (against real assembled records for objects 55035 and 88206) and
  [`NgaFileHarvesterTest.scala`](../../src/test/scala/dpla/ingestion3/harvesters/file/NgaFileHarvesterTest.scala).
- **Basis:** 5 sample records inspected + full-dataset field profiling (July 2026):
  **145,566 objects**; 82% have ≥1 published image; **43% (63,409) have ≥1
  open-access image**; 18% have no image. Not yet run through the full pipeline.
- **DPLA model & serialization:** field types in
  [`DplaMapData.scala`](../../src/main/scala/dpla/ingestion3/model/DplaMapData.scala);
  required/optional validation in the
  [`Mapping`](../../src/main/scala/dpla/ingestion3/mappers/utils/Mapping.scala) trait
  and [`Mapper.validateRights`](../../src/main/scala/dpla/ingestion3/mappers/Mapper.scala);
  JSON-L serializer in [`model/package.scala`](../../src/main/scala/dpla/ingestion3/model/package.scala).
  Registered via [`CHProviderProfiles.scala`](../../src/main/scala/dpla/ingestion3/profiles/CHProviderProfiles.scala)
  (`NgaProfile`) and [`CHProviderRegistry.scala`](../../src/main/scala/dpla/ingestion3/utils/CHProviderRegistry.scala)
  (registry key `nga`).

Notation: fields below are keys in the assembled JSON document; nested arrays are
`images`, `constituents` (each carrying its looked-up `constituent` record),
`terms`, and `textEntries`.

---

## 1. Mapped elements (assembled record → DPLA field)

### OreAggregation (object-level)

| DPLA field | Source | Logic / notes |
|---|---|---|
| `dplaUri` | *(minted)* | `mintDplaItemUri` — hash of the salted `originalId`. |
| *(originalId)* | `objectid` | The stable TMS object id. Salted with provider name `nga`. |
| `provider` | *(constant)* | `EdmAgent("National Gallery of Art", uri=http://dp.la/api/contributor/nga)`. |
| `dataProvider` | *(constant)* | `nameOnlyAgent("National Gallery of Art")` — single-institution hub. |
| `isShownAt` | *(constructed)* | `https://www.nga.gov/collection/art-object-page.{objectid}.html` — the legacy path, which 301-redirects to the canonical `/artworks/{id}-{slug}` page. Chosen because the slug is not reliably reconstructable (see §4). |
| `preview` | primary image `iiifthumburl` | The primary image's IIIF thumbnail (`…/full/!200,200/0/default.jpg`). Serialized to the API as `object`. |
| `mediaMaster` | every image's `iiifurl` | `{iiifurl}/full/full/0/default.jpg` for **each** image (multivalue — e.g. all plates of a portfolio). The fullest image NGA serves; **not** gated on rights (see §4). |
| `edmRights` | `openaccess` | CC0 URI (`https://creativecommons.org/publicdomain/zero/1.0/`) when the object has any `openaccess=1` image; otherwise empty. This is the **only** rights signal in the data (see §4). |
| `originalRecord` | *(whole record)* | Full assembled JSON, `Utils.formatJson`. |
| `sidecar` | *(minted)* | `prehashId` + `dplaId`. |

### SourceResource (descriptive)

| DPLA field | Source | Logic / notes |
|---|---|---|
| `title` | `title` | |
| `creator` | `constituents[roletype="artist"]` → `forwarddisplayname` | Includes role "artist after". **+ `exactMatch`**: Getty ULAN (`http://vocab.getty.edu/ulan/{ulanid}`) and/or Wikidata (`http://www.wikidata.org/entity/{wikidataid}`) from the joined `constituent` record. Donors/owners are **not** mapped as agents (provenance). |
| `date` | `displaydate` (+ `beginyear`/`endyear`) | → `EdmTimeSpan(begin, end, displayDate)`. |
| `description` | `textEntries[texttype="brief_narrative"]` **+** primary image `assistivetext` | `assistivetext` is NGA's per-image **"Visual Description"** (the rich text shown under that heading on nga.gov). brief_narrative is usually absent, so the visual description is the main descriptive text. |
| `subject` | `terms[termtype ∈ {Keyword, Theme, Style, School, Technique}]` → `term` | → `SkosConcept`. |
| `place` | `terms[termtype="Place Executed"]` → `term` | → `DplaPlace(name)`. |
| `format` | `classification` + `medium` | e.g. `["Print", "engraving"]`, distinct. |
| `extent` | `dimensions` | e.g. "overall: 25.2 x 20.2 cm (9 15/16 x 7 15/16 in.)". |
| `identifier` | `accessionnum` | e.g. "1990.28.3030". |
| `collection` | `series` + `portfolio` + `volume` | → `DcmiTypeCollection`, when present. |
| `type` | `classification` | Volume → `text`; Time-Based Media Art → `moving image`; all other classifications → `image`. See §4. |

**Config:** `useProviderName = true`, `getProviderName = "nga"`.

---

## 2. Source fields present but not mapped

- **`provenancetext`** — free-text provenance; no DPLA equivalent (donors/owners live here and in `objects_constituents`).
- **`creditline`** — *acquisition* credit (e.g. "Ailsa Mellon Bruce Fund"), **not** a rights/copyright statement — deliberately not used for `rights`.
- **`markings` / `inscription` / `watermarks`** — descriptions of marks physically on the artwork (this is where the ~3,448 `©` symbols in `objects.csv` live — copyright stamps *on* the work, not rights metadata).
- **`subclassification` / `visualbrowserclassification` / `visualbrowsertimespan` / `visualbrowsernationality`** — internal/normalized browse helpers.
- **`departmentabbr`, `locationid`, `parentid`, `isvirtual`, `customprinturl`, `lastdetectedmodification`, `accessioned`** — administrative/structural.
- **object-level `wikidataid`** — no OreAggregation slot for an object sameAs/exactMatch (see §4).
- **`terms[termtype="Systematic Catalogue Volume"]`** — a catalogue reference, not a subject.
- **`constituents[roletype ∈ {donor, owner}]`** and their `prefix/suffix/displaydate/country/zipcode`** — provenance transactions.
- **Un-joined CSVs:** `alternative_identifiers.csv` (Wikidata/ULAN — redundant with the joined fields — plus catalogue-raisonné numbers: Faille, Hulsker, RKDartists, Stieglitz), `objects_dimensions.csv` (structured dims; `objects.dimensions` text is used instead), `objects_historical_data.csv` (previous attributions/titles), `object_associations.csv` (parent/child structure), `constituents_altnames.csv`, `constituents_text_entries.csv`, `locations.csv`, `preferred_locations*.csv`.

---

## 3. DPLA fields — coverage

### Required fields (record rejected if missing) — satisfied for open-access items

`dplaUri`, `dataProvider`, `isShownAt`, `title`, `originalId` are always present.
The **rights** requirement is satisfied by `edmRights` (a record is rejected only
when *both* `rights` and `edmRights` are empty — see
[`Mapper.validateRights`](../../src/main/scala/dpla/ingestion3/mappers/Mapper.scala)),
so open-access items pass on their CC0 `edmRights` URI. **Items with no open-access
image have no rights signal at all and are intentionally rejected** — this scopes
NGA's contribution to its ~63,409 open-access works (see §4).

### Recommended / present

- `preview` (thumbnail), `mediaMaster` (full images), `edmRights` (CC0) — all populated for open-access items.
- `description` — populated from the image Visual Description even when no narrative text exists.

---

## 4. Notes, disclaimers, and OPEN QUESTIONS for the partner

### Rights — the central issue (OPEN QUESTION for NGA)

NGA's CSVs carry **no rights field**. The only rights signal is the per-image
`openaccess` flag (1 = CC0 under NGA's Open Access policy). Consequently:

- Open-access items → `edmRights` = CC0 (preferred, standardized URI).
- The other ~57% (restricted-image-only, or image-less) have **no** rights and are
  **dropped by design** rather than given a fabricated statement.

But NGA clearly *holds* richer rights data: their web UI shows copyright lines (e.g.
"© Robert Frank Foundation" for object 88206), and their own extractor
(`scripts/extract_opendata.py` / `tables.sql`) explicitly filters
`ri_photocredit is null` — i.e. it **excludes** images that carry a photo credit.
**Recommendation / question:** can NGA export the rights/photo-credit field (or a
`rightsstatements.org` URI per item)? That would let the restricted works map with a
proper rights statement instead of being dropped — potentially doubling NGA's
contribution.

### Other open questions & provisional choices

- **`isShownAt`** uses the legacy `/collection/art-object-page.{id}.html`, which
  301-redirects to the canonical `/artworks/{id}-{slug}`. The bare `/artworks/{id}`
  (no slug) 404s, and the slug is not reliably reconstructable from the data
  (accents, punctuation, NGA's exact rules). *Option:* build & validate a slug
  function to emit the canonical URL directly and avoid the redirect hop.
- **`description` uses `assistivetext`** — NGA's per-image Visual Description. This
  reads as machine-/accessibility-generated alt text ("The image shows…"). It is the
  richest description available; **confirm it is acceptable as DPLA `description`.**
- **`type`** is derived from the 12-value `classification` (a clean signal, unlike
  free-text `medium`). `Technical Material` (366) and `Ephemera (non-NGA)` (1)
  currently default to `image` — review whether that's right.
- **`iiifManifest` is not populated** — NGA exposes only the IIIF **Image API 2.1**
  (`api.nga.gov/iiif/{uuid}`); there is no IIIF **Presentation** manifest in their
  repo or at any probed endpoint. `mediaMaster` (Image API full URLs) is the
  best available full-image source.
- **Object-level `wikidataid`** is not mapped (no OreAggregation field for it), though
  it would be useful for Wikimedia entity matching. Creator ULAN/Wikidata IDs *are*
  captured as `exactMatch` (consumed by the Wikimedia/Wikidata linking step).
- **Composite objects** (portfolios/albums) appear both as single objects with many
  images (→ multivalue `mediaMaster`) and as separate parent/child objects
  (`object_associations.csv`, not yet used). Confirm the desired treatment.
- **Everything here is DRAFT.** The hub is `status = test`; output is not synced to S3
  and cannot reach the index. Field decisions should be reviewed with NGA.

---

## 5. Test-ingest results (full pipeline, EC2, 2026-07-28)

A complete harvest → mapping → enrichment → JSON-L run of the entire dataset on the
ingest EC2 instance (test hub — **not** synced to S3). Mapping runtime ~28s.

### Totals

| | count | of harvested |
|---|---|---|
| Harvested (objects.csv rows) | 145,566 | — |
| **Mapped → JSON-L** | **63,409** | **43.6%** |
| Failed (rejected) | 82,157 | 56.4% |

Every failure is the intended rights gate — see errors below.

### Errors (reject the record)

| reason | records |
|---|---|
| Missing required field: rights or edmRights | 82,157 |

i.e. every object with **no open-access image** is dropped (no rights signal), exactly
as designed (§4). No other error type occurs.

### Warnings (informational; do not reject)

| reason | records |
|---|---|
| Missing recommended: publisher | 145,566 |
| Missing recommended: language | 145,566 |
| Missing recommended: place | 134,022 |
| Normalized https→http, edmRights | 63,409 |
| Missing recommended: description | 30,522 |
| Missing recommended: subject | 5,030 |
| Missing recommended: date | 47 |

`publisher` and `language` have no source in the data (expected). The `edmRights`
"normalization" is the enricher rewriting the CC0 URI to its `http` form.

### Field coverage (of the 63,409 mapped records)

| field | coverage |
|---|---|
| `dataProvider`, `provider`, `isShownAt`, `edmRights`, `preview`(object), `mediaMaster` | 100% |
| `title`, `creator`, `format`, `identifier`, `type` | 100% |
| `date` | ~100% (63,385) |
| `subject` | 98.2% |
| `description` (mostly image Visual Description) | 97.3% |
| `extent` | 83.0% |
| `collection` | 23.1% |
| `place` | 15.5% |
| `contributor`, `language`, `publisher`, `temporal`, `rights` (free text) | 0% (not mapped / no source) |

Every mapped record carries the full media set (preview + full-res mediaMaster) and
CC0 edmRights — consistent with the open-access-only scope.
