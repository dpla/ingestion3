# HBCU Library Alliance — Draft OAI Qualified Dublin Core → DPLA Mapping

**Status:** DRAFT / test hub — not approved for production. `hbcula.status = test` and
`hbcula.included_in_index = false` in i3.conf, so the index launcher
(`ingest_python_scripts/launch_indexer.py`) excludes it even when its JSON-L is in S3.
See [README_TEST_HUBS.md](README_TEST_HUBS.md).

- **Provider (hub):** HBCU Library Alliance — the HBCU Library Alliance Digital
  Collections, a shared CONTENTdm repository (`hbcudigitallibrary.auctr.edu`, hosted by
  the AUC Robert W. Woodruff Library) with one OAI set per member institution.
- **Feed (2026-10-01):** 32 sets; 13,828 OAI records, of which 8,540 are `status="deleted"`
  tombstones (dropped by the harvester) and **5,288 are live**.
- **Metadata format:** OAI Qualified Dublin Core (`oai_qdc`) — `dc:`/`dcterms:`
  elements inside an OAI-PMH `<record>`. One `<record>` per item.
- **Mapper:** [`HbculaMapping.scala`](../../src/main/scala/dpla/ingestion3/mappers/providers/experimental/HbculaMapping.scala)
- **Tests:** [`HbculaMappingTest.scala`](../../src/test/scala/dpla/ingestion3/mappers/providers/experimental/HbculaMappingTest.scala)
  (19 tests) against five real feed records, `src/test/resources/hbcula-{vsud,rwwl,becu,psua,suam}.xml`.
- **Harvest method:** OAI-PMH via the generic
  [`LocalOaiHarvester`](../../src/main/scala/dpla/ingestion3/harvesters/oai/LocalOaiHarvester.scala)
  (`harvest.type = "localoai"`, `metadataPrefix = "oai_qdc"`, endpoint
  `https://hbcudigitallibrary.auctr.edu/oai/oai.php`). No setlist — all sets are harvested.
- **DPLA model & serialization:** field types in
  [`DplaMapData.scala`](../../src/main/scala/dpla/ingestion3/model/DplaMapData.scala);
  base field defaults and required/optional validation flags in the
  [`Mapping`](../../src/main/scala/dpla/ingestion3/mappers/utils/Mapping.scala) trait;
  the JSON-L index serializer in
  [`model/package.scala`](../../src/main/scala/dpla/ingestion3/model/package.scala).
  Registered via [`CHProviderProfiles.scala`](../../src/main/scala/dpla/ingestion3/profiles/CHProviderProfiles.scala)
  (`HbculaProfile`) and [`CHProviderRegistry.scala`](../../src/main/scala/dpla/ingestion3/utils/CHProviderRegistry.scala)
  (registry key `hbcula`).

Notes on notation: `\` = direct child, `\\` = descendant. The mapper anchors to the OAI
`<record>` and reads descriptive fields under `\ "metadata"` via descendant (`\\`)
selectors, so a `dc:`/`dcterms:` prefix is matched by local name.

---

## 1. Mapped elements (oai_qdc source → DPLA field)

### OreAggregation (object-level)

| DPLA field | Source | Logic / notes |
|---|---|---|
| `dplaUri` | *(minted)* | `mintDplaItemUri` — hash of the salted `originalId`. |
| *(originalId — for ID minting & sidecar)* | `header/identifier` | e.g. `oai:hbcudigitallibrary.auctr.edu:VSUD/115`. Salted with provider name `hbcula`. |
| `provider` | *(constant)* | `EdmAgent("HBCU Library Alliance", uri=http://dp.la/api/contributor/hbcula)`. |
| `dataProvider` | `dc:source` | Split on `;`, `nameOnlyAgent`, **first value only**, as given (no cleanup, no fallback). See §4. |
| `isShownAt` | `dc:identifier` | First value that `startsWith("http")`, **as given** — `http://…/cdm/ref/collection/{coll}/id/{id}` (the site redirects it to the current `https://…/digital/collection/…` page). |
| `preview` (thumbnail) | *(constructed)* | `{coll}` and `{id}` parsed from the `isShownAt` URL → `https://hbcudigitallibrary.auctr.edu/utils/getthumbnail/collection/{coll}/id/{id}`. Emitted only when both parse. Serialized to the API as `object`. |
| `originalRecord` | *(whole record)* | Full record XML, `Utils.formatXml`. |
| `sidecar` | *(minted)* | `prehashId` + `dplaId`. |

### SourceResource (descriptive)

| DPLA field | Source | Logic / notes |
|---|---|---|
| `title` | `dc:title` | Trailing period stripped (`stripEndingPeriod`; ellipses kept). |
| `creator` | `dc:creator` | Split on `;` → `nameOnlyAgent`. |
| `contributor` | `dc:contributor` | Split on `;` → `nameOnlyAgent`. (Not present in the current feed.) |
| `date` | `dc:date` | Split on `;` → `stringOnlyTimeSpan` (displayDate only). |
| `description` | `dc:description` | All values, verbatim. |
| `format` | `dc:format` | Trailing punctuation and whitespace stripped (`cleanupEndingPunctuation`). Values are MIME types (`image/jpeg`, `application/pdf`). |
| `identifier` | `dc:identifier` | **All values, as given** — the local file/call number (e.g. `auc.001.bgx3.00000000.pho0039.jpg`, `becu.0117`) and the CONTENTdm URL. |
| `language` | `dc:language` | Split on `;` → `nameOnlyConcept`. |
| `place` | `dcterms:spatial` | Split on `;` → `nameOnlyPlace`. |
| `relation` | `dc:relation` | `eitherStringOrUri`. |
| `rights` | `dc:rights` | Free-text rights statement, verbatim. |
| `subject` | `dc:subject` | Split on `;` (empty segments dropped) → `nameOnlyConcept`. |
| `type` | `dc:type` | Split on `;`. Plain string. |
| `collection` | `dcterms:isPartOf` | → `nameOnlyCollection`. |

**Config:** `useProviderName = true`, `getProviderName = "hbcula"`.

---

## 2. Source field inventory (full live feed, 2026-10-01)

Every element present in the 5,288 live records, with the number of records carrying it.
**Every element is mapped** — the feed has no `dc:publisher`, `dc:coverage`,
`dcterms:temporal`, `dcterms:extent`, `dcterms:medium` or `dc:contributor`.

| Element | Records | DPLA field |
|---|---|---|
| `dc:identifier` | 5,288 (all have the CONTENTdm URL; 4,368 also a local ID) | `isShownAt`, `identifier`, `preview` (derived) |
| `dc:title` | 5,288 | `title` |
| `dc:description` | 5,286 | `description` |
| `dc:language` | 5,285 | `language` |
| `dc:format` | 5,284 | `format` |
| `dc:subject` | 5,283 | `subject` |
| `dc:date` | 5,274 | `date` |
| `dc:type` | 5,086 | `type` |
| `dc:rights` | 5,045 | `rights` |
| `dc:source` | 3,923 | `dataProvider` |
| `dc:creator` | 3,464 | `creator` |
| `dcterms:spatial` | 2,849 | `place` |
| `dcterms:isPartOf` | 1,082 | `collection` |
| `dc:relation` | 245 | `relation` |

`dc:relation` is almost all one value — Bethune-Cookman's library homepage
(`https://www.cookman.edu/library/index.html`, 242 records); the other 3 are links to
items or collections on the CONTENTdm site.

The OAI set name (`setName` in `ListSets`, e.g. `aamu` → "Alabama A&M University") is the
only institution-level data outside the records. It is not mapped (see §4).

---

## 3. DPLA fields — coverage

### Required fields

`dplaUri`, `isShownAt`, `title` and a persistent `originalId` are present on every record.
Two required fields are missing on part of the feed, so those records are rejected:

- **`dataProvider`** — 1,365 records have no `dc:source` (see §4, the main issue).
- **`rights` or `edmRights`** — 243 records have no `dc:rights` (`rwwl` 179, `suam` 60,
  `FUPP` 4).

From the 2026-10-01 feed, **3,886 of 5,288 (73.5%)** records satisfy both; 1,402 would be
rejected.

### Not mapped

- **`edmRights`** — no record carries a rightsstatements.org or creativecommons.org URI;
  `dc:rights` is free text everywhere. Per policy, no URI is inferred from the text.
- **`mediaMaster` / `iiifManifest`** — not in the feed. CONTENTdm serves IIIF for its
  items, but the OAI feed doesn't expose manifest URLs; adding them would mean
  constructing URLs, which should be discussed with the hub first.
- **`publisher`, `temporal`, `extent`** — no source element (§2).

---

## 4. Notes, decisions, and open questions for the partner

### Decided

- **`dataProvider` = first `dc:source` value, as given, no fallback.** A `dcterms:isPartOf`
  fallback was tried in April 2026 and reverted: `isPartOf` is a collection title, not an
  institution. Records without `dc:source` fail the required-field check.
- **`isShownAt` is the identifier URL exactly as the feed gives it** (`http://…/cdm/ref/…`),
  not rewritten to the newer `/digital/collection/` form; the site redirects it.
- **Thumbnails are built as `https`** — the URL is constructed by the mapper anyway, and
  the server serves it on both schemes.

### Open questions for HBCULA

1. **Missing `dc:source` (the main blocker — 1,365 records, 25.8% of the feed).** Six
   sets omit it entirely and three omit it on some records:

   | Set | Institution (OAI set name) | Live records | Missing `dc:source` |
   |---|---|---|---|
   | `becu` | Bethune-Cookman University | 313 | 313 |
   | `aamu` | Alabama A&M University | 303 | 303 |
   | `rwwl` | Atlanta University Center (AUC) Robert W. Woodruff Library | 288 | 288 |
   | `bcac` | Benedict College | 185 | 185 |
   | `GSBG` | Grambling State Black-Gold Collections | 179 | 179 |
   | `lumo` | 52 Steps, the Journey of Lincoln University of Missouri | 62 | 62 |
   | `suam` | Southern University and A&M College | 419 | 27 |
   | `UDCW` | UDC Digital Archives Collection | 256 | 6 |
   | `lane` | Lane College Early History Digital Collection | 54 | 2 |

   The gap is growing: 1,245 records were rejected for this in April 2026 and 1,398 in
   July. Can the export emit the holding institution in `dc:source` for every set? If
   HBCULA instead wants DPLA to use the OAI set name as the institution, that is the
   hub's decision to make — but set names are collection titles for some sets (e.g.
   `lumo`, `GSBG`), so they aren't a clean institution name either.
2. **`dc:source` values that aren't institution names (853 records).** These map, but the
   resulting `dataProvider` is a URL or other text:

   | Set | First `dc:source` value | Records |
   |---|---|---|
   | `abco` | `https://library.abcnash.edu/home` | 332 |
   | `shaw` | `Archives Site \| https://shawu.libguides.com/archives#s-lg-box-30734527` | 277 |
   | `psua` | `Library: https://libguides.philander.edu/home` | 104 |
   | `allu` | `https://allenuniversity.libguides.com/` | 82 |
   | `lane` | `https://www.lanecollege.edu/academics/library` | 52 |
   | `psua`, `shaw` | a copyright statement in `dc:source` | 6 |

   DPLA maps these as given; the fix is the institution's name in `dc:source`.
3. **Missing `dc:rights` (243 records)** in `rwwl`, `suam` and `FUPP` — can a rights
   statement be added? And would HBCULA's members consider adding rightsstatements.org
   URIs, which would populate `edmRights`?
4. **Varying institution names within a set.** 44 distinct `dataProvider` values come from
   32 sets; `UDCW` alone has 10 variants. DPLA doesn't normalize names, so each variant
   appears as a separate institution — worth flagging so members can make them consistent.

### Reminder

Everything here is DRAFT. Field decisions should be reviewed with HBCULA before any
production consideration. To graduate the hub, follow
[README_TEST_HUBS.md](README_TEST_HUBS.md#graduating-a-test-hub-to-production).

---

## 5. Test-ingest results (full pipeline, EC2, 2026-07-28)

A complete harvest → mapping → enrichment → JSON-L run on the ingest EC2 instance, from a
fresh OAI harvest. Total pipeline ~3m27s. This run used the **previous** mapper (before
`identifier`, `relation`, the title/format punctuation helpers and the https thumbnail were added); those changes add
fields but don't change which records pass or fail. The 2026-10-01 feed figures in §3
are the current expectation (5,288 live, 3,886 mapping).

### Totals

| | count | of harvested |
|---|---|---|
| Harvested (OAI records) | 5,219 | — |
| **Mapped → JSON-L** | **3,817** | **73.1%** |
| Failed (rejected) | 1,402 | 26.9% |

Compared with the 2026-04-10 baseline (5,201 harvested, 3,952 mapped), "missing
`dataProvider`" rejections rose from 1,245 to 1,398.

### Errors (reject the record)

| reason | records |
|---|---|
| Missing required field: dataProvider | 1,398 |
| Missing required field: rights or edmRights | 243 |

(1,641 error messages across 1,402 rejected records — some fail both checks.)

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
| `edmRights`, `mediaMaster` | 0% (no rights URI or media-master source in the feed) |

### Where the data lives

- **EC2:** `/home/ec2-user/data/hbcula/{harvest,mapping,enrichment,jsonl}/` — runs from
  2026-04-10 (×2) and 2026-07-28, plus a JSON-L-only re-export on 2026-08-19.
- **S3:** `s3://dpla-master-dataset/hbcula/jsonl/` holds those four JSON-L snapshots,
  copied by the 2026-08-19 bulk JSON-L re-export of all hubs. They stay out of the index
  because of `included_in_index = false` (see the status note at the top).
