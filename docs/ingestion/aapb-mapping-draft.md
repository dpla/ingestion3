# American Archive of Public Broadcasting — Draft PBCore → DPLA Mapping

**Status:** DRAFT / test hub — not approved for production, not synced to the index.
See [README_TEST_HUBS.md](README_TEST_HUBS.md).

- **Provider (hub):** American Archive of Public Broadcasting (AAPB) — a
  collaboration of GBH and the Library of Congress.
- **Metadata format:** PBCore 2.x (Public Broadcasting Metadata Dictionary), an XML
  standard derived from Dublin Core. One `<pbcoreDescriptionDocument>` per asset.
- **Mapper:** [`AapbMapping.scala`](../../src/main/scala/dpla/ingestion3/mappers/providers/experimental/AapbMapping.scala)
- **Tests:** [`AapbMappingTest.scala`](../../src/test/scala/dpla/ingestion3/mappers/providers/experimental/AapbMappingTest.scala)
- **Basis:** 4 sample records — two **live** AAPB PBCore records pulled from
  `americanarchive.org` (`cpb-aacip_513-000000145w`, a digitized/online "Raw
  Footage" asset with rights + ARK; and `cpb-aacip_37-95j9krh1`, a rights-bearing
  but *not*-online "Series" record) and two records from
  [pbcore.org/sample-records](https://pbcore.org/sample-records)
  (`The Great American Footrace`; `Prospects of Mankind with Eleanor Roosevelt`).
  All four were run end-to-end (map → enrich → JSON-L) and produce valid DPLA
  records (see §4 on the one that fails required-field validation).
- **Harvest method:** AAPB's Solr API via
  [`AapbHarvester`](../../src/main/scala/dpla/ingestion3/harvesters/api/AapbHarvester.scala)
  (`harvest.type = "api"`). AAPB's OAI-PMH endpoint (`americanarchive.org/oai.xml`)
  returns HTTP 500, so the harvester pages `https://americanarchive.org/api.json`
  with Solr **`cursorMark`** (cap-free deep enumeration; `sort=id asc`) and pulls the
  full PBCore per record inline via `fl=id,xml` — one pass, no per-record `.pbcore`
  fetch. **Scope** is set by `aapb.harvest.setlist` (comma-separated `access_types`,
  OR'd into an `fq`); default = the digitized ∪ online ∪ on-location superset
  (~348,689 records). See §4 for the scope options and rationale. (Per-record PBCore
  is also at `…/catalog/{id}.pbcore` and `…/api/{id}.xml`.)
- **DPLA model & serialization:** field types in
  [`DplaMapData.scala`](../../src/main/scala/dpla/ingestion3/model/DplaMapData.scala);
  base field defaults and required/optional validation flags in the
  [`Mapping`](../../src/main/scala/dpla/ingestion3/mappers/utils/Mapping.scala) trait;
  the JSON-L index serializer in
  [`model/package.scala`](../../src/main/scala/dpla/ingestion3/model/package.scala).
  Registered via [`CHProviderProfiles.scala`](../../src/main/scala/dpla/ingestion3/profiles/CHProviderProfiles.scala)
  and [`CHProviderRegistry.scala`](../../src/main/scala/dpla/ingestion3/utils/CHProviderRegistry.scala)
  (registry key `aapb`).

Notes on notation: `\` = direct child. `@x` = attribute. Paths are relative to the
`<pbcoreDescriptionDocument>` root, which the mapper anchors to via `pbcoreRoot`
(handling raw records, OAI `<metadata>` wrappers, and `<pbcoreCollection>` wrappers).

The full PBCore → Dublin Core crosswalk this mapping follows is published at
[pbcore.org/mappings](https://pbcore.org/mappings).

---

## 1. Mapped elements (PBCore source → DPLA field)

### OreAggregation (object-level)

| DPLA field | PBCore source | Logic / notes |
|---|---|---|
| `dplaUri` | *(minted)* | `mintDplaItemUri` — hash of the salted `originalId`. |
| *(originalId — for ID minting & sidecar)* | `pbcoreIdentifier[@source="http://americanarchiveinventory.org"]` | The canonical AACIP id, e.g. `cpb-aacip/513-000000145w`. Fallback: first `pbcoreIdentifier`, then an OAI `header/identifier`. Salted with provider name `aapb`. |
| `provider` | *(constant)* | `EdmAgent("American Archive of Public Broadcasting", uri=http://dp.la/api/contributor/aapb)`. |
| `dataProvider` | `pbcoreAnnotation[@annotationType="organization"]` (top-level) | The contributing/holding organization (e.g. "University of Houston", "GBH", "Vision Maker Media"). Falls back to `"American Archive of Public Broadcasting"` if none. |
| `isShownAt` | *(constructed)* | `https://americanarchive.org/catalog/{id}` where `{id}` is the AACIP id normalized to the **underscore** form (`cpb-aacip_513-000000145w`). The id arrives with `/`, `-`, or `_` after `cpb-aacip`; all are normalized to `_`. |
| `preview` (thumbnail) | *(constructed, gated)* | `https://s3.amazonaws.com/americanarchive.org/thumbnail/{id}.jpg` where `{id}` is the **all-hyphen** form (`cpb-aacip-513-000000145w`). **Only emitted when the record is flagged `Level of User Access = "Online Reading Room"`** — non-online assets resolve to a `*_NOT_AVAIL.png` placeholder, so no preview is emitted for them. Serialized to the API as the field literally named `object`. |
| `edmRights` | `pbcoreRightsSummary\rightsLink` | Standardized rights URI (rightsstatements.org / CC), http only. Absent in the current samples; present on some AAPB records. |
| `rights` | `pbcoreRightsSummary\rightsSummary` | Free-text rights statement. AAPB uses two shapes: a plain sentence (used verbatim, e.g. "In Copyright"), or a WGBH-internal **structured blob** `Rights Note:…,Rights Type:…,Rights Credit:…,Rights Holder:…` (a WGBH convention packed into the free-text element — *not* PBCore; see §4). For the blob, the `Rights Note` field is used as the statement (the raw `Key:Value` string is not surfaced). Empty when a record has no `rightsSummary` (see §3). |
| `rightsHolder` | `pbcoreRightsSummary\rightsSummary` → `Rights Holder` | The `Rights Holder` field parsed out of a structured WGBH rights blob (e.g. "WGBH Educational Foundation"). Empty for plain-text rights. **NB:** captured in the MAP model but, like `genre`, the JSON-L index serializer does not project `rightsHolder` (see §3). |
| `originalRecord` | *(whole record)* | Full PBCore XML, `Utils.formatXml`. |
| `sidecar` | *(minted)* | `prehashId` + `dplaId`. |

### SourceResource (descriptive)

| DPLA field | PBCore source | Logic / notes |
|---|---|---|
| `title` | `pbcoreTitle` (+ `@titleType`) | Prefer a primary title (`@titleType` in `Title`/`Program`/`Preferred Title`). Otherwise combine the `Series` + `Episode` titles as `"Series; Episode"` (dropping `Episode Number` and other qualifiers). Otherwise any title verbatim. |
| `alternateTitle` | `pbcoreTitle[@titleType~="alternate"]` | Any titleType containing "alternate" (case-insensitive). |
| `creator` | `pbcoreCreator\creator` | Name text; **+ `exactMatch`** from `creator/@ref` (http only, e.g. an LC name authority). `creatorRole` is not mapped. |
| `contributor` | `pbcoreContributor\contributor` | Name text; **+ `exactMatch`** from `@ref`. `contributorRole` is not mapped. |
| `publisher` | `pbcorePublisher\publisher` | Name text; **+ `exactMatch`** from `@ref`. |
| `date` | `pbcoreAssetDate` (any `@dateType`) | `cleanDate` strips PBCore "00" padding: `2002-00-00`→`2002`, `1959-10-00`→`1959-10`. → `EdmTimeSpan`. |
| `description` | `pbcoreDescription` (all `@descriptionType`) | Whitespace-collapsed. |
| `subject` | `pbcoreSubject` **+** `pbcoreGenre[AAPB Topical Genre]` | Subjects packed as `;`-delimited text are split into separate concepts; a subject carrying an http `@ref` is kept whole with `@ref` → `exactMatch`. AAPB **Topical** Genre (`@source="AAPB Topical Genre"` / `@annotation="topic"`) is a topic → mapped here, not to `genre`. |
| `genre` | `pbcoreGenre` (**excluding** AAPB Topical Genre) | Format/LCGFT genres → `SkosConcept` **+ `exactMatch`** from `@ref`. **NB:** the index serializer does not currently expose `genre` (see §3/§4). |
| `type` | `pbcoreInstantiation\instantiationMediaType` | e.g. "Moving Image" → enriched to DCMI `moving image`. Distinct. |
| `format` | `pbcoreAssetType` | e.g. "Program", "Episode", "Raw Footage". |
| `extent` | `pbcoreInstantiation\instantiationDuration` | e.g. "0:56:46". Distinct. |
| `language` | `instantiationLanguage` + `essenceTrack\essenceTrackLanguage` | ISO 639 codes / names (e.g. "eng"); enriched. → `SkosConcept`. |
| `place` | `pbcoreCoverage\coverage` where sibling `coverageType="Spatial"` | → `DplaPlace(name)` **+ `exactMatch`** from `coverage/@ref` (http, e.g. a Wikidata URI). |
| `temporal` | `pbcoreCoverage\coverage` where sibling `coverageType="Temporal"` | → `EdmTimeSpan`. |
| `collection` | `pbcoreTitle[@titleType="Series"]` | → `DcmiTypeCollection`. |
| `identifier` | `pbcoreIdentifier` (all `@source`) | All identifier values verbatim (noisy — see §4). |

**Config:** `useProviderName = true`, `getProviderName = "aapb"`.

**On authority URIs:** agent `exactMatch` (`@ref`), subject/place `exactMatch`
**are** populated in the DPLA MAP model. The shared
[index/API serializer](../../src/main/scala/dpla/ingestion3/model/package.scala)
flattens `creator`/`contributor`/`publisher`/`place` to display strings (only
`subject`, `dataProvider`, `provider` expose URIs downstream); captured agent URIs
still feed the Wikimedia/Wikidata entity-linking step, so capturing them is
worthwhile regardless of index exposure.

---

## 2. Source fields we are dropping (present in PBCore, no mapping)

Dropped because there is no DPLA equivalent, or the value is administrative/technical:

- **The bulk of `pbcoreInstantiation`** — `instantiationIdentifier`,
  `instantiationPhysical`, `instantiationDigital` (MIME type), `instantiationStandard`,
  `instantiationLocation` (shelf/holdings, or an internal ARK/URL — **not** a public
  media URL for AAPB; see §4), `instantiationGenerations`, `instantiationFileSize`,
  `instantiationDataRate`, `instantiationColors`, `instantiationTracks`,
  `instantiationChannelConfiguration`, `instantiationDate`, and every
  `instantiationEssenceTrack` technical field (codec, frame size, bit depth, etc.).
  Only `instantiationMediaType` (→ `type`), `instantiationDuration` (→ `extent`), and
  language are used.
- **`creatorRole` / `contributorRole` / `publisherRole`** — DPLA agents carry no role.
- **`pbcoreAssetDate/@dateType`, `@annotation`** — the date-type distinction
  (Broadcast vs. Copyright vs. distributed) is not preserved; the value goes to `date`.
- **`pbcoreAnnotation`** except `@annotationType="organization"` (→ `dataProvider`)
  and `Level of User Access` (gates `preview`). Dropped: `Transcript URL`,
  `Transcript Status`, `Project Code`, `special_collections`, `last_modified`,
  `MAVIS Number`, `Outside URL`, free-text grant credits, etc. *(Some are opportunities — see §4.)*
- **`pbcoreRelation`** (`pbcoreRelationType` + `pbcoreRelationIdentifier`) — related-asset
  pointers (series/version/raw-materials). Not mapped (`relation` left empty).
- **`pbcoreExtension` / `instantiationExtension`** (extensionWrap/Element/Value) —
  source-system extensions (e.g. AACIP nomination status).
- **`pbcorePart`, `pbcoreAudienceLevel`, `pbcoreAudienceRating`** — not present / no equivalent.
- **`pbcoreAssetType/@source`, `pbcoreGenre/@source`, `pbcoreSubject/@subjectType`,
  `pbcoreTitle/@titleType`** authority/type attributes — used for routing (see §1) but
  not themselves retained.
- **`rightsSummary` when it is only an inquiry/contact note** is still mapped to
  `rights` verbatim (e.g. "Inquiries may be submitted to archives@iowapbs.org.") —
  see §4 on rights quality.
- **Structured WGBH rights-blob sub-fields other than `Rights Note` and `Rights
  Holder`** — `Rights Type` (e.g. "All", "Web"), `Rights Coverage`, and `Rights
  Credit` are parsed out but dropped. `Rights Type`/`Coverage` describe internal
  rights-clearance scope (not a public rights status — see §4); `Rights Credit` is
  an attribution that largely duplicates `Rights Holder`.

---

## 3. DPLA fields not currently mapped (opportunities)

### Required fields — status

The hard-required DPLA fields (a record is rejected without them) are:
`dplaUri`, `dataProvider`, `isShownAt`, `title`, `rights`, and a persistent
`originalId`.

- On **live AAPB records** all six are satisfied: `title` from `pbcoreTitle`,
  `rights` from `pbcoreRightsSummary\rightsSummary`, `isShownAt`/`dataProvider`
  constructed as above. ✅
- ⚠️ **`rights` is the fragile one.** The two older *pbcore.org* sample records carry
  **no `pbcoreRightsSummary`**, so they map with an empty `rights` and would be
  **rejected at validation**. Live records sampled from AAPB do carry a
  `rightsSummary`. Coverage should be confirmed across the real feed (see §4).

### Recommended but currently empty / unmapped ⚠️

| DPLA field | Status | Opportunity |
|---|---|---|
| `preview` (thumbnail) | Emitted for ORR records only | Correct per AAPB's access model. For non-ORR records there is no online asset, so no thumbnail is appropriate. No change recommended beyond confirming the ORR gate. |
| `edmRights` | Usually empty | Present only when a record has `rightsSummary\rightsLink`. Encourage AAPB to populate a `rightsstatements.org`/CC URI in `rightsLink` so items get a standardized rights statement. |

> **Media-field note.** DPLA's live media roles are `isShownAt` (landing page),
> `preview` (the thumbnail, serialized to the API as `object`), and `mediaMaster`
> (full-res master for the Wikimedia upload). AAPB streams via its own player
> (`americanarchive.org/media/{id}`) off access-controlled S3 proxies and exposes
> **no IIIF manifests and no public direct-file URL** in the PBCore, so
> `mediaMaster`/`iiifManifest` are intentionally unmapped. Users reach the media via
> `isShownAt`.

### Other unmapped fields with a plausible source

| DPLA field | Opportunity |
|---|---|
| `genre` visibility | `genre` **is** mapped (format/LCGFT genres) but the index serializer does not currently serialize the `genre` field — so those genres are captured in the MAP model but not exposed in the item API. To surface them, either (a) also map format genres to `format`, or (b) add `genre` to the serializer platform-wide. **Decision point.** |
| `relation` | `pbcoreRelation\pbcoreRelationIdentifier` (+ type) could populate `relation` if the related-asset links are considered useful. |

### Unmapped, no obvious source (informational)

`hasView`, `intermediateProvider`, `tags`, `replacedBy`, `replaces` —
no clear equivalent in the AAPB PBCore.

---

## 4. Notes, disclaimers, and recommendations

### Settled decisions

- **`dataProvider`** = the top-level `pbcoreAnnotation[@annotationType="organization"]`
  (the contributing station/producer, e.g. "University of Houston", "GBH"), falling
  back to "American Archive of Public Broadcasting". This is the correct mapping.
- **`collection`** = the PBCore `Series` title. The `special_collections` annotation
  (a slug, e.g. `vision-maker-media`, `net-catalog`) is a site-grouping label, not the
  DPLA collection; the `Series` title is correct.

### Additional assessment (opportunities & risks)

- **Harvest method (implemented).** OAI-PMH is down (HTTP 500), so `AapbHarvester`
  pages the Solr API with **`cursorMark`** — verified live: it advances cleanly
  (no offset cap; `start` deep-paging is risky above Solr's `maxWindowSize`), and
  `fl=id,xml` returns the full PBCore inline (~6.2 KB/record). At `rows=500` the
  superset (~348,689) is ~700 requests / ~2 GB — a single pass in roughly 5–20 min
  with `HttpUtils` retry/backoff. **Not yet run through the full EC2 pipeline** (per
  project rule, real harvests run on EC2, not locally). A negotiated bulk PBCore file
  delivery remains a fine alternative if AAPB prefers.
- **Scope: `access_types` options.** `access_types` is a multi-valued Solr facet;
  every record also carries a literal `all` token (so `q=*` = the whole 2,693,053-doc
  index). The buckets (verified via faceting): `online` **186,716** (streamable +
  thumbnail; ORR), `on-location` **161,959** (viewable only in LOC/GBH reading rooms —
  metadata public, no online media, placeholder thumbnail; mutually exclusive with
  `online`), `digitized` **323,406** (has a digital file; overlaps the other two
  inconsistently — not a clean axis on its own), `private` 3, and **2,344,364
  inventory-only** records with no digital object. Scope choices: (1) `online` only
  (~187k); (2) **the digitized ∪ online ∪ on-location superset (~348,689) — current
  default**; (3) `all` (2.7M) — rejected, mostly object-less stubs. The superset
  includes on-location items whose `isShownAt` is a real page but with no in-browser
  media/thumbnail; the `preview` gate already emits no thumbnail for those. Change the
  scope with `aapb.harvest.setlist`.
- **Possible duplication with Digital Commonwealth.** AAPB content already reaches DPLA
  indirectly as a data provider to the Digital Commonwealth hub. Adding AAPB as a
  standalone feed risks duplicate items — reconcile before production.
- **`rights` quality is uneven.** `rightsSummary` ranges from a real statement
  ("In Copyright") to a contact note ("Inquiries may be submitted to …"). The older
  pbcore.org samples have no rights at all. *Recommendation:* confirm rights coverage
  across the live feed and push for `rightsLink` URIs (`edmRights`).
- **The WGBH structured rights blob is not PBCore, and `Rights Type` is not usable as
  a rights status.** Some records pack a structured record into the free-text
  `rightsSummary`: `Rights Note:…,Rights Type:…,Rights Credit:…,Rights Holder:…`.
  PBCore itself defines **no** rights controlled vocabulary — `rightsSummary` is
  explicitly a free-text element — so `Rights Type` values (`All`, `Web`, or empty in
  our 24-record sample) are **not** PBCore terms; they are a WGBH cataloging convention
  (from WGBH's MediaLog/Open Vault rights records, which AAPB inherited) with no public
  lookup table. They appear to encode the **scope of rights that were cleared / the
  intended distribution** ("All" = all uses/media, "Web" = web/online use), i.e.
  clearance-scope metadata — *not* a copyright **status** — so they cannot be mapped to
  a `rightsstatements.org` / CC `edmRights` URI. We therefore use `Rights Note` for
  `rights` and `Rights Holder` for `rightsHolder`, and drop `Rights Type`/`Coverage`/
  `Credit`. *Open item:* confirm the exact `Rights Type` semantics with AAPB/GBH if we
  ever want to derive access/edmRights from them.
- **`identifier` is noisy.** Every `pbcoreIdentifier` is captured, including internal
  system ids (Sony Ci GUIDs, MARS/NOLA codes, barcodes). *Recommendation:* consider
  keeping only meaningful ids (AACIP id, ARK) and dropping opaque system GUIDs.
- **`type` vs. `format`.** `type` comes from `instantiationMediaType` (DCMI, e.g.
  "moving image"), `format` from `pbcoreAssetType` ("Program"/"Episode"/"Raw Footage").
  Confirm this split reads well on dp.la; `pbcoreAssetType` is arguably more genre than
  format.
- **Subjects mix topics and places.** AAPB `pbcoreSubject` values include place names
  ("Houston, Texas") with no `@subjectType`, so they land in `subject`, not `place`.
  Only clean `pbcoreCoverage[Spatial]` values become `place`. Acceptable, but noted.
- **`genre` and `rightsHolder` are captured but not indexed.** Both are populated in
  the DPLA MAP model but the JSON-L index serializer projects neither (it projects
  `rights` and `edmRights`, but not `rightsHolder`; and not `genre`). They are correct
  to capture (available in the MAP model and to downstream consumers like Wikimedia),
  but will not appear in the item API via this projection without a platform-wide
  serializer change. If genre facets or an explicit rights holder matter on dp.la,
  that is the decision point.
- **Everything here is DRAFT.** The hub is `status = test`; output is not synced to S3
  and cannot reach the index. Field decisions above should be reviewed with AAPB
  (especially the harvest method/scope and rights coverage) before any production
  consideration.

### Open questions for the partner (AAPB)

1. Rights: can `rightsLink` (standardized rights URIs) be populated across the feed?
2. Harvest: is pulling from the public Solr API (`api.json`, cursorMark) acceptable and
   sustainable long-term, or would AAPB prefer to provide a bulk PBCore file / revive OAI?
3. Scope: confirm DPLA should ingest only Online Reading Room items.
4. Duplication: how to reconcile with AAPB content already in DPLA via Digital Commonwealth.
