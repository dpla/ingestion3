# Dartmouth Libraries — Draft MODS → DPLA Mapping

**Status:** DRAFT / test hub — not approved for production, not synced to the index.
See [README_TEST_HUBS.md](README_TEST_HUBS.md).

- **Provider (hub):** Dartmouth Libraries (Dartmouth College)
- **Metadata format:** MODS, harvested from the **live OAI-PMH feed**.
- **Harvest:** `localoai` from `https://collections.dartmouth.edu/archive/oai`,
  `metadataPrefix=mods`, `verb=ListRecords`, paging via `resumptionToken`
  (`deletedRecord=no`, granularity to the second). Config in `ingestion3-conf/i3.conf`.
- **Mapper:** [`DartmouthMapping.scala`](../../src/main/scala/dpla/ingestion3/mappers/providers/experimental/DartmouthMapping.scala)
- **Tests:** [`DartmouthMappingTest.scala`](../../src/test/scala/dpla/ingestion3/mappers/providers/experimental/DartmouthMappingTest.scala)
  (fixtures are real OAI records from the live feed).
- **Collections (as of 2026-09-30):** the feed advertises 59 `ddlp-collections:*`
  sets. Of the 5 originally sampled, **3 currently emit MODS** and are in the
  harvest setlist: `black-creative-music`, `granite-state-maps`,
  `winter-carnival-posters`. The two TEI-text sets (`occom`,
  `Press_Translations_Japanese`) are advertised but return `noRecordsMatch` / 0
  records — see §4. The other 54 sets are out of scope for this mapping.
- **DPLA model & serialization:** field types in
  [`DplaMapData.scala`](../../src/main/scala/dpla/ingestion3/model/DplaMapData.scala);
  base field defaults / validation flags in the
  [`Mapping`](../../src/main/scala/dpla/ingestion3/mappers/utils/Mapping.scala) trait;
  the JSON-L index serializer in
  [`model/package.scala`](../../src/main/scala/dpla/ingestion3/model/package.scala).
  Registered via [`CHProviderProfiles.scala`](../../src/main/scala/dpla/ingestion3/profiles/CHProviderProfiles.scala)
  and [`CHProviderRegistry.scala`](../../src/main/scala/dpla/ingestion3/utils/CHProviderRegistry.scala).

Notation: `\` = direct child, `@x` = attribute. `getModsRoot` anchors to the
record's root `<mods>` (OAI-wrapped `<metadata><mods>` or a raw `<mods>`), so paths
below are relative to `<mods>`.

---

## 1. Mapped elements (MODS source → DPLA field)

### OreAggregation (object-level)

| DPLA field | MODS source | Logic / notes |
|---|---|---|
| `dplaUri` | *(minted)* | `mintDplaItemUri` — hash of the salted `originalId`. |
| *(originalId — ID minting & sidecar)* | `recordInfo/recordIdentifier[@source="DRB"]` | Fallback: OAI `header/identifier` (`oai:ddlp-id:<coll>/<item>`), then any `recordIdentifier`. Salted with `dartmouth`. |
| `provider` | *(constant)* | `EdmAgent("Dartmouth Libraries", uri=http://dp.la/api/contributor/dartmouth)`. |
| `dataProvider` | *(constant)* | Hardcoded `nameOnlyAgent("Dartmouth Libraries")`. Dartmouth may supply a per-record source later (see §4 dataProvider proposal). |
| `isShownAt` | `identifier[@type="doi"]` → `identifier[@type="ark"]` → `location/url[@usage="primary"][@access="object in context"]` | **Precedence** (first non-empty wins). Bare `doi:`/`ark:` normalized to resolvable URLs. All other identifiers (incl. `@type="uri" invalid="yes"`) are **not** used. |
| `iiifManifest` | `location/url[@note="IIIF manifest"]` | Literal attribute value `IIIF manifest` (with a space), as the feed emits it. Resolved if relative. |
| `preview` | `location/url[@access="preview"]` | The thumbnail (→ API `object`; see media-field note). **Relative paths are resolved** against `https://collections.dartmouth.edu` by `resolveUrl` (removable once Dartmouth makes them absolute). |
| `edmRights` | `accessCondition[@type="use and reproduction"]/@xlink:href` | Standardized rights URI — `rightsstatements.org` or `creativecommons.org`. |
| `rights` | `accessCondition` (direct text only) | Direct text of each `accessCondition`; a nested `cmd:copyright` block contributes no text (its holder → `rightsHolder`). |
| `rightsHolder` | `accessCondition/cmd:copyright/cmd:rights.holder/cmd:name` | → `EdmAgent` (e.g. "Trustees of Dartmouth College"). |
| `originalRecord` | *(whole record)* | Full MODS XML, `Utils.formatXml`. |
| `sidecar` | *(minted)* | `prehashId` + `dplaId`. |

### SourceResource (descriptive)

| DPLA field | MODS source | Logic / notes |
|---|---|---|
| `title` | `titleInfo` (not alternate) | `nonSort` + `title` + `subTitle`, whitespace-collapsed. |
| `alternateTitle` | `titleInfo[@type="alternative"|"translated"|"uniform"]/title` | |
| `creator` | `name[@usage="primary"]` (any type/role) | All primary names, and only those; records with no primary name have no creator. **+ `exactMatch`** from `@valueURI` (http) **+ `scheme`** from `@authorityURI`. |
| `contributor` | all non-primary `name`s **except the repository role** | Repository = `roleTerm` `repository` (text) or `rps` (MARC code), case-insensitive. |
| `publisher` | `originInfo/publisher` | Name only. |
| `date` | `relatedItem[@type="original"|"otherFormat"]/originInfo/{dateCreated,dateIssued}@w3cdtf`, else top-level `originInfo` date | Prefers the original/analog date over the top-level **digitization** date. → `EdmTimeSpan(displayDate)`. |
| `temporal` | `subject/temporal` | → `EdmTimeSpan`. |
| `subject` | `subject/{topic,temporal,titleInfo,name,genre}` | → `SkosConcept` + `exactMatch` (http or FAST-converted) + `scheme`. |
| `genre` | `genre` | → `SkosConcept` + `exactMatch` from `@valueURI` (http as-is, bare FAST codes converted to `id.worldcat.org/fast`); deduped by label. |
| `description` | `abstract` (direct child; excludes `@shareable="no"`) | Abstract only; `note` values excluded. |
| `extent` | `physicalDescription/extent` | |
| `type` | `typeOfResource` | Direct child. |
| `language` | `language/languageTerm[@type="text"]` | → `SkosConcept(name)`. |
| `place` | `originInfo/place/placeTerm[@type="text"]`; `subject/geographic`; `subject/cartographics/coordinates`; `subject/hierarchicalGeographic` | geographic → `DplaPlace(name)` + FAST/http `exactMatch`; coordinates → `DplaPlace(coordinates)` **as-is** (MARC-255 string — see §4); hierarchicalGeographic → structured `DplaPlace`. |
| `collection` | `relatedItem[@type="host"]/titleInfo/title` | → `DcmiTypeCollection(title)`. |

**Config:** `useProviderName = true`, `getProviderName = "dartmouth"`.
**Enrichment** is generic (no per-hub code).

**On authority URIs:** agent `exactMatch`/`scheme` and subject/place/genre
`exactMatch` are populated in the DPLA MAP model. The shared
[index/API serializer](../../src/main/scala/dpla/ingestion3/model/package.scala)
flattens `creator`/`contributor`/`publisher`/`place` to display strings (only
`subject` exposes its URI in the index); agent `exactMatch` still feeds the
Wikimedia/Wikidata entity-linking step, so capturing it is correct regardless of
index exposure.

---

## 2. Source fields dropped (present in the MODS, not mapped)

- **`identifier` other than doi/ark** — incl. `@type="uri" invalid="yes"` (legacy
  CONTENTdm), `panopto`, `ms-number`. Per Dartmouth, only doi/ark are used (for
  `isShownAt`); the rest are dropped. There is no DPLA `identifier` mapping now.
- **`mods:note` (all)** — content and administrative notes alike; excluded from `description`.
- **`abstract[@shareable="no"]`** — e.g. "Part 1 of 4".
- **`name/nameIdentifier`** (local person IDs), **`name/@authority`** code.
- **`originInfo/edition`, `originInfo/@eventType`, `originInfo/place/placeTerm[@type="code"]`** (marccountry).
- **`physicalDescription/form`, `internetMediaType`, `digitalOrigin`, `note[@type="technique"]`** (only `extent` kept). MODS `genre` now feeds DPLA `genre` (not `format`); **`format` is currently unmapped** — no clean MODS source (physicalDescription/form is RDA media/carrier noise).
- **`extension/drb:filename`** (master TIFF/WAV) and **`drb:flag`** — media is reached via `iiifManifest`.
- **`recordInfo/*`** except `recordIdentifier`.
- **`relatedItem`** — except `[@type="host"]/titleInfo/title` (→ `collection`) and `[@type="original"|"otherFormat"]/originInfo` dates (→ `date`).
- **`accessCondition` copyrightMD** beyond `rights.holder/name` (status attributes dropped).
- **`subject/@authority`, `typeOfResource` attributes, `titleInfo/@supplied`.**

---

## 3. DPLA fields not mapped (opportunities)

### Required fields — all satisfied ✅
`dplaUri`, `dataProvider`, `isShownAt`, `title`, `rights`, `originalId` — all mapped.

### Unmapped

| DPLA field | Note |
|---|---|
| `format` | No clean MODS source now that `genre` → DPLA `genre`. Could revisit `physicalDescription` if desired, but it is digital-surrogate noise. |
| `mediaMaster` | Full-res master for the Wikimedia upload. Not mapped — `iiifManifest` already gives that pipeline full images + thumbnails. |
| `hasView`, `intermediateProvider`, `tags`, `relation`, `replacedBy`, `replaces` | No equivalent in the Dartmouth MODS. |

> **Media-field note.** Three live media roles: `isShownAt` (landing page),
> `preview` (thumbnail), `mediaMaster` (full-res master, Wikimedia). In the index
> projection the API field named `object` is populated from the model's `preview`
> (DPLA MAP v3 legacy). The model's own `object` field is vestigial and not
> serialized — do not map it.

---

## 4. Resolution log and open items

### Resolved — Shaun Akhtar email, 2026-09-30 (applied 2026-09-30)

- **IIIF manifest** — now `location/url[@note="IIIF manifest"]` (hardcoded template removed). ✅
- **Preview / thumbnail** — mapped from `location/url[@access="preview"]`; relative paths resolved against the base URL. ✅
- **isShownAt** — doi → ark → primary-url precedence; all other identifiers dropped. ✅
- **Original date** — `relatedItem[@type="original"|"otherFormat"]/originInfo` date preferred over the top-level digitization date. ✅ (e.g. granite `NH_1638_001` now 1638, was 2015.)
- **Non-primary names** — now mapped to `contributor`, excluding the repository role. ✅
- **genre** — mapped to DPLA `genre` with `@valueURI`/FAST `exactMatch`. ✅
- **FAST codes** — bare `(OCoLC)fst…` converted to `id.worldcat.org/fast/{n}` on subjects, places, and genre. ✅
- **Geographic** — `cartographics/coordinates` and `hierarchicalGeographic` now mapped. ✅
- **Two-record objects** (Black Creative Music pairs) — treated as **separate DPLA items**; no consolidation logic exists or was added. Decision 2026-09-30. ✅
- **dataProvider** — remains hardcoded "Dartmouth Libraries"; per-record source proposal pending (QA/result file).

### Flags for Dartmouth (evidence in the QA/result file)

- **Repository name differs from dataProvider.** The `role="repository"` name on
  live records is **"Dartmouth Digital Library Program"** (maps) / **"Digital by
  Dartmouth Library"** (posters, BCM) — not "Dartmouth Libraries". It is still
  excluded from `contributor` (duplicative of provider), but the mismatch is noted
  for Dartmouth to reconcile with the intended dataProvider value.
- **Coordinates format (MAP 3.1 question).** Dartmouth emits MARC-255 strings, e.g.
  `(W 73°--W 70°/N 45°15ʹ--N 42°30ʹ).`, **not** decimal `lat,long`. Mapped as-is.
  Whether MAP 3.1 geographic requirements (decimal coordinates) still apply is
  Dominic's answer to give; evidence is in the result file.
- **Relative preview paths.** Dartmouth is making these absolute; `resolveUrl` is a
  temporary shim to delete once that lands.
- **FAST / rights URIs still rolling out.** The mapper accepts both bare FAST codes
  (converting them) and http URIs, and both `rightsstatements.org` and CC rights
  URIs; per-collection counts are in the result file.

### Still open (Dominic decides)

- **Text collections** (`occom`, `Press_Translations_Japanese`) emit no MODS yet
  (`noRecordsMatch` / 0 records). Whether DPLA ingests the text collections at all
  remains open.
- **Harvest scope** — the test harvest covers the 3 live mapped sets; the other 54
  advertised sets are not in the contributed set / not designed against.
- **doi vs ark precedence** — implemented doi-first; no live record currently carries
  both, so the tiebreak is untested against real data.
