# Dartmouth Libraries — Draft MODS → DPLA Mapping

**Status:** DRAFT / test hub — not approved for production, not synced to the index.
See [README_TEST_HUBS.md](README_TEST_HUBS.md).

- **Provider (hub):** Dartmouth Libraries (Dartmouth College)
- **Metadata format:** MODS 3.6 (with a Dartmouth `drb:` extension namespace)
- **Mapper:** [`DartmouthMapping.scala`](../../src/main/scala/dpla/ingestion3/mappers/providers/experimental/DartmouthMapping.scala)
- **Tests:** [`DartmouthMappingTest.scala`](../../src/test/scala/dpla/ingestion3/mappers/providers/experimental/DartmouthMappingTest.scala)
- **Basis:** 23 sample records across 5 collections — `black-creative-music` (BCM),
  `granite-state-maps` (NH), `winter-carnival-posters` (dwcposters), `occom`
  (The Occom Circle), `Press_Translations_Japanese`. The first three are
  image/map objects; occom and Press are TEI **text**.
- **Harvest method:** not yet finalized (OAI vs. file delivery). Records are raw
  `<mods:mods>` documents, one per file.
- **DPLA model & serialization:** field types in
  [`DplaMapData.scala`](../../src/main/scala/dpla/ingestion3/model/DplaMapData.scala);
  base field defaults and required/optional validation flags in the
  [`Mapping`](../../src/main/scala/dpla/ingestion3/mappers/utils/Mapping.scala) trait;
  the JSON-L index serializer in
  [`model/package.scala`](../../src/main/scala/dpla/ingestion3/model/package.scala).
  Registered via [`CHProviderProfiles.scala`](../../src/main/scala/dpla/ingestion3/profiles/CHProviderProfiles.scala)
  and [`CHProviderRegistry.scala`](../../src/main/scala/dpla/ingestion3/utils/CHProviderRegistry.scala).

Notes on notation: `\` = direct child, `\\` = descendant. `@x` = attribute. The
mapper anchors to the record's root MODS element via `getModsRoot`, so paths below
are relative to `<mods:mods>`.

---

## 1. Mapped elements (MODS source → DPLA field)

### OreAggregation (object-level)

| DPLA field | MODS source | Logic / notes |
|---|---|---|
| `dplaUri` | *(minted)* | `mintDplaItemUri` — hash of the salted `originalId`. |
| *(originalId — for ID minting & sidecar)* | `recordInfo/recordIdentifier[@source="DRB"]` | Fallback: any `recordIdentifier`, then an OAI `header/identifier`. Salted with provider name `dartmouth`. |
| `provider` | *(constant)* | `EdmAgent("Dartmouth Libraries", uri=http://dp.la/api/contributor/dartmouth)`. |
| `dataProvider` | *(constant)* | **Per Dartmouth (2026):** hardcoded `nameOnlyAgent("Dartmouth Libraries")` — same as `provider`. (Previously derived from the analog original's `subLocation`; dropped at Dartmouth's request.) |
| `isShownAt` | `location/url[@usage="primary"][@access="object in context"]` | Landing page. Absent on the map records in the sample set. |
| `iiifManifest` | *(constructed)* | `https://collections.dartmouth.edu/archive/iiif/{collection}/{item}-mods.json` where `{collection}` = host `relatedItem` DRB `recordIdentifier`, `{item}` = own DRB `recordIdentifier`. **Skipped when `typeOfResource = "text"`** (occom/Press have no manifest — verified 200 for image/map, 404 for text). |
| `preview` | `location/url[@access="preview"]` | **No such URL in any sample → currently empty.** See §4. |
| `edmRights` | `accessCondition[@type="use and reproduction"]/@xlink:href` | Standardized rights URI (e.g. a CC license). Present on BCM; absent on occom. |
| `rights` | `accessCondition` (**direct text only**) | Direct text of each `accessCondition`; a condition whose content is a nested `cmd:copyright` block contributes no rights text (its holder → `rightsHolder`). |
| `rightsHolder` | `accessCondition/cmd:copyright/cmd:rights.holder/cmd:name` | → `EdmAgent` (e.g. "Trustees of Dartmouth College"). |
| `originalRecord` | *(whole record)* | Full MODS XML, `Utils.formatXml`. |
| `sidecar` | *(minted)* | `prehashId` + `dplaId`. |

### SourceResource (descriptive)

| DPLA field | MODS source | Logic / notes |
|---|---|---|
| `title` | `titleInfo` (not `@type` alternative/translated/uniform) | `nonSort` + `title` + `subTitle`, whitespace-collapsed. |
| `alternateTitle` | `titleInfo[@type="alternative"|"translated"|"uniform"]/title` | |
| `creator` | `name[@usage="primary"]` (any type/role) | **Per Dartmouth (2026):** all names with `usage="primary"`, and **only** those — records with no primary name have no creator. Name from `namePart` (`family, given [, date]`, or plain). **+ `exactMatch`** from `@valueURI` (http only) **+ `scheme`** from `@authorityURI`. |
| `contributor` | *(not mapped)* | Intentionally unmapped — the treatment of non-primary names is an open question for Dartmouth (see §4). |
| `publisher` | `originInfo/publisher` | Name only. |
| `date` | `originInfo/dateCreated` or `relatedItem[@type="otherFormat"]/originInfo/dateCreated` (`@encoding="w3cdtf"`) | Prefers `dateCreated` (the analog original's date); falls back to `dateIssued`/`dateOther`/`copyrightDate`. → `EdmTimeSpan(displayDate)`. |
| `temporal` | `subject/temporal` | → `EdmTimeSpan`. |
| `subject` | `subject/{topic,temporal,titleInfo,name,genre}` | → `SkosConcept(providedLabel)` **+ `exactMatch`/`scheme`** from the child's `@valueURI`/`@authorityURI`. |
| `description` | `abstract` (direct child; excludes `@shareable="no"`) | **Abstract only.** `note` values excluded; `abstract[@shareable="no"]` (e.g. "Part 1 of 4") excluded as non-descriptive. |
| `extent` | `physicalDescription/extent` | |
| `format` | `genre` | With the digital-surrogate / format-type block filters. **`@valueURI` (Getty AAT) is dropped** — `format` is a plain string (see §3/§4). |
| `type` | `typeOfResource` | Direct child (record's own). |
| `language` | `language/languageTerm[@type="text"]` | → `SkosConcept(name)`. |
| `place` | `originInfo/place/placeTerm[@type="text"]` and `subject/geographic` | Origin place → name only. `subject/geographic` → `DplaPlace(name)` **+ `exactMatch`** when the subject/geographic `@valueURI` is http (FAST `(OCoLC)fst…` codes are ignored). |
| `collection` | `relatedItem[@type="host"]/titleInfo/title` | → `DcmiTypeCollection(title)`. |
| `identifier` | `identifier` (all `@type`s) | DOI, `ms-number`, `ark`, `uri`, `panopto`, … all captured verbatim as strings (see §4 — noisy). |

**Config:** `useProviderName = true`, `getProviderName = "dartmouth"`.

**On authority URIs:** creator `exactMatch`/`scheme` and place/subject
`exactMatch` **are** populated in the DPLA MAP model. Note the shared
[index/API serializer](../../src/main/scala/dpla/ingestion3/model/package.scala)
flattens `creator`/`publisher`/`place` to display strings
(only `subject` exposes its URI downstream); the creator URIs are still consumed by
the Wikimedia/Wikidata entity-linking step. Capturing them is correct regardless of
current index exposure.

---

## 2. Source fields we are dropping (present in the MODS, no mapping)

Dropped because there is no DPLA equivalent, or the value is administrative/technical:

- **`genre/@valueURI` (Getty AAT URIs)** — `format` is a plain string with no URI slot. *(But see §3: the DPLA `genre` field could hold these.)*
- **`mods:note` (all)** — TEI-conversion, Handwriting, Paper, Ink, Noteworthy, "additional physical form". Deliberately excluded from `description`.
- **`abstract[@shareable="no"]`** — e.g. "Part 1 of 4"; excluded from `description` as non-descriptive.
- **Names without `@usage="primary"`** (e.g. the addressee "Wheelock", the un-marked BCM performers, the "repository" name) — currently dropped, since only `usage="primary"` names map to `creator` and `contributor` is unmapped. *(Open question — see §4.)*
- **`name/nameIdentifier`** (local person IDs, e.g. `pers0007.ocp`), **`name/@authority`** code (`naf`), **`role/roleTerm`** — roles are no longer used (creator is selected by `@usage="primary"`); relator label/URI not retained.
- **`originInfo/edition`** (e.g. "The Occom Circle: A digital edition").
- **`originInfo/place/placeTerm[@type="code"]`** (marccountry `nhu`), **`originInfo/@eventType`**.
- **`physicalDescription/form`, `internetMediaType`, `digitalOrigin`, `note[@type="technique"]`** (only `extent` is kept).
- **`extension/drb:filename`** (master TIFF/WAV filenames) and **`drb:flag`** — no file-server/derivative URL exists in the metadata; media is reached via `iiifManifest` instead.
- **`recordInfo/*`** except `recordIdentifier` — `descriptionStandard`, `recordContentSource`, `recordCreationDate`, `recordChangeDate`, `recordInfoNote`, `recordOrigin`, `languageOfCataloging`.
- **`relatedItem[@type="otherFormat"]`** — everything except `originInfo/dateCreated` (→ `date`); e.g. `holdingSimple/subLocation` and `shelfLocator` (subLocation was previously used for `dataProvider`, now hardcoded), and its `titleInfo`.
- **`relatedItem[@type="host"]`** — everything except `titleInfo/title` (→ `collection`) and `recordInfo/recordIdentifier` (→ `iiifManifest`); e.g. its `location/url`, `typeOfResource[@collection]`.
- **`accessCondition` nested `cmd:copyright`** (copyrightMD) — `cmd:rights.holder/cmd:name` → `rightsHolder`; the other attributes (`copyright.status`, `publication.status`) are dropped.
- **`subject/cartographics/coordinates`, `subject/hierarchicalGeographic`** — not mapped (relevant if the map collection carries coordinates).
- **`subject/@authority` and FAST `valueURI` codes** in `(OCoLC)fst…` form — ignored as non-URIs.
- **`typeOfResource` attributes** (`@manuscript`, `@collection`), **`titleInfo/@supplied`**.

---

## 3. DPLA fields not currently mapped (opportunities)

### Required fields — all satisfied ✅

The hard-required DPLA fields (a record is rejected without them) are all mapped:
`dplaUri`, `dataProvider`, `isShownAt`, `title`, `rights`, and a persistent
`originalId`. **No required field is missing.**

### Recommended but currently empty / unmapped ⚠️

| DPLA field | Status | Opportunity |
|---|---|---|
| `preview` (thumbnail) | **Empty** (warn-only, but important for display on dp.la) | The thumbnail. Serialized to the API as the `object` field (see media-field note). Derive from the IIIF manifest (a IIIF Image API thumbnail) or ask Dartmouth to expose a thumbnail URL / `location/url[@access="preview"]`. **The one real media gap.** |

> **Media-field note.** DPLA's media fields are easy to confuse. There are three
> live roles: `isShownAt` (the provider landing page), **`preview`** (the thumbnail),
> and **`mediaMaster`** (the full-resolution master file(s), consumed by the Wikimedia
> upload). In the JSON-L/index projection the API field literally named `object` is
> populated from the model's `preview` — i.e. **`object` = thumbnail** (a DPLA MAP v3
> legacy). The model's own `object` field ("full size image") is a vestigial artifact
> that is **not serialized** — do not map it. See
> [`model/package.scala`](../../src/main/scala/dpla/ingestion3/model/package.scala)
> (~lines 150-156).

### Other unmapped fields with a plausible source

| DPLA field | Opportunity |
|---|---|
| `genre` (`SkosConcept`, **supports `exactMatch`/`scheme`**) | Map `mods:genre` here (in addition to `format`) to **preserve the Getty AAT URIs** currently dropped. |
| `mediaMaster` | Full-resolution master file(s) for the Wikimedia upload. Not mapped by design — `iiifManifest` already gives the pipeline both full images and thumbnails, so it is redundant here. Revisit only if a direct full-res asset URL is needed outside IIIF. |

### Unmapped, no obvious source (informational)

`hasView`, `intermediateProvider`, `tags`, `relation`, `replacedBy`, `replaces` —
no clear equivalent in the Dartmouth MODS.

---

## 4. Notes, disclaimers, and recommendations

### Carried over from code TODOs

- **`preview`/thumbnail source unknown.** No thumbnail URL in the samples.
  *Recommendation:* confirm the IIIF Image API base (or a `location/url` thumbnail)
  so `preview` (the thumbnail, exposed as the API `object` field) can be populated.
- **`iiifManifest` URL is a hardcoded template** (`collections.dartmouth.edu/archive/iiif/…`).
  Ideally Dartmouth would emit the manifest URL explicitly in the MODS (e.g.
  `location/url[@note="iiifManifest"]`), as several DPLA hubs do.
- **`date` precedence is heuristic.** We prefer `dateCreated` (analog original) over
  `dateIssued` (digitization). If a record has only `dateIssued`, the displayed date
  will be the digitization year (e.g. 2025), which is usually not desired.

### Additional assessment (opportunities & risks)

- **Contributor / non-primary names (OPEN QUESTION for Dartmouth).** Per Dartmouth,
  `creator` now maps only `name[@usage="primary"]`, and `contributor` is not mapped.
  This means every non-primary name — an addressee, a performer, the "repository"
  name, etc. — is currently **dropped**. Dartmouth should decide whether any of those
  names should be preserved (e.g. as `contributor`), and if so, by what rule (all
  non-primary names, or only those with certain roles). Until then, only primary
  names appear, and some records have no agent at all.
- **Text vs. image collections.** occom and Press are TEI **text** — no IIIF
  manifest, no thumbnail, no `object`. These records would be thin (text metadata +
  a landing page) and, for occom, `isShownAt` is a generic project homepage
  (`www.dartmouth.edu/~occom/`), not the item. *Decision point:* whether/how to
  ingest text-only collections, and whether occom's `isShownAt` is item-specific
  enough.
- **`edmRights` coverage is uneven.** BCM carries a CC URI; occom has only free-text
  rights (no standardized URI). *Recommendation:* ask Dartmouth to supply
  `rightsstatements.org`/CC URIs across collections.
- **`identifier` is noisy.** We capture every `mods:identifier` type verbatim,
  including `panopto` GUIDs and `uri[@invalid="yes"]` legacy CONTENTdm links.
  *Recommendation:* filter or type-label (e.g. keep DOI/ARK, drop `invalid="yes"`
  and internal GUIDs).
- **FAST authority IDs are dropped.** Subject/place `valueURI`s are FAST
  `(OCoLC)fst…` codes, not http URIs. *Opportunity:* convert to
  `http://id.worldcat.org/fast/{n}` to capture subject/place `exactMatch`.
- **Getty AAT genre URIs are dropped** because `format` is a string. *Opportunity:*
  also map `genre` → the DPLA `genre` `SkosConcept` field to preserve them.
- **Authority URIs for agents are captured but not exposed in the index.** The
  creator/contributor `exactMatch` (LC name authorities) feeds Wikimedia entity
  linking but does not appear in the DPLA item API — surfacing it there would be a
  platform-wide serializer change, out of scope for this hub.
- **Two-file object pattern.** Some objects arrive as a pair (`…-mods.xml` and
  `…-images-mods.xml`) representing distinct records (e.g. an audio object and its
  associated images). Confirm with Dartmouth whether both should ingest as separate
  DPLA items or be consolidated.
- **Everything here is DRAFT.** The hub is `status = test`; output is not synced to
  S3 and cannot reach the index. Field decisions above should be reviewed with
  Dartmouth (especially `dataProvider`, `preview`, rights URIs, and the two-file
  pattern) before any production consideration.
