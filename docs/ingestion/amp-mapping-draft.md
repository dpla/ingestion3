# Arizona Memory Portal (AMP) — Harvest & Mapping Assessment (DRAFT)

**Status:** DRAFT / test hub — not approved for production, not synced to the index.
See [README_TEST_HUBS.md](README_TEST_HUBS.md).

- **Provider (hub):** Arizona Memory Project / Arizona Memory Portal, run by the
  State of Arizona Research Library — Arizona State Library, Archives and Public
  Records (a division of the Secretary of State). Home: https://azmemory.azlibrary.gov/
- **Scale (July 2026):** ~280,000 items ("nearly 280,000"), 612 collections,
  118 contributing agencies/institutions.
- **Registry key:** `arizona` → `ArizonaMapping.scala`, `arizona.status = test`.

---

## 1. Platform

The site runs on **Recollect** (Recollect Limited, New Zealand) — a hosted digital
archives / community-history platform (CakePHP-based). Footer: "RECOLLECT is
Copyright © 2011-2026 by Recollect Limited". AMP migrated to Recollect from CONTENTdm.

Recollect signatures observed:
- Item URLs: `https://azmemory.azlibrary.gov/nodes/view/{id}`
- Browse: `/nodes/browse/{collectionId}`, `/nodes/browse/?meta=<base64>`
- Thumbnails / images: `/assets/pic/{id}`, `/assets/nodeimg/{id}/...`
- Theme leftovers: `/theme/invercargill/...` (Invercargill = Recollect's origin)
- CakePHP `_Token` fields in forms; JSON error envelope `{request_id, success,
  status, error{message,type}}` from the `/api/` router.

---

## 2. Harvest endpoint findings (investigated July 2026)

| Path tried | Result |
|---|---|
| `/oai`, `/oai/`, `/oai-pmh`, `/oai-pmh/`, `/oai2`, `/OAI`, `/harvest` (`?verb=Identify`) | **404** — no OAI-PMH. "oai" is not referenced anywhere in the app JS. |
| `/api/`, `/api/v1/`, `/api/items/{id}`, `/api/assets/{id}`, `/api/search`, `/api/nodes` | **404 JSON** (`NotFoundError`). A CakePHP JSON API *router* exists (the app JS calls `/api/items/` and `/api/assets/`), but no keyless public item route responds — Recollect's API requires an **admin-issued API key**. |
| `/nodes/view/{id}` | 200 HTML — full item page (OpenGraph tags + rendered descriptive metadata). No JSON-LD, no DC `<meta>`. |
| `/nodes/search?keywords=…` | 200 — **server-rendered HTML**, ~8.8 MB for one query (no clean JSON). |
| `/sitemap.xml` | 200 — sitemap index → 6 × `sitemap-nodes-0000N.xml`, **50,000 `/nodes/view/{id}` URLs each (~300k total, matches the ~280k item count)** + `sitemap-pages.xml`. |
| `/nodes/download/{id}`, `/assets/downloadwiz/` | Present but robots-disallowed; UI-driven "download wizard" (per-item / basket export), not a documented bulk feed. |
| `robots.txt` | Disallows `/nodes/download`, `/assets/downloadwiz/`, `/baskets`, admin paths. Advertises the sitemap. No OAI/API hint. |

**Bottom line:** AMP exposes **no OAI-PMH feed and no keyless public API**. The two
technically viable ingest paths are:

### Option A — Recollect API key (preferred if the hub can grant it)
Recollect ships a REST/JSON API (the `/api/` router is live here). With an API key
from Arizona State Library / Recollect, records come back as clean JSON — the same
"api" harvester shape as AAPB. Cleanest, lowest-maintenance path. **Action: ask the
hub contact whether they can provide API access, or a metadata export.**

### Option B — Data export delivered by the hub (file-based)
Like NGA / Dartmouth / HBCULA: the hub delivers a dump (CSV / XML / JSONL) to an
`s3://dpla-hub-*` bucket, harvested with a file harvester. Requires hub effort but
avoids scraping 280k pages.

### Option C — Sitemap-driven scrape harvester (fallback, no hub dependency)
The sitemaps enumerate every item; each item page renders clean, labeled descriptive
metadata (see §3) that is reliably scrapeable from the `.metadata` blocks. This is a
new "api"-style harvester (sitemap → per-item fetch → parse). Heaviest option
(~280k HTTPS fetches, WAF/Cloudflare rate limits — the origin sits behind Cloudflare)
and the most brittle (breaks if the theme markup changes), but needs nothing from AMP.

**Recommendation:** pursue **Option A** (API key) with **Option B** as the fallback we
ask the hub for; keep **Option C** as the no-dependency backstop for a test harvest.

---

## 3. Item metadata available (from `nodes/view/{id}` `.metadata` blocks)

**DOM structure.** Descriptive metadata renders as a run of `<span class="metadata">`
inside `.portlet-content`. The first value of each field carries a
`<span class="titlelabel">Label</span>`; the value is the text of a facet-search
`<a href="…/nodes/search/?meta=<base64>">`. **Repeatable fields** (e.g. a second
Subject) render as further `.metadata` spans with **no** `titlelabel` — they continue
the previous field. Parser rule: start a new field when a span has a `titlelabel`;
otherwise append the value to the current field.

**Sample basis.** 96 leaf items sampled across all six node sitemaps (spread of IDs),
spanning item types Document / Image (Still Image) / Newspaper / Periodical / Yearbook /
Handwritten Document. Saved (normalized JSON) at
[`src/test/resources/arizona/sample-records.json`](../../src/test/resources/arizona/sample-records.json).

### Field inventory (count out of the 96-item sample → DPLA target)

| AMP label (freq) | Example | DPLA field | Notes |
|---|---|---|---|
| *(og:title / page title)* | "Redbud Canyon" | `title` | Reliable; strip " \| Arizona Memory Project". |
| *(Copyright portlet `#creative_commons`)* | `a.extlink` → `…/vocab/NoC-US/1.0/`; else icon code | `edmRights` (+ `rights`) | **URI source resolved** — see §3.1. Not the `Rights Statement` text field. |
| Type (94) | "Text", "Still Image" | `type` | Clean; → DCMI (`text`, `image`). |
| Original Format (94) | "Newspapers", "Black-and-white photographs", "Bills (legislative records)" | `format` | Controlled AAT-style vocab. |
| Digital Format (94) | "PDF", "JPEG", "JPG" | *(drop or `format`)* | Technical; likely drop. |
| Contributing Institution (94) | "State of Arizona Research Library- Arizona State Library, Archives and Public Records" | `dataProvider` | Holding org. Whitespace variants exist ("Library- …" vs "Library-…"). |
| Collection (94) / Subcollection (54) | "Law Collection" | `collection` | Use Collection; Subcollection optional. |
| State (94) / Country (92) / County (64) / City or Town (62) / Place (10) / Geographic Feature (24) / Tribal Homeland (2) | "Arizona" / "United States" / "Pima" / "Tucson" | `place` (spatial) | Combine hierarchy into `DplaPlace`. |
| Date Digitized (90) | "2019-…" | *(drop)* | Administrative. |
| Date Range (90) | "1940s (1940-1949)" | `date` (fallback) | Bucketed decade label; parse to a range. |
| Date Original (48) | "1935", "1977-05-27" | `date` (**preferred**) | Clean; prefer over Date Range when present. |
| Language (82) | "English", "Spanish" | `language` | Enrich to ISO. |
| Subject (82) | "Law--Arizona" (LCSH-style, repeatable) | `subject` | Split compound; keep `--` LCSH strings whole. |
| Publisher (80) | "Arizona Attorney General's Office" | `publisher` | |
| OCLC Number (60) / Call Number (48) / LCCN (28) / Identifier (16) | | `identifier` | Concatenate all as identifiers. |
| Contributor (30) | | `contributor` | |
| Description (20) | | `description` | Sparse. |
| Topic (14) | | `subject` | Additional subject facet. |
| Creator (26) | "Fred O. Wilson" | `creator` | Only ~27% of sample — many items have no creator. |
| Government Document Type / State Agency / Issue / Volume / Opinion Number / … | | *(drop or `description`)* | Collection-specific admin fields. |
| *(og:image)* `/assets/pic/{id}` | | `preview` | Thumbnail; present for most items. |
| *(og:url)* `/nodes/view/{id}` | | `isShownAt` | Stable per-id URL. |
| *(minted from id)* | | `dplaUri`, `originalId`, `sidecar` | Salt originalId with provider `arizona`. |

### 3.1 Rights — `edmRights` URI source (RESOLVED)

The authoritative rights value is **not** the `Rights Statement` metadata field — it is a
dedicated **Copyright portlet**: `<div id="creative_commons" class="portlet">`. Structure:

```html
<div id="creative_commons" class="portlet">
  <div class="portlet-header">…Copyright</div>
  <div class="portlet-content">
    <div class="cc_i_txt">
      <img src="/htmluploads/azmemory/creative_commons/9.png" …/>   <!-- icon = category code -->
      <a href="http://rightsstatements.org/vocab/NoC-US/1.0/" class="extlink">NO COPYRIGHT - UNITED STATES</a>
      <div class="copyDescrip">The organization that has made the Item available believes…</div>
    </div>
  </div>
</div>
```

**Extraction rule for `edmRights`:**
1. In `#creative_commons`, take the first `a[href*="rightsstatements.org"]` or
   `a[href*="creativecommons.org"]` → that **href is `edmRights`** (e.g. the user's
   [node 338958](https://azmemory.azlibrary.gov/nodes/view/338958) → `…/vocab/NoC-US/1.0/`).
2. If there is no such anchor, use the **rights category code** — the integer `N` in the
   icon `…/creative_commons/N.png` (plus the label text) — and crosswalk to a URI (below).
3. Always capture `.copyDescrip` (the free-text statement) → `rights` (dcRights).

**Rights category vocabulary.** The site defines **22 rights categories**, icons
`…/creative_commons/1.png … 22.png`. Codes **1–12 correspond to the 12 standard
rightsstatements.org statements** and render their URI directly in the `extlink` anchor.
Codes **13+ are Arizona-custom categories that carry no URI in the markup**. Observed in
the 48-item sample:

| icon code | category label | URI in markup | → DPLA | sample n |
|---|---|---|---|---|
| 9 | *(NoC-US)* | ✅ `…/vocab/NoC-US/1.0/` | `edmRights` (the URI) | 12 |
| 1 | *(InC)* | ✅ `…/vocab/InC/1.0/` | `edmRights` (the URI) | 4 |
| 12 | *(NKC)* | ✅ `…/vocab/NKC/1.0/` | `edmRights` (the URI) | 1 |
| 13 | IN COPYRIGHT- AZGOVDOC | ❌ | **`rights` (free text) only** | 20 |
| 14 | Arizona Digital Newspaper Program | ❌ | **`rights` (free text) only** | 6 |
| 15 | Archives | ❌ | **`rights` (free text) only** | 5 |

**Policy — no invented crosswalk.** We do **not** map AZ's non-standard categories
(13/14/15/…) onto rightsstatements.org URIs. AMP has already adopted the standardized
statements where it means to (codes 1–12), so a non-standard value is a **deliberate,
semantically meaningful partner choice**, not an omission — inventing a URI for it would
change the meaning. For any category without a published URI, `edmRights` is left empty
and the statement text (`.copyDescrip`) is carried in **`rights`** (dcRights) verbatim.
Adopting a URI for these categories is a **partner decision**, made with the hub, not in
our mapper.

**Sample coverage (48 items):** **17/48 (35%)** carry a partner-published `edmRights`
URI. The remaining 31 (codes 13/14/15) map to free-text `rights` only.

> DPLA requires either a rights URI **or** a rights statement; free-text `rights` satisfies
> the requirement, so these records are not rejected for rights. Quantify the URI-vs-text
> split across the full ~280k at test-harvest time (as with AAPB).

**Notes:**
- The `Rights Statement` metadata field sometimes shows a Cloudflare-obfuscated email
  (rendered `[email protected]` / `<a class="__cf_email__">`) — that is **email
  obfuscation inside a real statement, not the rights value**; ignore it and read rights
  from `#creative_commons` as above.
- Codes 16–22 were not present in the sample; identifying them is informational (they
  either carry a URI → `edmRights`, or don't → free-text `rights`, per the same rule).

### 3.2 Other mapping notes

- **`provider` (hub)** = Arizona Memory Project (constant). **`dataProvider`** =
  Contributing Institution (118 distinct institutions across the site).
- **Collections vs items**: low IDs and many mid-range nodes are Collections /
  Subcollections / Publication Sets (container nodes) — must be **filtered out**;
  harvest only leaf items. (The sample's derived `itemType` has display artifacts like
  "ImageImage"/"NewspaperDocument" from scraping innerText; the clean signal is the
  `Type` field = Text/Still Image plus the absence of "Item Type: Collection".)
- **Dates**: prefer `Date Original`; fall back to `Date Range` (parse "1940s (1940-1949)"
  → 1940/1949). Drop `Date Digitized`.
- **isShownAt / preview**: construct from the node id — no scraping needed.

---

## 4. Implementation status

**Done (first pass):**
- [`ArizonaMapping.scala`](../../src/main/scala/dpla/ingestion3/mappers/providers/experimental/ArizonaMapping.scala)
  — `JsonMapping` over the per-item JSON shape (§3); minting salts with `arizona`.
- [`ArizonaMappingTest.scala`](../../src/test/scala/dpla/ingestion3/mappers/providers/experimental/ArizonaMappingTest.scala)
  — 21 tests, all passing, against
  [`arizona.json`](../../src/test/resources/arizona.json) (real item 256212).
- Registered: `ArizonaProfile` (`JsonProfile`) in `CHProviderProfiles.scala` +
  `"arizona"` in `CHProviderRegistry.scala`. **Interim harvester** = generic
  `DplaJsonlFileHarvester` (only that line changes when the real harvester lands).
- Rights follow the §3.1 policy: `edmRights` = partner-published URI only; everything
  else → free-text `rights`. No invented crosswalk.
- Preview guard: `og:image` that resolves to the AMP site **logo** placeholder
  (`/theme/…/logo…`) is suppressed (item 256212 has no real thumbnail).

**Open mapping items:**
- **HTML entities in text** — titles/values arrive HTML-escaped (e.g.
  `What&#039;s On`). Decode to `What's On` (mapper or an enrichment) before indexing.
- **Date Range parsing** — the mapper sets only `originalSourceDate` (e.g. `"1940s
  (1940-1949)"`); begin/end are derived downstream by the date enrichment
  (`DateBuilder.generateBeginEnd`). Verify it parses the "decade (start-end)" shape well
  at scale.
- **Type/Format** — `Type` (Text/Still Image) passes through to enrichment for DCMI
  normalization; confirm `Original Format` → `format` is right vs. `genre`.
- **`_id` for the interim harvester** — `DplaJsonlFileHarvester` keys on `_id`
  (`arizona--<nodeid>`); the JSONL prep/harvester must emit that field.

**Next steps:**
1. Contact Arizona State Library (STARL, 602-926-3870, www.azlibrary.gov/starl) about:
   (a) a Recollect API key, or (b) a metadata export to an `s3://dpla-hub-*` bucket
   (Option A/B in §2).
2. Expand test coverage across item types (photo/map/newspaper) using
   [`sample-records.json`](../../src/test/resources/arizona/sample-records.json).
3. Once a harvest mechanism is chosen, run a full test harvest + pipeline on EC2
   (per README_TEST_HUBS.md — **no S3 sync**) to validate map/reject rates and the
   rights URI-vs-text split at full scale.
