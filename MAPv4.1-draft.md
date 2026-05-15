# Metadata Application Profile, version 4.1

**DRAFT — [Date TBD]**

**Document URI:** http://dp.la/info/map

*This draft was prepared by DPLA staff and has not been reviewed by a working group.*

---

## Working Group Review Required

This draft documents the current implementation state of the DPLA ingest codebase ([dpla/ingestion3](https://github.com/dpla/ingestion3)). It is not a proposal — it is a record of what the code actually does. The following items require working group attention before this document is finalized:

1. **`dpla:iiifManifest` on `ore:Aggregation`** — This field exists in the current implementation but appears in neither MAP v4.0 nor v5.0. MAP v5.0 proposed placing IIIF references on `edm:WebResource` via `dcterms:isReferencedBy` and `svcs:has_Service`. The working group should determine whether to formalize the current top-level approach, align with v5.0's WebResource placement, or support both. Formal property URI, definition, and obligation are needed.

2. **`dpla:mediaMaster` on `ore:Aggregation`** — Present in the codebase; absent from both MAP versions. Formal property URI, definition, and obligation are needed.

3. **`dpla:tags` on `ore:Aggregation`** — Present in the codebase; absent from both MAP versions. Formal property URI, definition, and obligation are needed.

4. **`edm:Place` — `coordinates` field** — This field (`wgs84_pos:lat_long`) was deprecated in v4.0 and was not restored in v5.0, which uses separate `lat`, `long`, and `alt` fields instead. The current codebase retains it. The working group should decide: formalize it, replace it with separate coordinate fields, or both.

5. **`edm:hasType` label and path (Genre vs. Subtype)** — The codebase uses the v4.0 name "Genre" and path `.sourceResource.genre`. MAP v5.0 renamed this property "Subtype" and introduced a preferred terms list (AAT). This draft retains the v4.0 naming. The working group should decide whether to adopt the v5.0 rename and associated controlled vocabulary.

6. **`dc:rights` retention** — MAP v5.0 deprecated `dc:rights` from `dpla:SourceResource` and `edm:WebResource`. The current codebase retains it as required on SourceResource and present on WebResource. This draft preserves the v4.0 behavior. The working group should decide whether and when to deprecate.

---

## Scope

This document is the technical specification of the DPLA Metadata Application Profile, version 4.1. It documents the current state of DPLA's metadata implementation as reflected in the ingestion codebase. It does not represent a policy proposal; changes from v4.0 are described in Appendix C.

Other documentation available:

- **An Introduction to the DPLA Metadata Application Profile** — https://dp.la/info/developers/map/  
  A general introduction to DPLA's metadata standards, workflows, and processes.

- **DPLA Metadata Quality Guidelines** — http://bit.ly/dpla-metadata-qual  
  Best practices for creating shareable metadata for DPLA aggregation.

- **DPLA Standardized Rights Statements Implementation Guidelines** — http://bit.ly/dpla-rights-guidelines  
  Description of DPLA's implementation of standardized rights statements.

---

## Namespaces Referenced

| Prefix | URI |
|---|---|
| dpla: | http://dp.la/about/map/ |
| cnt: | http://www.w3.org/2011/content# |
| dc: | http://purl.org/dc/elements/1.1/ |
| dcterms: | http://purl.org/dc/terms/ |
| dcmitype: | http://purl.org/dc/dcmitype/ |
| edm: | http://www.europeana.eu/schemas/edm/ |
| gn: | http://www.geonames.org/ontology# |
| oa: | http://www.w3.org/ns/oa# |
| ore: | http://www.openarchives.org/ore/terms/ |
| rdf: | http://www.w3.org/1999/02/22-rdf-syntax-ns# |
| rdfs: | http://www.w3.org/2000/01/rdf-schema# |
| skos: | http://www.w3.org/2004/02/skos/core# |
| wgs84: | http://www.w3.org/2003/01/geo/wgs84_pos# |

---

## How to Use This Guide

Each class of the DPLA MAP is provided as a table below. Columns indicate:

- **Label** — a human-readable label for the property
- **Property** — the RDF value of the metadata element, followed by its JSON-LD path
- **Sub-property of** — a parent RDF value that the property belongs to
- **Range** — the data type of the value
- **Usage** — description of how the property should be used
- **Data Type** — whether the value is a string `Literal` or a `Reference` (URI)
- **Vocab/Syntax Schema** — a controlled vocabulary or syntax for the value
- **Obligation** — the required number of values

Notes:

1. Labels in bold display in the DPLA portal. Labels with asterisks (*) are recommended. See Appendix B for required fields.
2. Properties with a maximum value of "1" allow one instance per class. Properties with maximum "n" are unlimited.
3. Items marked **⚑ WG Review** require working group attention before this draft is finalized.

---

## 4.0 DPLA MAP Classes

### 4.1 Core Classes

The core classes are required for all DPLA objects.

---

### 4.1.A class = dpla:SourceResource

This class is a subclass of `edm:ProvidedCHO`, which comprises the described resources (cultural heritage objects) about which DPLA collects descriptions. Attributes of the described resource are located here, not attributes of digital representations.

| Label | Property | Sub-property of | Range | Usage | Data Type | Vocab/Syntax Schema | Obligation |
|---|---|---|---|---|---|---|---|
| Alternate Title | dcterms:alternative `.sourceResource.alternative` | dc:title | | Any alternative title of the described resource, including abbreviations and translations. | Literal | | 0 – n |
| Collection* | dcterms:isPartOf `.sourceResource.collection` | dc:relation | dcmitype:Collection | Collection or aggregation of which described resource is a part. | Reference | | 0 – n |
| Contributor | dcterms:contributor `.sourceResource.contributor` | | edm:Agent | Entity responsible for making contributions to described resource. | Reference | | 0 – n |
| Creator* | dcterms:creator `.sourceResource.creator` | | edm:Agent | Entity primarily responsible for making described resource. | Reference | | 0 – n |
| Date* | dc:date `.sourceResource.date` | | edm:TimeSpan | Date value as supplied by Data Provider. | Reference | | 0 – n |
| Description | dcterms:description `.sourceResource.description` | | | Includes but is not limited to: an abstract, a table of contents, or a free-text account of described resource. | Literal | | 0 – n |
| Extent | dcterms:extent `.sourceResource.extent` | | | Size or duration of described resource. | Literal | | 0 – n |
| Format* | dc:format `.sourceResource.format` | | | Physical medium or dimensions of described resource. | Literal | | 0 – n |
| **Genre*** ⚑ WG Review | edm:hasType `.sourceResource.genre` | edm:isRelatedTo | skos:Concept | Captures categories of described resource in a given field. Does not capture aboutness. See note on v5.0 rename to "Subtype" in Appendix C. | Reference | AAT | 0 – n |
| Identifier | dcterms:identifier `.sourceResource.identifier` | | rdfs:Literal | ID of described resource within a given context. | Literal | | 0 – n |
| Language* | dcterms:language `.sourceResource.language` | | dcterms:LinguisticSystem (as skos:Concept) | Language(s) of described resource. Strongly recommended for text materials. | Reference | Lexvo | 0 – n |
| Place* | dcterms:spatial `.sourceResource.spatial` | | dpla:Place | Spatial characteristics of described resource, such as a country, city, region, address, or other geographical term. Captures aboutness. | Reference | | 0 – n |
| Publisher* | dcterms:publisher `.sourceResource.publisher` | | edm:Agent | Entity responsible for making the described resource available, typically the publisher of a text (not edm:dataProvider or edm:provider). | Reference | | 0 – n |
| Relation | dc:relation `.sourceResource.relation` | | | Related resource. | Literal or Reference | | 0 – n |
| Replaced By | dpla:isReplacedBy `.sourceResource.isReplacedBy` | dc:relation | | Another resource that references, cites, or otherwise points to the described resource. | Literal | | 0 – n |
| Replaces | dpla:replaces `.sourceResource.replaces` | dc:relation | | A related resource that is supplanted, displaced, or superseded by the described resource. | Literal | | 0 – n |
| **Rights*** | dc:rights `.sourceResource.rights` | | | Information about rights held in and over the described resource. Required. See Appendix C for note on v5.0 deprecation. | Literal | | 1 – n |
| Rights Holder | dcterms:rightsholder `.sourceResource.rightsHolder` | | edm:Agent | A person or organization owning or managing rights over the resource. | Literal | | 0 – n |
| Subject* | dcterms:subject `.sourceResource.subject` | | skos:Concept | Topic of described resource. | Reference | | 0 – n |
| Temporal Coverage | dcterms:temporal `.sourceResource.temporal` | dc:coverage | edm:TimeSpan | Temporal characteristics of the described resource. Captures aboutness. | Reference | | 0 – n |
| **Title*** | dcterms:title `.sourceResource.title` | | | Primary name given to the described resource. | Literal | | 1 – n |
| Type* | dcterms:type `.sourceResource.type` | | rdf:Class | Nature or genre of described resource. Strongly recommended. | Reference | dcterms:DCMI Type | 0 – n |

---

### 4.1.B class = edm:WebResource

Contains the attributes of the digital representation of the web resource, not of the SourceResource.

| Label | Property | Sub-property of | Range | Usage | Data Type | Vocab/Syntax Schema | Obligation |
|---|---|---|---|---|---|---|---|
| File Format | dc:format `.[property].format` | | | Web resource format. | Literal | dcterms:imt | 0 – n |
| Rights | dc:rights `.[property].rights` | | | Statement about rights associated with the web resource. See Appendix C for note on v5.0 deprecation. | Literal | | 0 – n |
| Rights Statement | edm:rights `.[property].rights` | dc:rights | dcterms:RightsStatement | The rights statement that applies to this digital representation. | Reference | | 0 – 1 |
| IIIF Manifest | dcterms:isReferencedBy `.[property].isReferencedBy` | | | A resource that references or otherwise points to this described resource. Used for a manifest URI for a IIIF resource. | Reference | | 0 – 1 |

*Note: `svcs:has_Service` (IIIF Base URL), added in MAP v5.0, is not currently implemented.*

---

### 4.1.C class = ore:Aggregation

The aggregation of attributes that apply to the described resource as a whole, grouped from `edm:WebResource` and `dpla:SourceResource`.

| Label | Property | Sub-property of | Range | Usage | Data Type | Vocab/Syntax Schema | Obligation |
|---|---|---|---|---|---|---|---|
| Aggregated SR | edm:aggregatedCHO `.aggregatedCHO` | | edm:ProvidedCHO | Unambiguous ID to SR. | Reference | | 1 |
| Data Provider* | edm:dataProvider `.dataProvider` | dcterms:provenance | edm:Agent | The organization or entity that supplies data to DPLA through a Provider. If a Data Provider contributes data directly to DPLA the values in edm:dataProvider and edm:provider will be the same. | Reference | | 1 |
| Digital Resource Original Record | dpla:originalRecord `.originalRecord` | ore:aggregates | edm:InformationResource | Complete original record provided by partner. | Reference | | 1 |
| Has View | edm:hasView `.hasView` | ore:aggregates | edm:WebResource | Relates an ore:Aggregation with an edm:WebResource. | Reference | | 0 – n |
| Intermediate Provider | dpla:intermediateProvider `.intermediateProvider` | edm:hasMet | edm:Agent | An intermediate organization that selects, collates, or curates data from a Data Provider that is then aggregated by a Provider from which DPLA harvests. The organization must be distinct from both the Data Provider and the Provider in the data supply chain. | Reference | | 0 – 1 |
| Is Shown At* | edm:isShownAt `.isShownAt` | edm:hasView | edm:WebResource | Unambiguous URL reference to digital object in its full information context. | Reference | | 1 |
| Object* | edm:object `.object` | edm:hasView | edm:WebResource | The URL of a suitable source object in the best resolution available on the website of the Data Provider from which edm:preview could be generated for use in a portal. | Reference | | 0 – 1 |
| Preview* | edm:preview `.preview` | edm:hasView | edm:WebResource | The URL of a thumbnail, extract, or other type of resource representing the digital object for the purposes of providing a preview. | Reference | | 0 – 1 |
| Provider* | edm:provider `.provider` | edm:hasMet | edm:Agent | Service or content hub aggregating or providing access to the Data Provider's content. May contain the same value as Data Provider. | Reference | | 1 |
| Rights Statement* | edm:rights `.rights` | dc:rights | dcterms:RightsStatement | The rights statement that applies to the digital representation given in edm:object or edm:isShownAt. | Reference | | 0 – 1 |
| IIIF Manifest ⚑ WG Review | dpla:iiifManifest `.iiifManifest` | | | *[Definition needed. Currently implemented as a top-level field on the Aggregation. See Working Group Review section.]* | Reference | | 0 – 1 |
| Media Master ⚑ WG Review | dpla:mediaMaster `.mediaMaster` | | edm:WebResource | *[Definition needed. Currently implemented as zero-to-many on the Aggregation. See Working Group Review section.]* | Reference | | 0 – n |
| Tags ⚑ WG Review | dpla:tags `.tags` | | | *[Definition needed. Currently implemented as zero-to-many URIs on the Aggregation. See Working Group Review section.]* | Reference | | 0 – n |

---

### 4.2 Context Classes

Context classes contain further information about agents, collections, concepts, places, and time spans referred to by a SourceResource. The expression `[property]` stands in for the appropriate property depending on context.

---

### 4.2.A class = edm:Agent

This class comprises people, either individually or in groups, who have the potential to perform intentional actions for which they can be held responsible.

| Label | Property | Sub-property of | Range | Usage | Data Type | Vocab/Syntax Schema | Obligation |
|---|---|---|---|---|---|---|---|
| Name | skos:prefLabel `.[property].name` | rdfs:label | | The preferred name of the Agent. | Literal | | 0 – 1 |
| Provided Label | dpla:providedLabel `.[property].providedLabel` | rdfs:label | | The label extracted from the original provided data prior to DPLA enhancement. | Literal | | 0 – 1 |
| Note | skos:note `.[property].note` | | | Information relating to the agent. | Literal | | 0 – 1 |
| Scheme | skos:inScheme `.[property].inScheme` | | | The URI of an agent scheme. | Reference | | 0 – 1 |
| Exact Match | skos:exactMatch `.[property].exactMatch` | | | An equivalent URI from an external data source. Used for high-confidence matches. | Reference | | 0 – n |
| Close Match | skos:closeMatch `.[property].closeMatch` | | | A similar URI from an external data source. | Reference | | 0 – n |

---

### 4.2.B class = dcmitype:Collection

A collection is an aggregation of items. The term collection means that the resource is described as a group; its parts may be separately described and navigated.

*Note: `dcmitype:Collection` objects can be expressed either as top-level objects in the DPLA API, or as components of an item's `dpla:SourceResource` description.*

| Label | Property | Sub-property of | Range | Usage | Data Type | Vocab/Syntax Schema | Obligation |
|---|---|---|---|---|---|---|---|
| Collection Title* | dcterms:title `.sourceResource.collection.title` | | | Name of the collection or aggregation. | Literal | | 0 – 1 |
| Collection Description* | dcterms:description `.sourceResource.collection.description` | | | Free-text account of aggregation, for example an abstract or content scope note. | Literal | | 0 – n |
| Is Shown At | edm:isShownAt `.sourceResource.collection.isShownAt` | edm:hasView | edm:WebResource | Unambiguous URL reference to the collection in its full information context. | Reference | | 0 – 1 |

---

### 4.2.C class = skos:Concept

A unit of thought or meaning from an organized knowledge base (such as subject terms from a thesaurus or controlled vocabulary) where URIs or local identifiers have been created to represent each concept.

| Label | Property | Sub-property of | Range | Usage | Data Type | Vocab/Syntax Schema | Obligation |
|---|---|---|---|---|---|---|---|
| Concept | skos:prefLabel `.[property].name` | rdfs:label | | The preferred form of the name of the concept. | Literal | | 0 – 1 |
| Provided Label | dpla:providedLabel `.[property].providedLabel` | rdfs:label | | The label extracted from the original provided data prior to DPLA enhancement. | Literal | | 0 – 1 |
| Note | skos:note `.[property].note` | | | Information relating to the concept. | Literal | | 0 – 1 |
| Scheme | skos:inScheme `.[property].inScheme` | | | The URI of a concept scheme. | Reference | | 0 – 1 |
| Exact Match | skos:exactMatch `.[property].exactMatch` | | | An equivalent concept from an external data source. Used for high-confidence matches. | Reference | | 0 – n |
| Close Match | skos:closeMatch `.[property].closeMatch` | | | A similar concept from an external data source. | Reference | | 0 – n |

---

### 4.2.D class = edm:Place

Description of a specific place or region related to the SourceResource.

*Note: Several fields from v4.0 and v5.0 are not currently implemented (see below table). The `coordinates` field is retained from v4.0 despite its deprecation in that version; see Appendix C.*

| Label | Property | Sub-property of | Range | Usage | Data Type | Vocab/Syntax Schema | Obligation |
|---|---|---|---|---|---|---|---|
| Name | skos:prefLabel `.sourceResource.spatial.name` | rdfs:label | | The name of the place as supplied by the Data Provider. | Literal | | 0 – 1 |
| Provided Label | dpla:providedLabel `.sourceResource.spatial.providedLabel` | rdfs:label | | The label extracted from the original provided data prior to DPLA enhancement. | Literal | | 0 – 1 |
| City | dpla:city `.sourceResource.spatial.city` | | | Describes a resource whose label is an inhabited place incorporated as a city, town, etc. | Literal | | 0 – 1 |
| County | dpla:county `.sourceResource.spatial.county` | | | Describes a resource whose label is the largest local administrative unit (e.g. Warwickshire) in a country. | Literal | | 0 – 1 |
| State | dpla:state `.sourceResource.spatial.state` | | | Describes a resource whose label is a first-order political division (e.g. Montana) within a country. | Literal | | 0 – 1 |
| Country | dpla:country `.sourceResource.spatial.country` | | | Describes a resource whose label is a country. | Literal | | 0 – 1 |
| Region | dpla:region `.sourceResource.spatial.region` | | | Describes a resource whose label is an area usually incorporating more than one first-level jurisdiction. | Literal | | 0 – 1 |
| Coordinates ⚑ WG Review | wgs84_pos:lat_long `.sourceResource.spatial.coordinates` | | | Latitudinal and longitudinal coordinates for the most specific geographic location provided. Retained from v4.0 despite deprecation in that version. See Appendix C. | Literal | wgs84 | 0 – 1 |
| Exact Match | skos:exactMatch `.sourceResource.spatial.exactMatch` | skos:mappingRelation | skos:Concept | An equivalent URI from an external data source. Used for high-confidence matches. | Reference | | 0 – n |

**Fields present in v4.0 and/or v5.0 but not currently implemented:**

| Label | Property | Present in |
|---|---|---|
| Latitude | wgs84_pos:lat | v4.0, v5.0 |
| Longitude | wgs84_pos:long | v4.0, v5.0 |
| Altitude | wgs84_pos:alt | v4.0, v5.0 |
| Geometry | geojson:geometry | v4.0, v5.0 |
| Country Code | gn:countryCode | v4.0, v5.0 |
| Note | skos:note | v4.0, v5.0 |
| Scheme | skos:inScheme | v4.0 |
| Close Match | skos:closeMatch | v4.0 |
| Continent | dpla:continent | v5.0 only |
| Territory | dpla:territory | v5.0 only |
| City Section | dpla:citysection | v5.0 only |
| Island | dpla:island | v5.0 only |
| Area | dpla:area | v5.0 only |
| Extraterrestrial Area | dpla:extraterrestrialarea | v5.0 only |

---

### 4.2.E class = edm:TimeSpan

TimeSpan declares more complete information about a specific date or range of time related to a SourceResource.

| Label | Property | Sub-property of | Range | Usage | Data Type | Vocab/Syntax Schema | Obligation |
|---|---|---|---|---|---|---|---|
| Original Source Date | dpla:providedLabel `.[property].providedLabel` | | | Date value as supplied by data provider. | Literal | | 0 – n |
| Display Date | skos:prefLabel `.[property].displayDate` | | | Date value as enriched by DPLA. | Literal | EDTF | 0 – 1 |
| Begin | edm:begin `.[property].begin` | | | Date timespan started. | Literal | EDTF | 0 – 1 |
| End | edm:end `.[property].end` | | | Date timespan finished. | Literal | EDTF | 0 – 1 |

---

## Appendix A: Annotation Class and Subclasses

The annotation classes (`oa:Annotation`, `cnt:ContentAsText`, `oa:Motivation`) are carried forward unchanged from MAP v4.0 and v5.0. They are not implemented in the current ingest codebase. For full definitions see MAP v4.0 Appendix A.

---

## Appendix B: Required, Required if Available, and Recommended Properties

### dpla:SourceResource
This class is mandatory. Required or recommended properties are listed below; all others are optional.

| Label | Obligation |
|---|---|
| Collection | Recommended |
| Creator | Recommended |
| Date | Recommended |
| Format | Recommended |
| Language | Recommended |
| Place | Recommended |
| Publisher | Recommended |
| Rights | **Required** |
| Subject | Recommended |
| Title | **Required** |
| Type | Recommended |

### edm:WebResource
This class is mandatory. All fields are optional unless noted.

| Label | Obligation |
|---|---|
| Rights Statement | Recommended |

### ore:Aggregation
This class is mandatory. The fields below are required; all others are optional.

| Label | Obligation |
|---|---|
| Data Provider | **Required** |
| Is Shown At | **Required** |
| Preview | Required if available |
| Provider | **Required** |
| Rights Statement | **Required** |

### dcmitype:Collection
This class is not mandatory but is strongly recommended when present.

| Label | Obligation |
|---|---|
| Collection Title | Required when present |
| Collection Description | Recommended |

---

## Appendix C: Changes from Version 4.0

The following table documents every divergence of MAP v4.1 from MAP v4.0. Each change is marked with its source: **v5.0** (implementing a change introduced in MAP v5.0), **New work** (introduced by DPLA staff, not driven by either MAP version), or **Retained** (a v4.0 deprecated field kept in the implementation).

| Change | Class affected | v4.0 | v5.0 | v4.1 | Source |
|---|---|---|---|---|---|
| `dcterms:isReferencedBy` (IIIF Manifest) added to WebResource | edm:WebResource | Absent | Added | Present | v5.0 |
| `dc:rights` retained on WebResource (v5.0 deprecated it) | edm:WebResource | Present | Deprecated | Present | Retained (WG review) |
| `edm:rights` label changed from "Standardized Rights Statement" to "Rights Statement" | edm:WebResource, ore:Aggregation | "Standardized Rights Statement" | "Rights Statement" | "Rights Statement" | v5.0 |
| `skos:prefLabel` (Display Date) added to TimeSpan | edm:TimeSpan | Absent | Added | Present | v5.0 |
| `edm:isShownAt` added to Collection | dcmitype:Collection | Absent | Added | Present | v5.0 |
| Hierarchical place fields restored: city, county, state, country, region (were deprecated in v4.0) | edm:Place | Deprecated | Re-added | Present | v5.0 |
| `gn:parentFeature` removed from Place | edm:Place | Present | Deprecated | Absent | v5.0 |
| `coordinates` field retained on Place despite v4.0 deprecation | edm:Place | Deprecated | Not restored | Present | Retained (WG review) |
| `dc:rights` retained on SourceResource (v5.0 deprecated it) | dpla:SourceResource | Required | Deprecated | Required | Retained (WG review) |
| `dpla:iiifManifest` added as top-level field on Aggregation | ore:Aggregation | Absent | Absent | Present | New work (WG review) |
| `dpla:mediaMaster` added to Aggregation | ore:Aggregation | Absent | Absent | Present | New work (WG review) |
| `dpla:tags` added to Aggregation | ore:Aggregation | Absent | Absent | Present | New work (WG review) |

### v5.0 changes not adopted in v4.1

The following changes were introduced in MAP v5.0 but are not reflected in the current implementation and therefore not included in v4.1:

| Change | Note |
|---|---|
| `edm:hasType` renamed from "Genre" to "Subtype"; path `.sourceResource.genre` → `.sourceResource.subtype` | Codebase uses v4.0 naming throughout |
| `dc:rights` deprecated from SourceResource and WebResource | Retained as required in codebase |
| `dcterms:RightsStatement` class added | Not implemented |
| `svcs:has_Service` (IIIF Base URL) added to WebResource | Not implemented |
| Additional place fields: continent, territory, citysection, island, area, extraterrestrialarea | Not implemented |
| `dc:format` range declared as `skos:Concept` | Codebase retains string/literal |
| Preferred terms list (AAT) for Subtype/Genre (v5.0 Appendix C) | Not implemented |
