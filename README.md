# DPLA Ingestion 3

This project is an ETL system for cultural heritage metadata. The system has five primary components, harvesting original records, mapping original records into DPLA MAP records, enriching DPLA MAP records, exporting DPLA MAP records as JSON-L to be indexed, and exporting eligible DPLA MAP records in Wikimedia markup.

* [Harvest](#harvest)
* [Mapping and validation](#mapping-and-validation)
    * [XML mapping example](#xml-mapping-example)
    * [JSON mapping example](#json-mapping-example)
    * [Filtering](#filtering)
    * [Validations](#validations)
        * [edmRights](#edmrights-normalization-and-validation)  
    * [Summary and logs](#summary-and-logs)
* [Enrichment and normalization](#enrichment-and-normalization)
    * [Text normalizations](#text-normalizations)
    * [Enrichments](#enrichments)
        * [dataProvider](#dataprovider)
        * [date](#date)
        * [language](#language)
        * [type](#type)
    * [Summary and reports](#summary-and-reports)
* [JSON-L](#jsonl)
* [Wikimedia](#wikimedia)
    * [Eligibility](#eligibility)
    * [Metadata](#metadata)
    * [Media](#media)
        * [ContentDM and IIIF Manifests](#contentdm-and-iiif-manifests)



# Harvest

We harvest data from multiple sources but generally they fall into three categories: api, file, and oai.

<img src="https://i.imgur.com/WZWuYnr.png" height="300"/>


# Mapping and Validation
Each data provider has their own mapping document which defines how values are moved from the harvested records into DPLA records. While some mapping may look similar because the data providers use the same metadata schema, we do not reuse mappings between providers. All provider mappings are defined [here](https://github.com/dpla/ingestion3/tree/develop/src/main/scala/dpla/ingestion3/mappers/providers).

### XML mapping example

If we take this PA Digital original XML record
```xml
<record>
  <header>
    <identifier>oai:YOUR_OAI_PREFIX:dpla_test:oai:libcollab.temple.edu:dplapa:BALDWIN_kthbs_arch_699</identifier>
    <datestamp>2019-08-29T16:57:36Z</datestamp>
    <setSpec>dpla_test</setSpec>
  </header>
  <metadata>
    <oai_dc:dc>
      <dcterms:date>1955</dcterms:date>
    </oai_dc:dc>
  </metadata>
</record>
```
and look at the mapping for the `date()` field from the PA Digital hub ([code](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/mappers/providers/PaMapping.scala#L40-L42)).  It extracts the text from the `date` property in the original XML document, `data`. The text value is then used to create `EdmTimeSpan` objects using the [stringOnlyTimeSpan()](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/model/package.scala#L48) method.  

```scala
  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "date")
      .map(stringOnlyTimeSpan)
```

### JSON mapping example
Similarly, looking at this Digital Library of Georgia original JSON record
```json
{
  "id": "aaa_agpapers_1016",
  "collection_titles_sms": [
    "Auburn University - Agriculture and Rural Life Newspapers Collection"
  ],
  "dcterms_title_display": [
    "1899-12-14: Manufacturers' Review, Birmingham, Alabama, Volume 1, Issue 5"
  ],
  "dc_date_display": [
    "1899-12-14"
  ],
  "created_at_dts": "2017-05-25T21:19:27Z",
  "updated_at_dts": "2017-06-07T15:17:06Z"
}
```
the `date()` mapping ([code](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/mappers/providers/DlgMapping.scala#L76-L78)) takes the text values extracted from  the `dc_date_display` field in the original JSON document, `data`.  The text value is then used to create `EdmTimeSpan` objects using the [stringOnlyTimeSpan()](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/model/package.scala#L48) method.  
```scala
  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings("dc_date_display")(data)
      .map(stringOnlyTimeSpan)
```

While the original records look very different, the code used to map the values looks quite similar. This makes the code more readable and allows us to write fairly homogeneous mappings regardless of the format or schema of the underlying original records.  

### Filtering
There are provider specific rules and exceptions written into some mappings and it is outside the scope of this document to enumerate and explain all of them but one example of filtering non-preferred values is provided below.

For records coming from the Ohio Digital Network we have a filter in place for the `format` field ([code](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/mappers/providers/OhioMapping.scala#L58-L65)). This level of filtering is not common and is based on careful review of existing metadata with an eye towards strict compliance with existing metadata guidelines.

```scala
  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    // Extract text values from format property
    extractStrings(data \ "metadata" \\ "format")
      // if text value contains `;` split it into multiple values
      .flatMap(_.splitAtDelimiter(";"))
      // filter out any text values contained in the following
      // term block lists
      //   - DigitalSurrogateBlockList (e.g. application/pdf, application/xml)
      //   - FormatTypeValuesBlockList (e.g. Image, Still image, Sound, Audio)
      //   - ExtentIdentificationList (e.g. 1 x 2, 1 X 2, 1x2 but not 1 xerox)
      .map(_.applyBlockFilter(
         DigitalSurrogateBlockList.termList ++
         FormatTypeValuesBlockList.termList ++
        ExtentIdentificationList.termList))
      .filter(_.nonEmpty)
```


### Validations
After attempting to map all fields from an original record we inspect the results and perform validations on specific fields. These validations fall into two two groups, errors and warnings. Errors will cause a record to fail mapping and warnings are informational only, the record will pass and appear online.

  Errors are generally recorded only for **missing required fields** which include
  * dataProvider
  * isShownAt
  * edmRights/rights
  * title
  * originalId (basis for DPLA identifier)

#### edmRights normalization and validation
This is a special case because it inverts how we approach this process. Typically, normalization follows validation but in this we normalize edmRights value first and then validate the normalized value. This process has also been documented the [DRAFT DPLA Rights Statement Validation Whitepaper](https://docs.google.com/document/d/1PyJM_Zo9q34HvctoB9Cx0xoUd7PcNX3uzD_19txzYY0/edit?usp=sharing).

How are `edmRights` URIs normalized?
1. Change `https://` to `http://`
2. Drop `www`
3. Change `/page/` to `/vocab/` (applies to rightsstatements.org values)
4. Drop query parameters (ex. `?lang=en`)
5. Add missing `/` to end of URI
6. Remove trailing punctuation (ex. http://rightsstaments.org/vocab/Inc/;)
7. Remove all leading and trailing whitespace

Messages for all of these operations are logged as warnings in our mapping summary reports.

```text
                                Message Summary
Warnings
- Normalized remove trailing punctuation, edmRights.......................79,594
- Normalized https://, edmRights...........................................7,220
- Normalized /page/ to /vocab/, edmRights....................................660
- Normalized add trailing `/`, edmRights......................................57
- Total..................................................................167,182
```

After we have normalized `edmRights` we validate those values against a list of ~600 accepted rightsstatements.org and creativecommons.org URIs ([full list here](https://github.com/dpla/ingestion3/blob/6a4e1e38152da480e5b33070df2996fedd3ea51f/src/main/scala/dpla/ingestion3/model/DplaMapData.scala#L153-L745)). If the value we created during normalization does not exactly match one of the 600 accepted values then we do not map the value.


**Scenario A**
```xml
<metadata>
    <oai_qdc:qualifieddc
            xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/"
            xmlns:dcterms="http://purl.org/dc/terms/"
            xmlns:edm="http://www.europeana.eu/schemas/edm/">
        <edm:rights>http://specialcollections.edu/rights/copyright/1.0</edm:rights>
        <dc:rights>All rights reserved</dc:rights>
    </oai_qdc:qualifieddc>
</metadata>
```
A record the record has an invalid edmRights URI but also contains a `dc:rights` value. It will pass mapping because *either* `edmRights` or `dc:rights` is required but the edmRights value will not appear in the record.

**Scenario B**
```xml
<metadata>
    <oai_qdc:qualifieddc
            xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/"
            xmlns:dcterms="http://purl.org/dc/terms/"
            xmlns:edm="http://www.europeana.eu/schemas/edm/">
        <edm:rights>http://specialcollections.edu/rights/copyright/1.0</edm:rights>
    </oai_qdc:qualifieddc>
</metadata>
```
A record only contains an invalid `edmRights` URI and it did not pass our validation check then the record will fail mapping because it will not have any rights information associated with the record.

### Summary and logs
After every mapping job we generate three sources of feedback:
- a `_SUMMARY` file
- a CSV detailing the errors
- a CSV detailing the warnings.  

**Summary**

This file provides a high level overview of the mapping process and summarizes the messages logged. Importantly, it breaks down results in terms of records and messages because a single record may have multiple fatal errors (missing both rights and dataProvider for example) or multiple warnings (a single edmRights values is normalized in multiple ways, https:// to http:// and page to vocab).

```text
                                Mapping Summary

Provider...........................................................ORBIS-CASCADE
Start date...................................................02/10/2021 10:56:14
Runtime.............................................................00:03:45.285

Duplicate records in harvest...................................................0

Attempted.................................................................79,651
Successful................................................................38,608
Failed....................................................................41,043


                              Errors and Warnings

Messages
- Errors..................................................................41,043
- Warnings...............................................................167,182

Records
- Errors..................................................................41,043
- Warnings................................................................79,651

                                Message Summary
Warnings
- Normalized remove trailing punctuation, edmRights.......................79,594
- Duplicate, rights and edmRights.........................................76,747
- Normalized https://, edmRights...........................................7,220
- Not a valid edmRights URI, edmRights.....................................2,904
- Normalized /page/ to /vocab/, edmRights....................................660
- Normalized add trailing `/`, edmRights......................................57
- Total..................................................................167,182
Errors
- Missing required field, isShownAt.......................................41,043
- Total...................................................................41,043

~/orbis-cascade/mapping/20210210_085614-orbis-cascade-MAP4_0.MAPRecord.avro/_LOGS/errors
~/orbis-cascade/mapping/20210210_085614-orbis-cascade-MAP4_0.MAPRecord.avro/_LOGS/warnings
```

**Error logs**

- **message** - Describes the error recorded, most frequently this will be *Missing required field*
- **level** - *error* by default
- **field** - The afflicted field in DPLA MAP model  
- **id** - The local persistent identifier which includes the DPLA prefix `hub--{local_id}`
- **value** - Value of the field if available but `MISSING` when the error is *Missing required field*

```csv
message,level,field,id,value
Missing required field,error,isShownAt,orbis-cascade--http://harvester.orbiscascade.org/record/8a98044e8c695b3ae99dc15e9ed75026,MISSING
Missing required field,error,isShownAt,orbis-cascade--http://harvester.orbiscascade.org/record/07b9a70513bb7240d8586ca5f51fa3cb,MISSING
```

**Warning logs**

- **message** - Describes the warning recorded
- **level** - *warn* by default
- **field** - The afflicted field in DPLA MAP model  
- **id** - The local persistent identifier which includes the DPLA prefix `{hub}--{local_id}`
- **value** - Value of the field if available but `MISSING` if no value in field

```csv
message,level,field,id,value
Not a valid edmRights URI,warn,edmRights,orbis-cascade--http://harvester.orbiscascade.org/record/3d07e14cc52609689e1fca1cc17273b2,http://creativecommons.org/share-your-work/public-domain/pdm/
Normalized remove trailing punctuation,warn,edmRights,orbis-cascade--http://harvester.orbiscascade.org/record/cf959976c177c9468e302e52dffcee1e,http://rightsstatements.org/vocab/InC/1.0/
```
# Enrichment and Normalization

All successfully mapped records are run through a series of text normalizations and enrichments. Almost every field is normalized in some fashion and some field are subject to a more robust set of normalizations. The enrichments are limited to a specific set of fields.

## Text normalizations

For a comprehensive view of the normalizations that are run please see [this code](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/enrichments/normalizations/StringNormalizations.scala) but these normalizations are run over almost every field in every class.
- `deduplication` - Only unique values are propagated to the index
- `stripHTML` - Removes any HTML markup in the text
- `reduceWhitespace` - Reduce multiple whitespace values to a single and removes leading and trailing white space

### Class and field specific normalizations
These normalizations are run over all instances of the specified class

#### sourceResource
*format*
- `capitalizeFirstChar` - Find and capitalize the first character in a given string

*title*
- `stripBrackets` - Removes matching leading and trailing brackets (square, round and curly braces)
- `cleanupLeadingPunctuation` - Removes leading colons, semi-colons, commas, slashes, hyphens and whitespace characters (whitespace, tab, new line and line feed) that precede the first letter or digit
- `cleanupEndingPunctuation` - Removes trailing colons, semi-colons, commas, slashes, hyphens and whitespace characters (whitespace, tab, new line and line feed) that follow the last letter or digit

#### edmAgent (creator, contributor, publisher, dataProvider)

*name*
- `stripBrackets` - Removes matching leading and trailing brackets (square, round and curly braces)
- `stripEndingPeriod` - Removes singular period from the end of a string. Ignores and removes trailing whitespace
- `cleanupLeadingPunctuation` - Removes leading colons, semi-colons, commas, slashes, hyphens and whitespace characters (whitespace, tab, new line and line feed) that precede the first letter or digit
- `cleanupEndingPunctuation` - Removes trailing colons, semi-colons, commas, slashes, hyphens and whitespace characters (whitespace, tab, new line and line feed) that follow the last letter or digit

#### skosConcept (language, subject)

*concept*, *providedLabel*
- `stripBrackets` - Removes matching leading and trailing brackets (square, round and curly braces)
- `stripEndingPeriod` - Removes singular period from the end of a string. Ignores and removes trailing whitespace
- `cleanupLeadingPunctuation` - Removes leading colons, semi-colons, commas, slashes, hyphens and whitespace characters (whitespace, tab, new line and line feed) that precede the first letter or digit
- `cleanupEndingPunctuation` - Removes trailing colons, semi-colons, commas, slashes, hyphens and whitespace characters (whitespace, tab, new line and line feed) that follow the last letter or digit
- `capitalizeFirstChar` - Find and capitalize the first character in a given string


#### edmTimeSpan

*prefLabel*, *begin*, *end*
- `stripDblQuotes` - Strip all double quotes from the given string

## Enrichments
Enrichments modify existing data and improve its quality to enhance its functionality

### dataProvider
One requirement of the Wikimedia project is that data provider values must be mapped to a Wikidata URI. DPLA maintains a lookup table of data provider names and Wiki URIs (see [institutions.json](https://github.com/dpla/ingestion3/blob/develop/src/main/resources/wiki/institutions.json) and [hubs.json](https://github.com/dpla/ingestion3/blob/develop/src/main/resources/wiki/hubs.json)). The enrichment adds the Wiki URI to the `edmAgent.exactMatch` property. Without a Wikidata URI, a record cannot be uploaded to Wikimedia.

This enrichment is still under development and subject to change.

### date
Generates begin and end dates from a provided data label that matches either pattern:
- YYYY
- YYYY-YYYY

### language
Resolves ISO-639-1/3 codes to their full term. This will return an enriched SkosConcept class where the value mapped from the original record is stored in the `providedLabel` field and the complete term is stored in the `concept` field.

```scala
  // Lanuage Enrichment Test for 'Modern Greek', the language value mapped from the original records was `gre`

  // ISO-639 abbreviation, ISO-639 label
  // gre,"Greek, Modern (1453-)"
  it should "return an enriched SkosConcept for 'gre')" in {
    val originalValue = SkosConcept(providedLabel = Option("gre"))
    val expectedValue = SkosConcept(
      providedLabel = Option("gre"),
      concept = Option("Greek, Modern (1453-)")
    )
    assert(languageEnrichment.enrich(originalValue) === Option(expectedValue))
  }
```

For additional examples of how this enrichment functions see [LanguageEnrichmentTests](https://github.com/dpla/ingestion3/blob/d9305d5733ba3553522b5d1c287cec2cfa061bfd/src/test/scala/dpla/ingestion3/enrichments/LanguageEnrichmentTest.scala)

When exporting the JSON-L, SkosConcepts are evaluated and if a `concept` is defined then that value is used otherwise, `providedLabel` (see [language definition in JSON-L export](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/model/package.scala#L170-L183)).


### type
Resolves uncontrolled type terms mapped from original records to appropriate DCMIType values (case insensitive). The mapping of uncontrolled to controlled terms is defined ([here](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/enrichments/TypeEnrichment.scala#L17-L147)). If the original term cannot be mapped to a valid DCMIType value then it is dropped and the record is indexed without a `type` value.

 ```scala
   // Type Enrichment Test for 'book', the type value mapped from the original records was `book`
   // and the enrichment should return `text`.

   it should "return an enriched string for 'book'" in {
     val originalValue = "book"
     val expectedValue = Some("text")
     assert(typeEnrichment.enrich(originalValue) === expectedValue)
   }
```

For additional examples of how this enrichment functions see [TypeEnrichmentTest](https://github.com/dpla/ingestion3/blob/d9305d5733ba3553522b5d1c287cec2cfa061bfd/src/test/scala/dpla/ingestion3/enrichments/TypeEnrichmentTest.scala)

## Summary and reports

Enrichment results are also summarized and the transformations logged to CSV files. These summary and report documents only describe *enrichment* operations and do not include any transformations done through text normalizations.  

### Summary

This is an example overview of operations performed against records from the Big Sky Digital Network. In this case that overall we were able to improve almost every single record with only 557 which were not changed in any way.

The report breaks down per-enrichment how many records were improved.

```text
                               Enrichment Summary

Provider......................................................................MT
Start date...................................................02/15/2021 23:28:56
Runtime.............................................................00:02:51.742

Attempted.................................................................99,479
Improved..................................................................98,922
Unimproved...................................................................557

                               Field Improvements
Type......................................................................96,701
- Enriched value..........................................................72,010
- Original value..........................................................24,691
Language.......................................................................0

Date......................................................................88,761
- Enriched value..........................................................46,460
- Original value..........................................................42,301
Data Provider.............................................................99,479
- Original value..........................................................73,664
- Enriched value..........................................................25,815

                                   Log Files
~/mt/enrichment/20210215_212856-mt-MAP4_0.EnrichRecord.avro/_LOGS/type
~/mt/enrichment/20210215_212856-mt-MAP4_0.EnrichRecord.avro/_LOGS/date
~/mt/enrichment/20210215_212856-mt-MAP4_0.EnrichRecord.avro/_LOGS/dataProvider
```

### Reports

Beyond this summary report we drill down and for every record log how we did or did not transform each field that we attempt to enrich.

For example, if we examine the first few lines of `./_LOGS/type/` enrichment CSV report. It shows that we did not enrich the first record because its type value, `text`, was already a valid DCMIType term. The other records were enriched from `still image` to `image`.

```csv
level,message,field,value,enrichedValue,id
info,Original value,type,text,Not enriched,3cfe5197e0140ad4329742fb12e49190
info,Enriched value,type,still image,image,4485f5246b5ede59dae4486425f5149e
info,Enriched value,type,still image,image,c5c4dd8d0d90ea7df8e686e717de4f38
```

The reports are generated every time mapped data is enriched.


# JSONL

 TBD

# Wikimedia
Records which meet eligibility requirements can have their full-frame media assets and some associated metadata uploaded to Wikimedia. ingestion3 is only partly responsible for this process (chiefly the evaluation of eligibility and Wiki markdown/metadata creation) but the actual work of uploading images to Wikimedia is handled by the [ingest-wikimedia](https://github.com/dpla/ingest-wikimedia) project.

* [Eligibility](#eligibility)
* [Metadata](#metadata)
* [Media](#media)
    * [ContentDM and IIIF Manifests](#contentdm-and-iiif-manifests)

Records which meet eligibility requirements can have their fullframe media assets and some associated metadta uploaded to Wikimedia.

## Eligibility

Records must meet three minimum requirements to be eligible for upload
1. **Standardized rights** - The record must have an `edmRights` URI and it must be one of these values. All ports and versions of these values are valid.
```text
http://rightsstatements.org/vocab/NoC-US/
http://creativecommons.org/publicdomain/mark/
http://creativecommons.org/publicdomain/zero/
http://creativecommons.org/licenses/by/
http://creativecommons.org/licenses/by-sa/
```

2. **Media assets** - The record must have either a `iiifManifest` or a `mediaMaster` URL. This value is distinct from the `object` mapping which is a single value and expected to a low resolution thumbnail (150px). The URLs for `mediaMaster` should point to the highest resolution possible and can be more than one URL.
3. **Data Provider URI** - The `dataProvider` name must be reconciled to a WikiData URI. This is an enrichment that DPLA performs on these values (see [dataProvider enrichments](#dataprovider))


## Metadata
For each image file that is uploaded a corresponding block of metadata is also attached.

* Creator (multiple values joined by a `;`)
* Title (multiple values joined by a `;`)
* Description (multiple values joined by a `;`)
* Date (multiple values joined by a `;`)
* edmRights
* Data Provider Wikidata URI
* Provider
* isShownAt
* DPLA ID
* Local IDs (multiple values joined by a `;`)

This is the Wiki markdown block ([code](https://github.com/dpla/ingestion3/blob/62f809499846a5cca1ebfd10ca23662d433c5df1/src/main/scala/dpla/ingestion3/model/package.scala#L224-L250))
```text
{{int:filedesc}} ==
| {{ Artwork
|   | Other fields 1 = {{ InFi | Creator | ${record.sourceResource.creator.flatMap { _.name }.map(escapeWikiChars).mkString("; ")} }}
|   | title = ${record.sourceResource.title.map(escapeWikiChars).mkString("; ")}
|   | description = ${record.sourceResource.description.map(escapeWikiChars).mkString("; ")}
|   | date = ${record.sourceResource.date.flatMap { _.prefLabel }.map(escapeWikiChars).mkString("; ")}
|   | permission = {{${getWikiPermissionTemplate(record.edmRights)}}}
|   | source = {{ DPLA
|       | ${escapeWikiChars(dataProviderWikiUri)}
|       | hub = ${escapeWikiChars(record.provider.name.getOrElse(""))}
|       | url = ${escapeWikiChars(record.isShownAt.uri.toString)}
|       | dpla_id = $dplaId
|       | local_id = ${record.sourceResource.identifier.map(escapeWikiChars).mkString("; ")}
|   }}
|   | Institution = {{ Institution | wikidata = $dataProviderWikiUri }}
|   | Other fields = ${getWikiOtherFieldsRights(record.edmRights)}
| }}
```
An [example](https://commons.wikimedia.org/wiki/File:%22Babe%22,_Walbridge_Park_elephant,_Toledo,_Ohio_-_DPLA_-_6777c0761ba2881404729e3cc9593207_(page_1).jpg) of that markdown on Commons and the same item in [DPLA](https://dp.la/item/6777c0761ba2881404729e3cc9593207).

## Media
Unlike the normal media fields we aggregate (thumbnail/object/preview) which are limited to a single asset, these uploads will include all media assets provided in either the `mediaMaster` or `iiifManifest` mappings. Media assets will be uploaded with a file name and Wikimedia page name the follows the this convention.

```python
        # take only the first 181 characters of record title
        # replace [ with (
        # replace ] with )
        # replace / with -
        escaped_title = title[0:181] \
            .replace('[', '(') \
            .replace(']', ')') \
            .replace('/', '-') \
            .replace('{', '(') \
            .replace('}', ')')

        # Add pagination to page title if needed
        if page is None:
            return f"{escaped_title} - DPLA - {dpla_identifier}{suffix}"
        else:
            return f"{escaped_title} - DPLA - {dpla_identifier} (page {page}){suffix}"
```

Key points to note:
* The record title is limited to the first 181 characters
* There is character substitution for `[]/{}`
* If there are multiple assets associated with a metadata record we add a `page (n)` to the title.

### ContentDM and IIIF Manifests

![Millhouse the magician](https://media.giphy.com/media/ieREaX3VTHsqc/giphy.gif)

When validating whether a record meets the minimum requirements, and neither a IIIF manifest nor media master value is provided, we will attempt to programmatically generate a IIIF manifest url if the isShownAt value looks like a ContentDM URL.

```Java
// If there is neither a IIIF manifest or media master mapped from the original  
// record then try to construct a IIIF manifest from the isShownAt value.
// This should only work for ContentDM URLs.
val dplaMapRecord =
  if(dplaMapData.iiifManifest.isEmpty && dplaMapData.mediaMaster.isEmpty)
    dplaMapData.copy(iiifManifest = buildIIIFFromUrl(dplaMapData.isShownAt))
```

Where `buildIIIFFromUrl()` uses regex matching to identify and extract the necessary
components of the isShownAt URL.

```Java
def buildIIIFFromUrl(isShownAt: EdmWebResource): Option[URI] = {
//    We want to go from this isShownAt URL
//    http://www.ohiomemory.org/cdm/ref/collection/p16007coll33/id/126923
//    to this iiifManifest URL
//    http://www.ohiomemory.org/iiif/info/p16007coll33/126923/manifest.json
//
//    ^(.*)/collection/(.*?)/id/(.*?)$  -> \iiif/info/\2/\3/manifest.json
//    The first match group should catch only through the TLD, not the /cdm/ref/ or
//    whatever is that in between part of the URL before /collection/ (which should be discarded).

  val contentDMre = "(.*)(.*\\/cdm\\/.*collection\\/)(.*)(\\/id\\/)(.*$)"
  val uri = isShownAt.uri.toString

  val pattern = Pattern.compile(contentDMre)
  val matcher = pattern.matcher(uri)
  matcher.matches()

  Try {
      val domain: String = matcher.group(1)
      val collection: String = matcher.group(3)
      val id: String = matcher.group(5)
      Some(URI(s"$domain/iiif/info/$collection/$id/manifest.json"))
    } match {
      case Success(s: Option[URI]) => s
      case Failure(_) => None
    }
}
```
This generated URL is then used to satisfy the `isAssetEligible` criteria during
the overall Wikimedia validation process. The record will still need to pass both
the rights URI validation and eligible hub or contributing institution validation.

Refer back to the [Wikimedia eligibility](#eligibility) section for details


=======


[![Codacy Badge](https://api.codacy.com/project/badge/Grade/6a9dfda51ad04ce3acfb7fcb441af846)](https://www.codacy.com/app/mdellabitta/ingestion3?utm_source=github.com&utm_medium=referral&utm_content=dpla/ingestion3&utm_campaign=badger)
[![Build Status](https://travis-ci.org/dpla/ingestion3.svg?branch=master)](https://travis-ci.org/dpla/ingestion3)
