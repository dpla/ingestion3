# DPLA Ingestion 3

This project is an ETL system for cultural heritage metadata. The system has three primary components:

* [Harvest]()
* [Mapping and validation](#mapping-and-validation) 
* [Enrichment and normalization]()
* [JSONL]()
* [Wikimedia]()



# 

# Mapping and Validation 
Each data provider has their own mapping document which defines how values are moved from the harvested records into DPLA records. While some mapping may look similar because the data providers use the same metadata schema, we do not reuse mappings between providers. All provider mappings are defined [here](https://github.com/dpla/ingestion3/tree/develop/src/main/scala/dpla/ingestion3/mappers/providers). 

#### XML mapping example 

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
and look at the `data()` mapping from the PA Digital hub ([code](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/mappers/providers/PaMapping.scala#L40-L42)).  It extracts the text from the `date` property in the XML document, `data`. The text value is then used to create `EdmTimeSpan` objects using the [stringOnlyTimeSpan()](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/model/package.scala#L48) method.  

```scala 
  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "date")
      .map(stringOnlyTimeSpan)
```

#### JSON mapping example
Simliarlly, looking at this Digital Library of Georgia original JSON record
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
the `date()` mapping ([code](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/mappers/providers/DlgMapping.scala#L76-L78)) takes the text values extracted from  the `dc_date_display` field in the JSON document, `data`.  The text value is then used to create `EdmTimeSpan` objects using the [stringOnlyTimeSpan()](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/model/package.scala#L48) method.  
```scala 
  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings("dc_date_display")(data)
      .map(stringOnlyTimeSpan)
```

While the original records look very different, the code used to map the values looks quite similar. This makes the code more readable and allows us to write homogeneous mappings regardless of the format or schema of the underlying original records.  

#### Filtering 
There are provider specific rules and exceptions written into some mappings and it is outside the scope of this document to enumerate and explain all of them but one example of filtering non-preferred values is provided below. 

For records coming from the Ohio Digital Network we have a filter in place for the `format` field ([code](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/mappers/providers/OhioMapping.scala#L58-L65)). 

```scala 
  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    // Extract text values from format property
    extractStrings(data \ "metadata" \\ "format")
       // if text value contains `;` split around it 
      .flatMap(_.splitAtDelimiter(";"))
      // filter out any text values contained in the following 
      // term block lists
      //   - DigitalSurrogateBlockList (e.g. application/pdf, application/pdf)
      //   - FormatTypeValuesBlockList (e.g. Image, Still image, Sound, Audio)
      //   - ExtentIdentificationList (e.g. 1 x 2, 1 X 2, 1x2 but not 1 xerox)
      .map(_.applyBlockFilter(
         DigitalSurrogateBlockList.termList ++
         FormatTypeValuesBlockList.termList ++
        ExtentIdentificationList.termList))
      .filter(_.nonEmpty)
```
This level of filtering is based on careful review of existing metadata and with an eye towards strict compliance with existing metadata guidelines.







[![Codacy Badge](https://api.codacy.com/project/badge/Grade/6a9dfda51ad04ce3acfb7fcb441af846)](https://www.codacy.com/app/mdellabitta/ingestion3?utm_source=github.com&utm_medium=referral&utm_content=dpla/ingestion3&utm_campaign=badger)
[![Build Status](https://travis-ci.org/dpla/ingestion3.svg?branch=master)](https://travis-ci.org/dpla/ingestion3)

