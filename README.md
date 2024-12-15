# DPLA Ingestion 3

DPLA's ingestion system is one of the core business systems and is the source of data for the DPLA search portal. It is responsible for harvesting, mapping and enriching cultural heritage metadata from DPLA's network of partners into the DPLA Metadata Application Profile. These records are then indexed and populate our discovery portal of more than [49,000,000 images, texts, videos, and sounds](https://dp.la) from across the United States.

* [How to run ingests](#-running-dpla-cultural-heritage-ingests-)
  * [Helpful links and tools](#helpful-links-and-tools)
  * [Monthly scheduling communications](#scheduling-email)
  * [Running ingests](#running-ingests)
    * [EC2 ingest instance](#ec2-ingest-box)
    * [Locally](#running-ingests-locally)
    * [Hub specific instructions](#exceptions-and-unusual-ingests)
      * [Firewalled endpoints](#firewalled-endpoints) 
      * [Internet Archive - Community Webs](#community-webs)
      * [Digital Virginias](#digital-virginias)
      * Digital Commonwealth
      * Northwest Digital Heritage
      * Tennessee
      * [NARA](README_NARA.md)
      * [Smithsonian](README_SMITHSONIAN.md)
* [ingestion 3](#ingestion-3)
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
        * [Institutional](#institutional)
        * [Standardized rights](#standardized-rights)
        * [Media assets](#media-assets)
        * [Data provider URI](#data-provider-uri)
      * [Wikimedia metadata](#wikimedia-metadata)
      * [Media](#media)
          * [CONTENTdm and IIIF Manifests](#contentdm-and-iiif-manifests)


📚 Running DPLA Cultural Heritage ingests 📚
----

- [Helpful links and tools](#helpful-links-and-tools)
- [Scheduling](#scheduling-email)
- [Running ingests on EC2 instance](#ec2-ingest-box)
- [Running ingests locally](#running-ingests-locally)
- [Exceptions / Usual ingests](#exceptions-and-unusual-ingests)
      
  - [Firewalled endpoints](#firewalled-endpoints) 
  - [Internet Archive - Community Webs](#community-webs)
  - [Digital Virginias](#digital-virginias)
  - Digital Commonwealth
  - Northwest Digital Heritage
  - Tennessee
  - [NARA](README_NARA.md)
  - [Smithsonian](README_SMITHSONIAN.md)

## Helpful links and tools
* [Hub ingest schedule](https://digitalpubliclibraryofamerica.atlassian.net/wiki/spaces/CT/pages/84969744/Hub+Re-ingest+Schedule)
* [Ingestion3 configuration](https://github.com/dpla/ingestion3-conf/)
* [xmll](https://github.com/dpla/xmll)
* [`screen` command](https://lazyprogrammer.me/tutorial-how-to-use-linux-screen/)
* [`vim` quickstart ](https://eastmanreference.com/a-quick-start-guide-for-beginners-to-the-vim-text-editor)
* [OhMyZsh `copyfile` plugin](https://github.com/ohmyzsh/ohmyzsh/tree/master/plugins/copyfile)
* [jq](https://stedolan.github.io/jq/)
* [pyoaiharvester](https://github.com/dpla/pyoaiharvester)
* [aws cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

## Scheduling email
Monthly scheduling emails are sent to the hubs a month before they are scheduled to be run. We have used a common template for those scheduling emails to remind folks about the Wikimedia project and available documentation. The monthly schedule and contact information is documented [here](https://digitalpubliclibraryofamerica.atlassian.net/wiki/spaces/CT/pages/84969744/Hub+Re-ingest+Schedule). 

```text
Good morning - 

Here is the ingest schedule for <MONTH>.

<MONTH> DD-DD
- Hub A
- Hub B

1. Going forward we will trying to run all ingests on the last week 
of the month. 

2. We have automated our search index rebuild process 
and have it scheduled to run on Thursday evenings. You can expect 
your records to be available online the Friday of the week your 
ingest is scheduled to run.

3. Please let us know number of records to expect in your harvest and 
if you are providing a file export to DPLA, please let us know when 
we can expect delivery of the data to your s3 bucket

4. If you are participating in our Wikimedia project we will try to 
run an upload of images into Wikimedia the week following your DPLA 
metadata ingest. If you have any questions about that project, please 
contact our Data Fellow, Dominic, at dominic@dp.la

5. Lastly - we have updated some of the documentation for our ingestion 
system to provide a high level overview of how we map, normalize, 
validate and enrich your metadata, available at 
https://github.com/dpla/ingestion3. This documentation also unpacks some of those 
summary/error messages you are all quite familiar with. Please let me know 
if you have any questions about those stages or documentation.

The Wikimedia uploads will happen sometime in <MONTH>.  Please let us know 
if anything needs to be updated and, if possible, the number of records 
we can expect in our harvest.


```

## Running ingests

Aggregation data is laid out in the following way

```text
./data/<hub>/<activity>/<DATE_TIME>_ACTIVITY/<DATA>
./data/<hub>/<activity>/<DATE_TIME>_ACTIVITY/_LOGS/
./data/<hub>/<activity>/<DATE_TIME>_ACTIVITY/_LOGS/WARNINGS/
./data/<hub>/<activity>/<DATE_TIME>_ACTIVITY/_LOGS/ERRORS/

ex. 

./data/txdl/
├── enrichment
│   └── 20240214_102322-txdl-MAP4_0.EnrichRecord.avro
├── harvest
│   └── 20240214_100659-txdl-OriginalRecord.avro
├── jsonl
│   └── 20240214_102733-txdl-MAP3_1.IndexRecord.jsonl
├── mapping
│   └── 20240214_101733-txdl-MAP4_0.MAPRecord.avro
├── originalRecords
│   └── 20240214
└── wiki
    └── 20240214_102901-txdl-wiki.parquet
```

### File export Hubs

Some hubs submit data via a file export process. Some are exported to s3 and others are emailed or linked to. If there is any question about how a hub submits data check the `i3.conf` file or [ingestion3-conf](https://github.com/dpla/ingestion3-conf) project.

These original records are kept in an `./originalRecords` directory for the hub and each export should be placed in a directory named `YYYYMMDD`. When running a harvest for these hubs the target originalRecords directory should be updated in the `./conf/i3.conf` file.

**Hubs which submit file exports**
```text
Conneticut              s3://dpla-hub-ct/
Florida                 s3://dpla-hub-fl/
Georgia                 s3://dpla-hub-ga/
Heartland               s3://dpla-hub-mo/
Massachusetts           Sends a download link
NARA                    Sends a download link
NYPL                    s3://dpla-hub-nypl/
Ohio                    s3://dpla-hub-ohio/
Smithsonian             s3://dpla-hub-si/
Texas Digital Library   s3://dpla-hub-tdl/
Vermont                 s3://dpla-hub-vt
```

### EC2 Ingest box
There is an existing EC2 instance we use to run ingests and we bring it up and down as needed. Below are some useful alias commands for dealing with our ingestion and wikimedia ec2 boxes as well as syncing data to the `s3://dpla-master-dataset/` bucket. These commands depend on the aws cli. 

Add these commands to your `.bashrc` or `.zshrc` files. 
```shell
# Example
#   ec2-status ingest
#   ec2-status wikimedia
ec2-status ()
{
 aws ec2 describe-instance-status \
  --include-all-instances \
  --instance-ids $(aws ec2 describe-instances \
    --query 'Reservations[].Instances[].InstanceId' \
    --filters "Name=tag-value,Values=$1" \
    --output text) \
  | jq ".InstanceStatuses[0].InstanceState.Name"
}
# Example
#   ec2-start ingest
#   ec2-stop wikimedia
ec2-start ()
{
 aws ec2 start-instances \
  --instance-ids $(aws ec2 describe-instances --query 'Reservations[].Instances[].InstanceId' --filters "Name=tag-value,Values=$1" --output text) \
  | jq
}
# Example
#   ec2-stop ingest
#   ec2-stop wikimedia
ec2-stop ()
{
 aws ec2 stop-instances \
  --instance-ids $(aws ec2 describe-instances \
    --query 'Reservations[].Instances[].InstanceId' \
    --filters "Name=tag-value,Values=$1" \
    --output text) \
  | jq
}
# Sync data to and from s3. Set the $DPLA_DATA ENV value to the 
# root directory of ingest data (ex. /Users/dpla/data/).
# This command already exists on the ingest EC2 instance and does 
# not need to be modified. 

# Example
#   sync-s3 gpo
#   sync-s3 smithsonian
sync-s3 () {
  echo "Sync from $DPLA_DATA$1/ to s3://dpla-master-dataset/$1/"
  aws s3 sync --exclude '*.DS_Store' $DPLA_DATA$1/ s3://dpla-master-dataset/$1/
}
```

1. Start the EC2 instance by running `ec2-start ingest`

2. Start screen sessions for each provider you need to harvest. This makes re-attaching to running screen sessions much easier.
```shell
> cd ~/ingestion3/
> screen -S gpo  
> i3-harvest gpo
# Detach from session Ctrl+A, d

# Re-attach to gpo session
> screen -xS gpo
```
3. Sync data back to s3
```shell
> sync-s3 gpo
```

4. Run `ec2-stop ingest` when the ingests are finished, the data has been synced to s3 and the instance is no longer needed. Data on the EBS volume will persist.

### Local
Bringing up the ingest EC2 instance is not always required. You can run a lot of the ingests on your laptop to save $$$ and overhead. Typically, these would be low record count harvests. Providers like Maryland, Vermont, or Digital Virginias which have ~150,000 records.

## Exceptions and unusual ingests
Not all ingests are fire and forget, some require a bit of massaging before we can successfully harvest their data.

### Firewalled endpoints
Some hubs have their feed endpoints behind a firewall so the harvests needs to be run while behind out VPN. I've been meaning to try and get the EC2 instance behind the VPN but that work is not a high priority right now because we have a workaround (run locally behind the VPN). Hubs that need to be harvested while connected to the VPN:

- Illinois
- Indiana
- MWDL

### Community Webs

Internet Archive community webs will send us a SQL database which we need to open and export as a JSON file. Then convert the JSON file to JSONL

- [Install SQLite DB Brower](https://sqlitebrowser.org/dl/)
- Install `jq` if needed, `brew install jq`

**Steps**
1. Open `.sql` file they emailed in SQLite DB Browser and export to JSON `File -> Export -> JSON`
2. Convert JSON to JSONL `jq -c '.[]' community-web-json-export.json > community-webs-jsonl-export.jsonl`
3. Zip the JSONL file `zip cw.zip community-webs-jsonl-export.jsonl`
4. Update the i3.conf file with the new endpoint/location of the zip file, e.g.  `./community-webs/originalRecords/20230426/`
### Digital Virginias
Digital Virginias uses [multiple Github repositories](https://github.com/dplava) to publish their metadata. We need to clone these repos in order to harvest their metadata. This is a handy invocation to clone all the repositories for a Github account.

```shell
> cd ./data/virginias/originalRecords/20230425/
> curl -s https://[GITHUB_USER]:@api.github.com/orgs/dplava/repos\?per_page\=200 | jq ".[].ssh_url" | xargs -n 1 git clone
> zip -r -X ./dva-20230426.zip .
```

Then execute the harvest after updating the `virginas.harvest.endpoint` value in `i3.conf`

### NARA
NARA will email a link to a ZIP file containing the new and updated records as well as IDs for records to be deleted each ingest cycle.

**Preprocessing**
1. Examine file export from NARA
2. Move `deletes_*.xml` files into `./deletes` directory
3. Rename "deletes_XXX.xml" to include a datestamp
   
   `> for f in *.xml ; do mv ./$f ./<DATE>_$f ; done`
4. Files are typically shipped as `.zip` this needs to be re-compressed as `.tar.gz` or `.bz2` after removing the `deletes_*.xml` files
    
   `> tar -cvzf ../<YYYYMMDD>-nara-delta.tar.gz ./`

Here is how the files should be organized before running a delta ingest
```
~/nara/delta/<YYYYMMDD>/
-- /deletes/deletes_XXX.xml
-- <YYYYMMDD>_nara_delta.tar.gz
```

**Delta Harvest**
After preprocessing the data export from NARA run the `NaraDeltaHarvester`
1. Configure the `i3.conf` file to point to the delta directory
```
nara.provider = "National Archives and Records Administration"
nara.harvest.type = "nara.file.delta"
nara.harvest.delta.update =   "~/nara/delta/<YYYYMMDD>/"
```
Run NARA harvest

This will produce a harvest data set with all records provided in the delta (no deletions are performed at this step).
`<YYYYMMDD>_121258-nara-OriginalRecord.avro`. Move this into the delta directory. We want the deletes, original records and original harvest all alongside each other.

```
~/nara/delta/<YYYYMMDD>/
-- /deletes/deletes_XXX.xml
-- <YYYYMMDD>_nara_delta.tar.gz
-- <YYYYMMDD>_121258-nara-OriginalRecord.avro
```

**Merge Utility**
The `NaraMergeUtil` will take two harvested data sets (base and delta) and merge the delta into the base data set. Using the last successful harvest as the base, and the output from the last step as the delta.

The entry expects five parameters
1. The path to the last full harvest (`~/nara/harvest/20200301_000000.avro`)
2. The path to the delta harvest (`~/nara/delta/20200704/202000704_121258-nara-OriginalRecord.avro`)
3. The path to folder containing the `deletes_*.xml` files (`~/nara/delta/20200704/deletes/`)
4. The path to save the merged output between the previous harvest and the delta (`~/nara/harvest/20200704_000000-nara-OriginalRecord.avro`)
5. spark master (`local[*]`)

```shell
sbt "runMain dpla.ingestion3.utils.NaraMergeUtil 
~/nara/harvest/20200301_000000.avro
~/nara/delta/20200704/202000704_121258-nara-OriginalRecord.avro
~/nara/delta/20200704/deletes/ 
~/nara/harvest/20200704_000000-nara-OriginalRecord.avro 
local[*]"
```

The merge utility will log details about the merge and delete operations (duplicates, updates, inserts, deletions, failed deletions) in a summary files (e.g. `~/nara/harvest/20200704_000000.avro/_LOGS/_SUMMARY.txt`)
```
 Base
 ----------
 path: ~/nara/harvest/20200301_000000.avro
 record count 14,468,257
 duplicate count 0
 unique count 14,468,257

 Delta
 -----------
 path: ~/nara/delta/20200704/202000704_121258-nara-OriginalRecord.avro 
 record count: 149,750
 duplicate count: 1
 unique count: 149,749

 Merged
 -----------
 path: ~/nara/harvest/20200704_000000.avro
 new: 54,412
 update: 95,337
 total [actual]: 14,522,669
 total [expected]: 14,522,669 = 14,468,257 (base) + 54,412 (new)

 Delete
 ------------
 path: ~/nara/delta/20200704/deletes/
 ids to delete specified at path: 109,015
 invalid deletes (IDs not in merged dataset): 103,509
 valid deletes (IDs in merged dataset): 5,506
 actual removed (merged total - total after deletes): 5,506

 Final
 -----
 total [actual]: 14,517,163 = 14,522,669 (merge actual) - 5,506 (delete actual)
```

Additionally, all of insert, update, delete operations are logged with the corresponding NARA IDs in a series of CSV files alongside the summary text file.
```
id,operation
100104443,insert
100108988,update
100111103,delete
```

The final merged data set is ready for mapping/enrichment/topic_modeling/indexing.


### Smithsonian
For all of the following instructions replace `<DATE>` with `YYYYMMDD`  

**Preprocessing**
1. Download the latest export from Smithsonian in `s3://dpla-hub-si/` into `./smithsonian/originalRecords/<DATE>/`
2. Recompress files, there is some kind of issue the gzip files produced by their export scripts.

   ```shell
   > mkdir -p ./smithsonian/originalRecords/<DATE>/fixed/
   > find ./smithsonian/originalRecords/<DATE> -name "*.gz" -type f | xargs -I{} sh -c 'echo "$1" "./$(basename ${1%.*}).${1##*.}"' -- {} | xargs -n 2 -P 8 sh -c 'gunzip -dckv $0 | gzip -kv > ./smithsonian/originalRecords/<DATE>/fixed/$1'
   ```
3. Run [dpla/xmll](https://github.com/dpla/xmll) over the fixed files (clone the project if you haven't already!) 

```shell
> mkdir -p ./smithsonian/originalRecords/<DATE>/xmll/
> find ./smithsonian/originalRecords/<DATE>/fixed/ -name "*.gz" -type f | xargs -I{} sh -c 'echo "$1" ".smithsonian/originalRecords/<DATE>/xmll/$(basename ${1%.*}).${1##*.}"' -- {} | xargs -n 2 -P 8 sh -c 'java -jar ~/dpla/code/xmll/target/scala-2.13/xmll-assembly-0.1.jar doc $0 $1
```

4. The original downloaded files and the contents of `./fixed/` can be deleted and only the xmll'd files need to be retained for harvesting
5. Update the `smithsonian.harvest.enpoint` value in `i3.conf` and run standard Smithsonian harvest.

### Common errors
A list of common errors seen during ingests.
#### Zip file bug
If you see this error in a file harvest stacktrace there is a bug when loading zip files created on Windows.

```shell
java.lang.IllegalArgumentException: Size must be equal or greater than zero: -1
    at org.apache.commons.io.IOUtils.toByteArray(IOUtils.java:505)
```
The easiest solution is simply to unzip and zip the file OSX using command line tools and then rerun the ingest
```shell
> unzip target.zip
> zip -r -X ./target-20230426.zip .
```
# ingestion 3

This project is an ETL system for aggregating cultural heritage metadata from the [DPLA hub network](https://pro.dp.la/hubs). The system has five primary components, harvesting original records, mapping original records into DPLA MAP records, enriching DPLA MAP records, exporting DPLA MAP records as JSON-L to be indexed, and exporting eligible DPLA MAP records in Wiki markup.

# Harvest

We harvest data from multiple sources but generally they fall into three categories: api, file, and oai.

<img alt="oai: 25%, file: 15%, api: 7%" src="https://i.imgur.com/WZWuYnr.png" height="300"/>


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
There are provider specific rules and exceptions written into some mappings and it is outside the scope of this document to enumerate and explain all of them but one example of filtering perferred and non-preferred values is provided below.

There are three primary fields where we have opted to develop some filtering rules
* extent
* format
* type

The four big filtering functions are:
* [`extent` identification](https://github.com/dpla/ingestion3/blob/dcfe8d9a226c43eb3fe5180744d781995399072d/src/main/scala/dpla/ingestion3/enrichments/normalizations/filters/ExtentIdentificationList.scala). This uses pattern matching to identify values which look like dimensions of an object and would then map them to the exent field (e.x. a provider's format field contins both "format" values and "exent" values)
* [exclude `type` from `format`](https://github.com/dpla/ingestion3/blob/dcfe8d9a226c43eb3fe5180744d781995399072d/src/main/scala/dpla/ingestion3/enrichments/normalizations/filters/FormatTypeValuesBlockList.scala). A list of terms which belong in the type field rather than the format field. 
* [exclude digital surrogate from `format`](https://github.com/dpla/ingestion3/blob/dcfe8d9a226c43eb3fe5180744d781995399072d/src/main/scala/dpla/ingestion3/enrichments/normalizations/filters/DigitalSurrogateBlockList.scala). A list of generic and provider specific terms describing the  digital surrogates (. These values typically appear in the `format` field and should be removed. 
* [allow `type`](https://github.com/dpla/ingestion3/blob/dcfe8d9a226c43eb3fe5180744d781995399072d/src/main/scala/dpla/ingestion3/enrichments/normalizations/filters/TypeAllowList.scala). A list of terms which are allowed in the `type` field because they can be mapped to the allowed list of DCMIType values during the [type enrichment](#type)

Below is a specific example using DigitalSurrogateBlockList, FormatTypeValueBlockList, ExtentIdentificationList against the format field. 

For some provider we have a filter in place for the `format` field ([code](https://github.com/dpla/ingestion3/blob/develop/src/main/scala/dpla/ingestion3/mappers/providers/OhioMapping.scala#L58-L65)). This level of filtering is not common and is based on careful review of existing metadata with an eye towards strict compliance with existing metadata guidelines.

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
One requirement of the Wikimedia project is that data provider values must be mapped to a Wikidata URI. DPLA maintains a lookup table of data provider names and Wikidata IDs (see [institutions_v2.json](https://github.com/dpla/ingestion3/blob/develop/src/main/resources/wiki/institutions_v2.json). The enrichment adds the Wikidata URI to the `edmAgent.exactMatch` property. Without a Wikidata URI, a record cannot be uploaded to Wikimedia.

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
  * [Institutional](#institutional)
  * [Standardized rights](#standardized-rights)
  * [Media assets](#media-assets)
  * [Data provider URI](#data-provider-uri)
* [Wikimedia metadata](#wikimedia-metadata)
* [Media](#media)
    * [CONTENTdm and IIIF Manifests](#contentdm-and-iiif-manifests)

Records which meet eligibility requirements can have their fullframe media assets and some associated metadta uploaded to Wikimedia.

## Eligibility

### Institutional 
Overall eligibility is controlled in the [institutions_v2.json](https://github.com/dpla/ingestion3/blob/develop/src/main/resources/wiki/institutions_v2.json) by the `upload` property which is defined both at the hub and dataProvider levels. 

A `hub` can have `"upload": true` and all `dataProviders` will have their records evaluated for eligibility.

```json
{
  "National Archives and Records Administration": {
    "Wikidata": "Q518155",
    "upload": true,
    "institutions": {
      
    }
  }
}
```

A hub can have defined `"upload": false` and specific `dataProviders` (defined as `institutions` in the `instituions_v2.json` file) within that hub can have `"upload": true` and only those `dataProviders` will have records evaluated for eligibly in the Wikimedia project. 

```json
{
  "Ohio Digital Network": {
    "Wikidata": "Q83878495",
    "upload": false,
    "institutions": {
      "Cleveland Public Library": {
        "Wikidata": "Q69487402",
        "upload": true
      }
    }
  }
}
```

In addition to how institutional eligibility is defined in `institutions_v2.json` records must meet three minimum metadata requirements to be eligible for upload.

### Standardized rights
The record must have an `edmRights` URI, and it must be one of these values. All ports and versions of these values are valid.
```text
http://rightsstatements.org/vocab/NoC-US/
http://creativecommons.org/publicdomain/mark/
http://creativecommons.org/publicdomain/zero/
http://creativecommons.org/licenses/by/
http://creativecommons.org/licenses/by-sa/
```

### Media assets
The record must have either a `iiifManifest` or a `mediaMaster` URL. This value is distinct from the `object` mapping which is a single value and expected to a low resolution thumbnail (150px). The URLs for `mediaMaster` should point to the highest resolution possible and can be more than one URL.

### Data provider URI
The `dataProvider` name must be reconciled to a WikiData URI. This is an enrichment that DPLA performs on these values (see [dataProvider enrichments](#dataprovider))


## Wikimedia metadata
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

### CONTENTdm and IIIF Manifests

![Millhouse the magician](https://media.giphy.com/media/ieREaX3VTHsqc/giphy.gif)

When validating whether a record meets the minimum requirements, and neither a IIIF manifest nor media master value is provided, we will attempt to programmatically generate a IIIF manifest url if the isShownAt value looks like a CONTENTdm URL.

```Scala
// If there is neither a IIIF manifest or media master mapped from the original  
// record then try to construct a IIIF manifest from the isShownAt value.
// This should only work for CONTENTdm URLs.
val dplaMapRecord =
  if(dplaMapData.iiifManifest.isEmpty && dplaMapData.mediaMaster.isEmpty)
    dplaMapData.copy(iiifManifest = buildIIIFFromUrl(dplaMapData.isShownAt))
```

Where `buildIIIFFromUrl()` uses regex matching to identify and extract the necessary
components of the isShownAt URL.

```Scala
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
