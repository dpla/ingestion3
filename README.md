# DPLA Ingestion 3

This project is an ETL system for cultural heritage metadata. The system has three primary components:

### Harvesters
* [OAI-PMH](#oai-pmh)
* [ResourceSync]() TBD
* [API]() TBD
* [File]() TBD
* [Primo]() TBD
### Mappers
* [TBD]() 
### Enrichments
* [String enrichments]()
* [Linked data enrichments]()



## OAI-PMH Harvester
For additional information please read the [announcement blog post](https://dp.la/info/2017/08/08/dpla-launches-open-source-spark-oai-harvester/) or the complete documentation [on our wiki](https://digitalpubliclibraryofamerica.atlassian.net/wiki/spaces/TECH/pages/87658172/Spark+OAI+Harvester)

Clone the repo and build the JAR
```text
git clone https://github.com/dpla/ingestion3.git
cd ingestion3
sbt package
``` 


### Create a configuration file for the harvest

OAI Harvster options 

Option | Obligation | Usage 
---------|------------|----
endpoint | Required | The base URL for the OAI repository.
verb | Required | "ListSets" to harvest only sets; "ListRecords" to harvest records and any sets to which the records may belong. Case-sensitive.
outputDir | Required | Location to save output. This should be a local path. Amazon S3 may be supported at some point.
metadataPrefix | Required when verb="ListRecords"; prohibited when verb="ListSets". | The the metadata format in OAI-PMH requests issued to the repository.
provider | Required | The name of the source of the records.
harvestAllSets | 	Optional when verb="ListRecords"; cannot be used in conjunction with either setlist or blacklist. | "True" to harvest records from all sets. Default is "false". Case-insensitive. Results will include all sets and all their records. This will only return records that belong to at least one set; records that do not belong to any set will not be included in the results.
setlist | Optional when verb="ListRecords"; cannot be used in conjunction with either harvestAllSets or blacklist. | Comma-separated lists of sets to include in the harvest. Use the OAI setSpec to identify a set. Results will include all sets in the setlist and all their records.
blacklist | Optional when verb="ListRecords"; cannot be used in conjunction with either harvestAllSets or setlist. | Comma-separated lists of sets to exclude from the harvest. Use the OAI setSpec to identify a set. Results will include all sets not in the blacklist and all their records. Records that do not belong to any set will not be included in the results.

*Sample OAI harvester config file*
```
# oai-sample.conf
verb = "ListRecords"
endpoint = "http://fedora.sample.org/oaiprovider/"
metadataPrefix = "mods"
outputDir = "/path/to/somewhere" 
provider = "DPLA partner A"
blacklist = "ignore,ignore2"
```

### Running using SBT
To use SBT you need to specify the path to the config file you just created when invoking the harvester. Depending on where the config file is located you will do this in one of three ways as a VM parameter


*Specifying the configuration file*

```
# Example when the path is stored locally. This should address 95% of all use cases
-Dconfig.file=/loca/path/to/config.conf

# Exmple when the profile is stored remotely
-Dconfig.url=https://s3.amazonaws.com/dpla-i3-oai-profiles/sample_provider.conf

# Example when the config file is on the project classpath
-Dconfig.resource=profiles/sample_provider.conf
``` 

*Example invocation with local config file*

`sbt "run -Dconfig.file=/loca/path/to/oai.conf /path/to/ingestion3.jar"`

### Running using IntelliJ
Specify the config file parameter as a VM Option argument.

```text
-Dconfig.url=https://s3.amazonaws.com/dpla-i3-oai-profiles/sample_provider.conf
``` 

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/6a9dfda51ad04ce3acfb7fcb441af846)](https://www.codacy.com/app/mdellabitta/ingestion3?utm_source=github.com&utm_medium=referral&utm_content=dpla/ingestion3&utm_campaign=badger)
[![Build Status](https://travis-ci.org/dpla/ingestion3.svg?branch=master)](https://travis-ci.org/dpla/ingestion3)

## Development And Testing

```
$ sbt test  # Runs unit tests
$ GEOCODER_HOST="my-geocoder" sbt it:testOnly  # Runs integration tests
```
